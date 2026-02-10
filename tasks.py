"""Background tasks and payment listener for Lightning Goats."""

import asyncio
import random
import hashlib
import os
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import Any, Optional, Tuple
from loguru import logger


# --- Startup backfill guard (only process 'existing' payments for the current day) ---
_LG_TZ = ZoneInfo(os.getenv('LIGHTNING_GOATS_TIMEZONE', 'America/Chicago'))
_LG_STARTED_AT: datetime | None = None
_LG_TODAY_START: datetime | None = None
# Events that arrive shortly after extension startup can include a backlog check.
# We only want to process that backlog for payments whose paid/created timestamp is today.
_LG_STARTUP_BACKFILL_WINDOW_SECONDS = int(os.getenv('LIGHTNING_GOATS_STARTUP_BACKFILL_WINDOW_SECONDS', '180'))

from lnbits.tasks import wait_for_paid_invoices, create_permanent_unique_task
from lnbits.core.crud import get_wallet

from .crud import (
    get_settings,
    get_all_settings,
    try_claim_payment,
    mark_payment_processed,
    mark_payment_failed,
)
from .services.openhab import OpenHABService
from .services.messaging import (
    send_feeder_message,
    send_payment_received_message,
    send_interface_info_message,
    send_weather_message,
)
from .services.weather import fetch_weather_data
from .config import (
    DEFAULT_WEATHER_BROADCAST_INTERVAL,
    DEFAULT_WEATHER_BROADCAST_PROBABILITY,
    DEFAULT_WEATHER_URL,
)

# Payment listener name for LNbits
INVOICE_LISTENER_NAME = "ext_lightning_goats"


def _coerce_hex(value: Any) -> Optional[str]:
    """Best-effort conversion of a preimage/hash value to lowercase hex string."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, str):
        v = value.strip().lower()
        if v.startswith("0x"):
            v = v[2:]
        # If it's wrapped like "preimage: <hex>"
        v = v.split()[-1] if " " in v and all(c in "0123456789abcdefx:" for c in v.replace(" ", "")) else v
        return v
    if isinstance(value, dict):
        for k in ("preimage", "payment_preimage", "paymentProof", "proof", "r_preimage"):
            if k in value:
                return _coerce_hex(value[k])
    return None


def _extract_payment_hash_checking_id_preimage(payment: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract (payment_hash, checking_id, preimage) from LNbits Payment-like objects.

    LNbits versions / backends differ slightly, so we try multiple attribute names.
    """
    checking_id = getattr(payment, "checking_id", None) or getattr(payment, "checkingId", None)

    payment_hash = (
        getattr(payment, "payment_hash", None)
        or getattr(payment, "payment_hash_hex", None)
        or getattr(payment, "hash", None)
    )
    payment_hash = _coerce_hex(payment_hash)

    preimage = (
        getattr(payment, "preimage", None)
        or getattr(payment, "payment_preimage", None)
        or getattr(payment, "payment_proof", None)
        or getattr(payment, "proof", None)
        or getattr(payment, "paymentProof", None)
    )
    preimage = _coerce_hex(preimage)

    # Some LNbits event payloads nest details under `extra`
    if preimage is None:
        preimage = _coerce_hex(getattr(payment, "extra", None))
    if payment_hash is None:
        payment_hash = _coerce_hex(getattr(payment, "extra", None))

    return payment_hash, checking_id, preimage


async def _lookup_preimage_from_core(checking_id: Optional[str], payment_hash: Optional[str]) -> Optional[str]:
    """Best-effort: ask LNbits core for the stored preimage/proof, if available."""
    try:
        # NOTE: import inside to avoid hard dependency if LNbits core API differs.
        from lnbits.core.crud import get_payment  # type: ignore
    except Exception:
        return None

    try:
        p = None
        if checking_id:
            # Common signature: get_payment(checking_id)
            try:
                p = await get_payment(checking_id)  # type: ignore
            except TypeError:
                # Some versions may use kwargs
                p = await get_payment(checking_id=checking_id)  # type: ignore
        if not p or not payment_hash:
            return None

        # Try common attribute names
        pre = (
            getattr(p, "preimage", None)
            or getattr(p, "payment_preimage", None)
            or getattr(p, "proof", None)
            or getattr(p, "payment_proof", None)
        )
        pre_hex = _coerce_hex(pre)
        return pre_hex
    except Exception:
        return None


def _verify_payment_proof(payment_hash_hex: str, preimage_hex: str) -> bool:
    """Verify sha256(preimage) == payment_hash."""
    try:
        preimage_bytes = bytes.fromhex(preimage_hex)
        digest = hashlib.sha256(preimage_bytes).hexdigest()
        return digest == payment_hash_hex.lower()
    except Exception:
        return False




def _parse_ts_to_dt(value: Any) -> Optional[datetime]:
    """Best-effort parse various timestamp formats into a tz-aware datetime in _LG_TZ."""
    if value is None:
        return None
    try:
        # numeric epoch
        if isinstance(value, (int, float)):
            v = float(value)
            # heuristics: ms vs seconds
            if v > 1e12:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=_LG_TZ)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            # numeric string
            if s.isdigit():
                return _parse_ts_to_dt(int(s))
            # isoformat variants
            s2 = s.replace('Z', '+00:00')
            try:
                dt = datetime.fromisoformat(s2)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_LG_TZ)
                return dt.astimezone(_LG_TZ)
            except Exception:
                pass
            # common 'YYYY-mm-dd HH:MM:SS' without tz
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
                try:
                    dt = datetime.strptime(s, fmt).replace(tzinfo=_LG_TZ)
                    return dt
                except Exception:
                    continue
    except Exception:
        return None
    return None


def _extract_payment_dt(payment: Any) -> Optional[datetime]:
    """Extract a best-effort paid/created datetime from the event payload."""
    # Prefer paid/settled timestamps if present; fall back to created time.
    candidates = []
    for attr in (
        'paid_at', 'paidAt', 'settled_at', 'settledAt',
        'created_at', 'createdAt', 'time', 'timestamp', 'date',
    ):
        if hasattr(payment, attr):
            candidates.append(getattr(payment, attr))
    # Sometimes nested under extra/details
    extra = getattr(payment, 'extra', None)
    if isinstance(extra, dict):
        for k in ('paid_at','paidAt','settled_at','settledAt','created_at','createdAt','time','timestamp','date'):
            if k in extra:
                candidates.append(extra[k])
    for v in candidates:
        dt = _parse_ts_to_dt(v)
        if dt is not None:
            return dt
    return None


async def _lookup_payment_from_core(checking_id: Optional[str]) -> Any:
    """Best-effort: fetch stored payment/invoice record from LNbits core."""
    if not checking_id:
        return None
    try:
        from lnbits.core.crud import get_payment  # type: ignore
    except Exception:
        return None
    try:
        try:
            return await get_payment(checking_id)  # type: ignore
        except TypeError:
            return await get_payment(checking_id=checking_id)  # type: ignore
    except Exception:
        return None


def _extract_dt_from_core_payment(p: Any) -> Optional[datetime]:
    if p is None:
        return None
    # handle dict-like or object-like
    candidates = []
    if isinstance(p, dict):
        for k in ('paid_at','paidAt','settled_at','settledAt','created_at','createdAt','time','timestamp','date'):
            if k in p:
                candidates.append(p[k])
    for attr in ('paid_at','paidAt','settled_at','settledAt','created_at','createdAt','time','timestamp','date'):
        if hasattr(p, attr):
            candidates.append(getattr(p, attr))
    for v in candidates:
        dt = _parse_ts_to_dt(v)
        if dt is not None:
            return dt
    return None

async def _handle_payment(payment: Any) -> None:
    """
    Handle incoming payment event.
    
    This replaces the WebSocket monitoring logic from main.nozaps.py.
    Uses LNbits native payment listener for reliable payment processing.
    
    Args:
        payment: Payment object from LNbits
    """
    claimed_payment_hash: Optional[str] = None
    payment_failed: bool = False
    try:
        logger.info(
            f"🎯 Lightning Goats: PAYMENT EVENT RECEIVED\n"
        )
        logger.info(
            f"💰 Payment Details:\n"
            f"   • Wallet ID: {payment.wallet_id}\n"
            f"   • Amount: {payment.amount} msat ({payment.amount // 1000} sats)"
        )
        
        # Get amount in sats
        amount_sats = payment.amount // 1000
        
        if amount_sats <= 0:
            return
        
        # Get wallet for this payment
        wallet = await get_wallet(payment.wallet_id)
        if not wallet:
            logger.warning(f"Lightning Goats: could not find wallet {payment.wallet_id}")
            return
        
        # Get settings for this wallet's user
        settings = await get_settings(wallet.user)
        if not settings:
            logger.debug(f"Lightning Goats: no settings for user {wallet.user}")
            return
        
        # Check if this payment is for the configured herd wallet
        if settings.herd_wallet_id:
            payment_wallet_normalized = str(payment.wallet_id).strip().lower()
            herd_wallet_normalized = str(settings.herd_wallet_id).strip().lower()
            
            if payment_wallet_normalized != herd_wallet_normalized:
                logger.info(
                    f"Lightning Goats: payment to non-herd wallet {payment_wallet_normalized}. "
                    f"Expected (herd): {herd_wallet_normalized}. Skipping."
                )
                return
        
        
        # === Exactly-once payment processing (idempotency) ===
        # We use the invoice payment_hash (and its proof/preimage when available) as an idempotency key.
        payment_hash, checking_id, preimage = _extract_payment_hash_checking_id_preimage(payment)

        # Fall back to checking_id if payment_hash is unavailable (still stable across restarts).
        idempotency_key = payment_hash or checking_id
        if not idempotency_key:
            logger.warning("Lightning Goats: cannot determine idempotency key for payment; skipping to avoid double-processing")
            return

        # --- Startup backlog guard ---
        # LNbits' wait_for_paid_invoices can deliver a backlog of 'now-known-paid' invoices on extension startup.
        # We only want to process that backlog for payments whose paid/created timestamp is *today* (in _LG_TZ).
        now_dt = datetime.now(tz=_LG_TZ)
        startup_mode = (
            _LG_STARTED_AT is not None
            and _LG_TODAY_START is not None
            and (now_dt - _LG_STARTED_AT).total_seconds() <= _LG_STARTUP_BACKFILL_WINDOW_SECONDS
        )
        core_payment = None
        payment_dt = None
        if startup_mode:
            payment_dt = _extract_payment_dt(payment)
            if payment_dt is None and checking_id:
                core_payment = await _lookup_payment_from_core(checking_id)
                payment_dt = _extract_dt_from_core_payment(core_payment)
            # If we can determine the payment's time and it's before today's midnight, ignore it (idempotently).
            if payment_dt is not None and payment_dt < _LG_TODAY_START:
                # Record it once as 'failed' with an ignore reason so it won't spam every restart.
                preimage_to_store = preimage or 'unavailable'
                claimed = await try_claim_payment(
                    payment_hash=idempotency_key,
                    checking_id=checking_id,
                    wallet_id=wallet.id,
                    amount_msat=payment.amount,
                    preimage=preimage_to_store,
                )
                if claimed:
                    await mark_payment_failed(
                        idempotency_key,
                        f"Ignored historical payment before today ({payment_dt.isoformat()})",
                    )
                logger.info(
                    f"Lightning Goats: ignoring historical payment on startup (idempotency_key={idempotency_key}, payment_dt={payment_dt.isoformat()})"
                )
                return
            # If we can't determine timestamp during startup backlog, be conservative and ignore it once.
            if payment_dt is None and checking_id is None:
                preimage_to_store = preimage or 'unavailable'
                claimed = await try_claim_payment(
                    payment_hash=idempotency_key,
                    checking_id=checking_id,
                    wallet_id=wallet.id,
                    amount_msat=payment.amount,
                    preimage=preimage_to_store,
                )
                if claimed:
                    await mark_payment_failed(idempotency_key, "Ignored startup backlog payment with unknown timestamp")
                logger.info(
                    f"Lightning Goats: ignoring startup backlog payment with unknown timestamp (idempotency_key={idempotency_key})"
                )
                return


        # Try to obtain a preimage/proof if it wasn't included in the event payload.
        if preimage is None:
            # Reuse the core payment record if we already fetched it for timestamp filtering
            if core_payment is not None:
                preimage = _coerce_hex(getattr(core_payment, 'preimage', None)) or (
                    _coerce_hex(core_payment.get('preimage')) if isinstance(core_payment, dict) else None
                )
            if preimage is None:
                preimage = await _lookup_preimage_from_core(checking_id, payment_hash)

        # Persist a placeholder if proof is unavailable (we still get exactly-once, but can't cryptographically verify).
        preimage_to_store = preimage or "unavailable"

        # Atomically claim this payment for processing. If this returns False, we've already processed it.
        claimed = await try_claim_payment(
            payment_hash=idempotency_key,
            checking_id=checking_id,
            wallet_id=wallet.id,
            amount_msat=payment.amount,
            preimage=preimage_to_store,
        )
        if not claimed:
            logger.info(f"Lightning Goats: payment already processed (idempotency_key={idempotency_key}); skipping")
            return

        claimed_payment_hash = idempotency_key

        # If we have both a real preimage and a 32-byte payment_hash, verify sha256(preimage) == payment_hash.
        # (Skip verification when the key is checking_id fallback or proof is unavailable.)
        if payment_hash and preimage and len(payment_hash) == 64 and preimage_to_store != "unavailable":
            if not _verify_payment_proof(payment_hash, preimage):
                payment_failed = True
                await mark_payment_failed(claimed_payment_hash, "Invalid payment proof: sha256(preimage) != payment_hash")
                logger.error("Lightning Goats: invalid payment proof; refusing to process this payment")
                return

        # Get current wallet balance
        balance_sats = wallet.balance_msat // 1000
        logger.info(f"Lightning Goats: handling payment of {amount_sats} sats for user {wallet.user[:8]}. Current balance: {balance_sats} sats.")
        
        # Initialize OpenHAB service
        openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
        
        try:
            # Check if we should trigger feeder
            if balance_sats >= settings.feeder_trigger_sats:
                logger.info(f"Lightning Goats: feeder trigger threshold reached ({balance_sats} >= {settings.feeder_trigger_sats})")
                
                # Check override
                if await openhab.is_feeder_override_enabled():
                    logger.info(f"Lightning Goats: feeder override enabled, skipping trigger")
                else:
                    # Trigger feeder
                    if await openhab.trigger_feeder(settings.openhab_feeder_rule_id):
                        logger.info(f"Lightning Goats: feeder triggered successfully")
                        
                        # Send messaging notification with user_id for template selection
                        msg_success = await send_feeder_message(
                            balance_sats=balance_sats,
                            payment_amount=amount_sats,
                            user_id=wallet.user,
                        )
                        logger.info(f"Lightning Goats: feeder message sent: {msg_success}")
                        
                        # Distribute to cyberherd members using internal function
                        try:
                            # Use transfer_herd_balance_to_source directly to bypass the
                            # "send_splits_enabled" check in cyberherd.
                            from lnbits.extensions.cyberherd.services.send_splits import (
                                transfer_herd_balance_to_source,
                            )
                            from lnbits.extensions.cyberherd.crud import (
                                get_settings as get_ch_settings,
                            )

                            logger.info(f"Lightning Goats: fetching CyberHerd settings for payment distribution")
                            ch_settings = await get_ch_settings(wallet.user)
                            
                            if ch_settings:
                                logger.info(f"Lightning Goats: triggering CyberHerd payment distribution")
                                result = await transfer_herd_balance_to_source(ch_settings)
                                if result.get("ok"):
                                    logger.info(f"Lightning Goats: CyberHerd distribution complete: {result.get('sats')} sats")
                                else:
                                    logger.error(f"Lightning Goats: CyberHerd distribution failed: {result.get('error')}")
                            else:
                                logger.error(f"Lightning Goats: CyberHerd settings not found for user {wallet.user}")

                        except ImportError:
                            logger.warning("Lightning Goats: CyberHerd extension not available for payment distribution")
                        except Exception as e:
                            logger.error(f"Lightning Goats: Failed to trigger CyberHerd payment: {e}")
                    else:
                        logger.error(f"Lightning Goats: feeder trigger failed")
                        
            elif amount_sats >= settings.minimum_sats:
                # Send payment received message (if enabled)
                if settings.interface_messages_enabled:
                    logger.info(f"Lightning Goats: sending payment received message for {amount_sats} sats")
                    msg_success = await send_payment_received_message(
                        amount=amount_sats,
                        balance=balance_sats,
                        trigger_threshold=settings.feeder_trigger_sats,
                        user_id=wallet.user,
                    )
                    logger.info(f"Lightning Goats: payment received message sent: {msg_success}")
                else:
                    logger.debug(f"Lightning Goats: interface messages disabled")
            else:
                logger.debug(f"Lightning Goats: payment too small ({amount_sats} sats)")
                
        finally:
            try:
                await openhab.close()
            finally:
                if claimed_payment_hash and not payment_failed:
                    await mark_payment_processed(claimed_payment_hash)
            
    except Exception as e:
        if claimed_payment_hash and not payment_failed:
            try:
                await mark_payment_failed(claimed_payment_hash, f"Unhandled exception: {e}")
            except Exception:
                pass
        logger.error(f"Lightning Goats: Error handling payment: {e}", exc_info=True)


def start_payment_listener():
    """
    Register payment listener for Lightning Goats.
    
    This is called during extension initialization to set up the
    payment listener that replaces the WebSocket monitoring.
    Uses LNbits standard task creation patterns.
    
    Returns:
        Payment listener coroutine
    """
    try:
        global _LG_STARTED_AT, _LG_TODAY_START
        now = datetime.now(tz=_LG_TZ)
        _LG_STARTED_AT = now
        _LG_TODAY_START = datetime.combine(now.date(), dtime.min, tzinfo=_LG_TZ)
        logger.info(f"Lightning Goats: startup backfill window={_LG_STARTUP_BACKFILL_WINDOW_SECONDS}s; today starts at {_LG_TODAY_START.isoformat()}")

        async def _handler(payment: Any) -> None:
            await _handle_payment(payment)
        
        listener = wait_for_paid_invoices(INVOICE_LISTENER_NAME, _handler)
        create_permanent_unique_task("ext_lightning_goats_invoice_listener", listener)
        logger.info(f"Lightning Goats: Payment listener registered as '{INVOICE_LISTENER_NAME}'")
        return listener
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to start payment listener: {e}", exc_info=True)
        return None


async def periodic_informational_messages():
    """
    Background task for periodic informational messages.
    
    Sends weather updates and interface info messages at random intervals.
    This replaces the periodic_informational_messages from main.nozaps.py.
    """
    logger.info("Starting periodic informational messages task")
    
    while True:
        try:
            # Wait for the configured interval
            await asyncio.sleep(DEFAULT_WEATHER_BROADCAST_INTERVAL)
            
            try:
                all_settings = await get_all_settings()
            except Exception as exc:
                logger.error(f"Lightning Goats: failed to list settings for periodic messages: {exc}")
                continue

            if not all_settings:
                logger.debug("Lightning Goats: no persisted settings found for periodic messaging")
                continue

            for settings in all_settings:
                user_id = settings.user_id

                try:
                    # Interface info check first to preserve the documented 30% chance.
                    if settings.interface_messages_enabled:
                        if random.random() < DEFAULT_WEATHER_BROADCAST_PROBABILITY:
                            await send_interface_info_message(user_id=user_id)
                            continue  # Only one broadcast per cycle

                    if settings.weather_broadcast_enabled:
                        weather_probability = DEFAULT_WEATHER_BROADCAST_PROBABILITY
                        if settings.interface_messages_enabled and weather_probability < 1.0:
                            # Adjust so the unconditional chance remains 30% even when interface is evaluated first
                            denominator = 1.0 - DEFAULT_WEATHER_BROADCAST_PROBABILITY
                            if denominator > 0:
                                weather_probability = min(1.0, weather_probability / denominator)

                        if random.random() < weather_probability:
                            weather_url = settings.weather_station_url or DEFAULT_WEATHER_URL
                            if not weather_url:
                                logger.debug(
                                    f"Lightning Goats: skipping weather broadcast for user {user_id} - no URL configured"
                                )
                            else:
                                weather = await fetch_weather_data(weather_url)
                                if weather:
                                    await send_weather_message(weather, user_id=user_id)
                                else:
                                    logger.debug(
                                        f"Lightning Goats: no weather data available for user {user_id}"
                                    )
                except Exception as user_exc:
                    logger.warning(
                        f"Lightning Goats: periodic message handling failed for user {user_id}: {user_exc}"
                    )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Error in periodic informational messages: {exc}", exc_info=True)
            # Continue running despite errors
            await asyncio.sleep(10)


# Task references for cleanup
_background_tasks = []


def start_background_tasks():
    """
    Start all background tasks for Lightning Goats.
    
    Uses LNbits standard task creation patterns.
    Note: Bitcoin price history is handled by OpenHAB.
    """
    global _background_tasks
    
    try:
        # Start periodic messages task using asyncio
        task = asyncio.create_task(periodic_informational_messages())
        _background_tasks.append(task)
        logger.info("Lightning Goats: Periodic informational messages task started")
        
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to start background tasks: {e}", exc_info=True)


async def stop_background_tasks():
    """Stop all background tasks."""
    global _background_tasks
    
    logger.info("Stopping Lightning Goats background tasks")
    
    for task in _background_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    
    _background_tasks.clear()
    logger.info("Lightning Goats background tasks stopped")