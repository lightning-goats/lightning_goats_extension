"""Background tasks and payment listener for Lightning Goats."""

import asyncio
import random
import hashlib
import os
import time
from datetime import datetime, time as dtime, timezone
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
    reconcile_stale_processing_payments,
)
from .services.openhab import OpenHABService
from .services.messaging import (
    send_feeder_message,
    send_payment_received_message,
    send_interface_info_message,
    send_weather_message,
)
from .services.weather import fetch_weather_data
from .services.url_validation import OutboundURLPolicyError, ensure_outbound_url_allowed
from .config import (
    DEFAULT_WEATHER_BROADCAST_INTERVAL,
    DEFAULT_WEATHER_BROADCAST_PROBABILITY,
    DEFAULT_WEATHER_URL,
)

# Payment listener name for LNbits
INVOICE_LISTENER_NAME = "ext_lightning_goats"
# Unique task names registered with the LNbits task registry.
INVOICE_LISTENER_TASK_NAME = "ext_lightning_goats_invoice_listener"
PERIODIC_MESSAGES_TASK_NAME = "ext_lightning_goats_periodic_messages"

# Tasks we own, so we can cancel every one of them when the extension stops.
scheduled_tasks: list[asyncio.Task] = []

# Per-herd-wallet locks serialize payment processing so two payments crossing
# the feeder threshold at the same time cannot double-trigger the feeder or
# double-distribute the herd balance.
_herd_locks: dict[str, asyncio.Lock] = {}


def _get_herd_lock(key: str) -> asyncio.Lock:
    lock = _herd_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _herd_locks[key] = lock
    return lock


# Per-herd-wallet feeder cooldown. After a feed, the feeder will not fire again
# for this many seconds. This prevents the goats being fed repeatedly by
# successive payments when the balance is not cleared (e.g. a distribution
# failure leaves it above the trigger) and generally guards against
# over-feeding. Separate payments wait for this cooldown before feeding. Set to 0 to
# disable. Note: per-process (like the herd locks); a multi-worker deployment
# would need a shared store, but LNbits runs a single invoice listener.
FEEDER_COOLDOWN_SECONDS = max(
    0, int(os.getenv("LIGHTNING_GOATS_FEEDER_COOLDOWN_SECONDS", "60"))
)
# The OpenHAB owner has a 5-second cooldown. Six seconds allows that
# cooldown to expire between requests in a single payment's reward burst.
FEEDER_BATCH_INTERVAL_SECONDS = max(
    6.0, float(os.getenv("LIGHTNING_GOATS_FEEDER_BATCH_INTERVAL_SECONDS", "6"))
)
MAX_FEEDINGS_PER_PAYMENT = 3
_last_feed_monotonic: dict[str, float] = {}


async def _distribute_herd_balance(
    user_id: str, payment: Any = None, *, amount_sats: int | None = None,
    feeding_id: str | None = None,
) -> None:
    """Distribute after the feeder path reports a successful trigger.

    CyberHerd's independent invoice listener does not authorize payouts. This
    helper runs for each funded threshold cycle. The first three cycles per
    received payment feed the goats; remaining cycles distribute without feeding.
    """
    try:
        from lnbits.extensions.cyberherd.crud import get_settings as get_ch_settings

        ch_settings = await get_ch_settings(user_id)
        if not ch_settings:
            raise RuntimeError(f"CyberHerd settings not found for user {user_id}")

        if payment is not None:
            from lnbits.extensions.cyberherd.services.payment_coordinator import get_payment_coordinator
            coordinator = get_payment_coordinator(user_id=user_id)
            await coordinator.sync_payment_for_distribution(payment, settings=ch_settings)
        source_wallet = getattr(ch_settings, "source_wallet", None)
        if source_wallet:
            from lnbits.extensions.cyberherd.services.splits import update_split_targets_proportional
            await update_split_targets_proportional(source_wallet, user_id=user_id, strict=True)

        from lnbits.extensions.cyberherd.services.send_splits import (
            maybe_send_splits_for_user,
            transfer_herd_balance_to_source,
        )

        if getattr(ch_settings, "send_splits_enabled", False):
            result = await maybe_send_splits_for_user(
                user_id, ch_settings, feeder_triggered=True,
                amount_sats=amount_sats, feeding_id=feeding_id
            )
            if result is None:
                raise RuntimeError("CyberHerd distribution is not ready; retry required")
        else:
            result = await transfer_herd_balance_to_source(ch_settings, amount_sats=amount_sats, feeding_id=feeding_id)
        if result.get("ok"):
            logger.info(
                f"Lightning Goats: CyberHerd distribution complete: {result.get('sats')} sats"
            )
            return

        error = str(result.get("error") or "")
        if "zero" in error.lower():
            logger.info(
                "Lightning Goats: herd balance already distributed; nothing to transfer"
            )
            return
        raise RuntimeError(f"CyberHerd distribution failed: {error}")

    except ImportError:
        raise RuntimeError("CyberHerd extension not available for payment distribution")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to trigger CyberHerd payment: {e}") from e


async def _process_feeding_batch(
    wallet, settings, payment, openhab, origin, *, resume=False
):
    """Run threshold cycles under the herd lock, resuming transfers after restart."""
    from lnbits.extensions.cyberherd.services.treats import key, record, put

    batch_id = key("feeding-batch", wallet.id)
    batch = await record(batch_id)
    if resume and (
        not batch or batch["state"] != "pending" or batch["data"]["origin"] != origin
    ):
        return
    if batch and batch["state"] == "blocked":
        raise RuntimeError("Feeder outcome uncertain; operator reconciliation required")
    if not batch or batch["state"] == "complete":
        fresh = await get_wallet(wallet.id)
        threshold = settings.feeder_trigger_sats
        if not isinstance(threshold, int) or threshold <= 0:
            raise RuntimeError("Feeder threshold must be positive")
        data = {
            "index": 0,
            "origin": origin,
            "wallet": wallet.id,
            "payment_hash": getattr(payment, "payment_hash", None),
            "payment_sats": payment.amount // 1000,
            "threshold": threshold,
            "cycles": max(0, fresh.balance_msat // (threshold * 1000)),
            "last_feed_at": batch["data"].get("last_feed_at", 0) if batch else 0,
        }
        await put(batch_id, wallet.user, "feeding-batch", "pending", data)
    else:
        data = batch["data"]
    threshold = data["threshold"]
    if not isinstance(threshold, int) or threshold <= 0:
        raise RuntimeError("Feeder threshold must be positive")
    while True:
        # Freeze this deposit's budget; later deposits get their own batch and cap.
        if data["index"] >= data.get("cycles", 0):
            await put(batch_id, wallet.user, "feeding-batch", "complete", data)
            if origin != data["origin"]:
                await _process_feeding_batch(wallet, settings, payment, openhab, origin)
            return
        fresh = await get_wallet(wallet.id)
        feeding_id = key("feed", wallet.id, data["origin"], data["index"])
        saved = await record(feeding_id)
        if saved and saved["state"] == "requested":
            await put(batch_id, wallet.user, "feeding-batch", "blocked", data)
            raise RuntimeError(
                "Feeder request outcome uncertain; operator reconciliation required"
            )
        if saved and saved["state"] == "complete":
            data["index"] += 1
            await put(batch_id, wallet.user, "feeding-batch", "pending", data)
            continue
        if not saved:
            if not fresh or fresh.balance_msat < threshold * 1000:
                await put(batch_id, wallet.user, "feeding-batch", "complete", data)
                return
            if await openhab.is_feeder_override_enabled():
                return
            # Extra threshold cycles are distributions only after the third feed.
            if data["index"] >= MAX_FEEDINGS_PER_PAYMENT:
                await put(
                    feeding_id,
                    wallet.user,
                    "feeding",
                    "payout_only",
                    {"threshold": threshold},
                    insert=True,
                )
                await _distribute_herd_balance(
                    wallet.user,
                    payment=payment,
                    amount_sats=threshold,
                    feeding_id=feeding_id,
                )
                await put(
                    feeding_id,
                    wallet.user,
                    "feeding",
                    "complete",
                    {"threshold": threshold},
                )
                data["index"] += 1
                await put(batch_id, wallet.user, "feeding-batch", "pending", data)
                continue
            cooldown_key = settings.herd_wallet_id or wallet.id
            interval = (
                FEEDER_BATCH_INTERVAL_SECONDS
                if data["index"] > 0
                else FEEDER_COOLDOWN_SECONDS
            )
            delay = interval - (
                time.monotonic() - _last_feed_monotonic.get(cooldown_key, 0.0)
            )
            persisted_delay = interval - (time.time() - data.get("last_feed_at", 0))
            delay = min(interval, max(delay, persisted_delay))
            if delay > 0:
                await asyncio.sleep(delay)
            if await openhab.is_feeder_override_enabled():
                return
            # Re-read after waiting: never feed money that was withdrawn meanwhile.
            fresh = await get_wallet(wallet.id)
            if not fresh or fresh.balance_msat < threshold * 1000:
                await put(batch_id, wallet.user, "feeding-batch", "complete", data)
                return
            await put(
                feeding_id,
                wallet.user,
                "feeding",
                "requested",
                {"threshold": threshold},
                insert=True,
            )
            if not await openhab.trigger_feeder(settings.openhab_feeder_rule_id):
                await put(batch_id, wallet.user, "feeding-batch", "blocked", data)
                raise RuntimeError("Feeder trigger failed; reconcile before retry")
            _last_feed_monotonic[cooldown_key] = time.monotonic()
            from lnbits.extensions.cyberherd import crud as ch_crud

            data["last_feed_at"] = time.time()
            async with ch_crud.admission_transaction():
                await put(
                    feeding_id, wallet.user, "feeding", "fed", {"threshold": threshold}
                )
                await put(batch_id, wallet.user, "feeding-batch", "pending", data)
        if data["index"] == 0:
            queued = await send_feeder_message(
                balance_sats=data["cycles"] * threshold,
                payment_amount=data["payment_sats"],
                user_id=wallet.user,
                payment_id=data["origin"],
                feedings=min(MAX_FEEDINGS_PER_PAYMENT, data["cycles"]),
                payout_cycles=data["cycles"],
            )
            if not queued:
                raise RuntimeError(
                    "Feeder notification could not be queued; retry without feeding again"
                )
        await _distribute_herd_balance(
            wallet.user, payment=payment, amount_sats=threshold, feeding_id=feeding_id
        )
        await put(
            feeding_id, wallet.user, "feeding", "complete", {"threshold": threshold}
        )
        data["index"] += 1
        await put(batch_id, wallet.user, "feeding-batch", "pending", data)


async def resume_feeding_batches():
    """Recover funded cycles without replaying confirmed or uncertain feeder actions."""
    from lnbits.extensions.cyberherd import crud as ch_crud
    from lnbits.extensions.cyberherd.services.treats import decode
    from lnbits.core.crud import get_standalone_payment
    from lnbits.extensions.cyberherd.utils.common import is_extension_enabled_for_user

    while True:
        rows = await ch_crud.db.fetchall(
            f"""SELECT * FROM {ch_crud.db.references_schema}delivery_records
            WHERE kind='feeding-batch' AND state='pending'"""
        )
        for row in rows:
            data = decode(row["payload"])
            wallet = await get_wallet(data["wallet"])
            if (
                not wallet
                or not await is_extension_enabled_for_user(wallet.user)
                or not await is_extension_enabled_for_user(
                    wallet.user, "lightning_goats"
                )
            ):
                continue
            settings = await get_settings(wallet.user, auto_populate=False)
            if not settings or settings.herd_wallet_id != wallet.id:
                continue
            payment = (
                await get_standalone_payment(
                    data["payment_hash"], incoming=True, wallet_id=wallet.id
                )
                if data.get("payment_hash")
                else None
            )
            if not payment or not payment.success:
                continue
            async with _get_herd_lock(wallet.id):
                openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
                try:
                    if not await openhab.is_feeder_override_enabled():
                        await _process_feeding_batch(
                            wallet,
                            settings,
                            payment,
                            openhab,
                            data["origin"],
                            resume=True,
                        )
                except Exception:
                    logger.error(
                        "Lightning Goats: feeding-cycle recovery deferred; check cycle records"
                    )
                finally:
                    await openhab.close()
        await asyncio.sleep(10)


async def _alert_processing_failure(payment: Any, idempotency_key: str, error: str) -> None:
    """Best-effort operator alert when a claimed payment fails to process.

    Sats may have arrived without the feeder firing or the herd being paid, so
    make the failure visible on the herd websocket channel and via a stable
    ERROR log marker instead of only recording it in the database.
    """
    logger.error(
        f"Lightning Goats: PAYMENT_PROCESSING_FAILED idempotency_key={idempotency_key} "
        f"wallet={getattr(payment, 'wallet_id', '?')}: {error}"
    )
    try:
        wallet = await get_wallet(getattr(payment, "wallet_id", None))
        if not wallet:
            return
        from .services.messaging import (
            _broadcast_websocket_message,
            _resolve_websocket_topic,
        )

        topic = await _resolve_websocket_topic(wallet.user)
        await _broadcast_websocket_message(
            topic,
            {
                "type": "processing_error",
                "message": "A herd payment could not be fully processed. Check server logs.",
                "idempotency_key": idempotency_key,
            },
        )
    except Exception as exc:
        logger.debug(f"Lightning Goats: failed to broadcast processing-failure alert: {exc}")


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
        # If it's wrapped like "preimage: <hex>", take the last whitespace-delimited token
        if " " in v:
            candidate = v.split()[-1]
            if all(c in "0123456789abcdef" for c in candidate):
                v = candidate
        # Validate result contains only hex characters
        if v and all(c in "0123456789abcdef" for c in v):
            return v
        return None
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

    # Some LNbits event payloads nest the preimage under `extra`, but only under
    # known preimage keys. Only mine `extra` when it is a dict — a bare hex-like
    # string in `extra` is NOT a preimage and must not be treated as one (it
    # would then fail proof verification and reject a valid payment). `extra`
    # never carries the payment_hash, so we do not derive it from there.
    extra = getattr(payment, "extra", None)
    if preimage is None and isinstance(extra, dict):
        preimage = _coerce_hex(extra)

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
        # datetime object — LNbits' Payment.time / created_at / updated_at are
        # tz-aware UTC datetimes. Without this branch a datetime falls through to
        # `return None`, so every live payment is treated as "unknown timestamp"
        # and dropped by the startup-backlog guard for 180s after each restart.
        if isinstance(value, datetime):
            dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
            return dt.astimezone(_LG_TZ)
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
    herd_lock: Optional[asyncio.Lock] = None
    lock_acquired: bool = False
    try:
        logger.info("Lightning Goats: payment event received")
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
        
        # Get settings for this wallet's user. Do not auto-populate here: that
        # writes to the DB, and the payment handler is a hot path. Settings are
        # populated when the user saves them in the UI.
        settings = await get_settings(wallet.user, auto_populate=False)
        if not settings:
            logger.debug(f"Lightning Goats: no settings for user {wallet.user}")
            return
        if not getattr(settings, "openhab_url", None):
            logger.debug(f"Lightning Goats: OpenHAB URL not configured for user {wallet.user}")
            return
        
        # Check if this payment is for the configured herd wallet. Fail closed:
        # if no herd wallet is configured we cannot confirm this payment belongs
        # to the herd, so we must not trigger the feeder on an unrelated wallet.
        if not settings.herd_wallet_id:
            logger.debug(
                f"Lightning Goats: no herd wallet configured for user {wallet.user}; "
                f"skipping payment to wallet {payment.wallet_id}"
            )
            return

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
            # If we can't determine the timestamp during startup backlog, be
            # conservative and ignore it once — regardless of whether we have a
            # checking_id. Processing an unknown-age payment could re-fire the
            # feeder for a historical invoice.
            if payment_dt is None:
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

        # Serialize processing per herd wallet so two payments crossing the
        # threshold together cannot double-trigger the feeder or double-distribute.
        herd_lock = _get_herd_lock(settings.herd_wallet_id or wallet.id)
        await herd_lock.acquire()
        lock_acquired = True

        # Re-read the wallet under the lock so the threshold decision uses a
        # balance that cannot shift underneath a concurrent payment.
        fresh_wallet = await get_wallet(wallet.id)
        if fresh_wallet:
            wallet = fresh_wallet

        # Get current wallet balance
        balance_sats = wallet.balance_msat // 1000
        logger.info(f"Lightning Goats: handling payment of {amount_sats} sats for user {wallet.user[:8]}. Current balance: {balance_sats} sats.")
        
        # Initialize OpenHAB service
        openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
        processing_succeeded = False
        
        try:
            # Check override state once for use in feeder and messaging decisions
            override_enabled = await openhab.is_feeder_override_enabled()

            # Each full threshold gets a payout cycle; the first three cycles
            # per received payment also request a feeding.
            if settings.feeder_trigger_sats <= 0:
                raise RuntimeError("Feeder threshold must be positive")
            if balance_sats >= settings.feeder_trigger_sats:
                if override_enabled:
                    logger.info("Lightning Goats: OverrideSwitch is ON, skipping feeder")
                else:
                    await _process_feeding_batch(wallet, settings, payment, openhab, idempotency_key)

            elif amount_sats >= settings.minimum_sats:
                # Send payment received message (if enabled and override is off)
                if override_enabled:
                    logger.info("Lightning Goats: OverrideSwitch is ON, skipping payment received message")
                elif settings.interface_messages_enabled:
                    logger.info(f"Lightning Goats: sending payment received message for {amount_sats} sats")
                    msg_success = await send_payment_received_message(
                        amount=amount_sats,
                        balance=balance_sats,
                        trigger_threshold=settings.feeder_trigger_sats,
                        user_id=wallet.user,
                    )
                    logger.info(f"Lightning Goats: payment received message sent: {msg_success}")
                else:
                    logger.debug("Lightning Goats: interface messages disabled")
            else:
                logger.debug(f"Lightning Goats: payment too small ({amount_sats} sats)")

            processing_succeeded = True

        finally:
            # Neither closing the OpenHAB client nor recording the terminal status
            # may escape here: if they did, the outer handler would mark an
            # already-successful, money-moved payment as failed and alert on it.
            try:
                await openhab.close()
            except Exception as close_exc:
                logger.warning(f"Lightning Goats: error closing OpenHAB client: {close_exc}")

            if claimed_payment_hash and not payment_failed and processing_succeeded:
                try:
                    await mark_payment_processed(claimed_payment_hash)
                except Exception as mark_exc:
                    logger.error(
                        f"Lightning Goats: payment {claimed_payment_hash} succeeded but "
                        f"could not be marked processed: {mark_exc}"
                    )

    except Exception as e:
        if claimed_payment_hash and not payment_failed:
            try:
                await mark_payment_failed(claimed_payment_hash, f"Unhandled exception: {e}")
            except Exception:
                pass
            # Surface money-moving failures to operators instead of failing silently.
            await _alert_processing_failure(payment, claimed_payment_hash, str(e))
        logger.error(f"Lightning Goats: Error handling payment: {e}", exc_info=True)
    finally:
        if herd_lock is not None and lock_acquired:
            herd_lock.release()


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
        task = create_permanent_unique_task(INVOICE_LISTENER_TASK_NAME, listener)
        scheduled_tasks.append(task)
        scheduled_tasks.append(create_permanent_unique_task("ext_lightning_goats_feed_recovery", resume_feeding_batches))
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

    # Reconcile any payments left stuck in 'processing' by an interrupted run
    # (crash/restart between claim and terminal status) once at startup.
    try:
        await reconcile_stale_processing_payments()
    except Exception as exc:
        logger.warning(f"Lightning Goats: stale-payment reconciliation failed at startup: {exc}")

    while True:
        try:
            # Wait for the configured interval
            await asyncio.sleep(DEFAULT_WEATHER_BROADCAST_INTERVAL)

            # Periodically reconcile stale 'processing' rows so an interrupted
            # payment does not stay silently stuck.
            try:
                await reconcile_stale_processing_payments()
            except Exception as exc:
                logger.debug(f"Lightning Goats: stale-payment reconciliation failed: {exc}")

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
                    # Check OverrideSwitch — skip all messages when ON
                    if settings.openhab_url and settings.openhab_auth:
                        openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
                        try:
                            if await openhab.is_feeder_override_enabled():
                                logger.debug(f"Lightning Goats: OverrideSwitch is ON for user {user_id}, skipping periodic messages")
                                continue
                        finally:
                            await openhab.close()

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
                                try:
                                    weather_url = ensure_outbound_url_allowed(
                                        weather_url,
                                        "weather station URL",
                                    )
                                except OutboundURLPolicyError as exc:
                                    logger.warning(
                                        f"Lightning Goats: skipping blocked weather URL for user {user_id}: {exc}"
                                    )
                                    continue
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


def start_background_tasks():
    """
    Start all background tasks for Lightning Goats.

    Uses the LNbits permanent-unique-task registry so the loop auto-restarts on
    unexpected errors and is tracked for cancellation on extension stop.
    Note: Bitcoin price history is handled by OpenHAB.
    """
    try:
        task = create_permanent_unique_task(
            PERIODIC_MESSAGES_TASK_NAME, periodic_informational_messages
        )
        scheduled_tasks.append(task)
        logger.info("Lightning Goats: Periodic informational messages task started")

    except Exception as e:
        logger.error(f"Lightning Goats: Failed to start background tasks: {e}", exc_info=True)


async def stop_background_tasks():
    """Stop every task the extension started and unregister the payment listener.

    This must cancel the invoice-listener task and remove it from the core
    invoice-listener registry; otherwise a disabled extension keeps receiving
    paid-invoice events and would continue triggering the feeder and moving
    funds (mirrors cyberherd_stop()).
    """
    logger.info("Stopping Lightning Goats background tasks")

    # Stop routing paid invoices to us before cancelling the listener task.
    try:
        from lnbits.tasks import invoice_listeners

        invoice_listeners.pop(INVOICE_LISTENER_NAME, None)
        logger.info("Lightning Goats: invoice listener unregistered")
    except ImportError:
        # Current LNbits stores deprecated queue forwarders in task_manager.
        from lnbits.tasks import task_manager
        for name in (INVOICE_LISTENER_NAME + "_invoice_listener",
                     "forward_" + INVOICE_LISTENER_NAME + "_invoice_listener"):
            listener = task_manager.get_task(name)
            if listener is not None:
                task_manager.cancel_task(listener)

    for task in scheduled_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as ex:
                logger.warning(f"Lightning Goats: error cancelling task: {ex}")

    scheduled_tasks.clear()
    logger.info("Lightning Goats background tasks stopped")
