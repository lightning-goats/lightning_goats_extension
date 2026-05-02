"""API routes for Lightning Goats extension."""

import inspect
from typing import Any, cast
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from lnbits.core.crud import get_wallet, get_wallets
from lnbits.core.crud.extensions import get_user_active_extensions_ids
from lnbits.core.models import WalletTypeInfo
from lnbits.decorators import require_admin_key

from .crud import get_settings, upsert_settings, delete_settings, get_cyberherd_settings
from .models import (
    LightningGoatsSettings,
    LightningGoatsSettingsUpdate,
    BitcoinPriceData,
    FeederTriggerRequest,
)
from .services.bitcoin_price import get_bitcoin_price_data
from .services.openhab import OpenHABService
from .services.url_validation import OutboundURLPolicyError, ensure_outbound_url_allowed


lightning_goats_api_router = APIRouter()


def is_operationally_configured(settings: LightningGoatsSettings | None) -> bool:
    """Return true when settings are complete enough to call OpenHAB."""

    return bool(settings and settings.openhab_url and settings.openhab_url.strip())


async def ensure_user_wallet(user_id: str, wallet_id: str | None):
    """Return wallet_id's wallet only if it belongs to user_id."""

    if not wallet_id:
        return None
    wallet_obj = await get_wallet(wallet_id)
    if not wallet_obj:
        raise HTTPException(status_code=404, detail="Wallet not found")
    if wallet_obj.user != user_id:
        raise HTTPException(status_code=400, detail="Herd wallet does not belong to this user")
    return wallet_obj


async def check_extension_enabled(wallet: WalletTypeInfo = Depends(require_admin_key)) -> WalletTypeInfo:
    """
    Check if Lightning Goats extension is enabled for the user.
    
    This dependency ensures users must explicitly enable the extension
    before accessing its API endpoints, following LNbits architecture patterns.
    """
    user_id = wallet.wallet.user
    active_extensions = await get_user_active_extensions_ids(user_id)
    
    if "lightning_goats" not in active_extensions:
        raise HTTPException(
            status_code=403,
            detail="Lightning Goats extension is not enabled. Please enable it in the Extension Manager."
        )
    
    return wallet


async def _get_cyberherd_active_count(user_id: str) -> int:
    """Return active CyberHerd member count using the available CRUD helper."""
    try:
        from lnbits.extensions.cyberherd import crud as cyberherd_crud
    except ImportError as exc:
        raise HTTPException(status_code=503, detail="CyberHerd extension not installed") from exc

    size_fn = getattr(cyberherd_crud, "get_active_cyberherd_size", None)
    if callable(size_fn):
        result = size_fn(user_id=user_id)
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            return 0
        if isinstance(result, (int, float)):
            return int(result)
        if isinstance(result, str):
            try:
                return int(float(result))
            except ValueError:
                pass
        logger.debug("Unexpected active size result type: %s", type(result))
        raise HTTPException(status_code=500, detail="Invalid active member count from CyberHerd")

    members_fn = getattr(cyberherd_crud, "get_active_cyberherd_members", None)
    if callable(members_fn):
        result = members_fn(user_id=user_id)
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            return 0
        if isinstance(result, (list, tuple, set)):
            return len(result)
        try:
            return len(list(result))  # type: ignore[arg-type]
        except TypeError:
            logger.debug("Unexpected active members result type: %s", type(result))
            raise HTTPException(status_code=500, detail="Invalid active members data from CyberHerd")

    raise HTTPException(status_code=503, detail="CyberHerd extension missing active member helper")


@lightning_goats_api_router.get("/api/v1/wallets")
async def get_user_wallets_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """Get all wallets for current user."""
    try:
        wallets = await get_wallets(wallet.wallet.user)
        return [{"id": w.id, "name": w.name, "inkey": w.inkey} for w in wallets]
    except Exception as e:
        logger.error(f"Failed to get user wallets: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user wallets")


@lightning_goats_api_router.get("/api/v1/cyberherd_defaults")
async def get_cyberherd_defaults_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """Get default settings from CyberHerd extension."""
    try:
        cyberherd_settings = await get_cyberherd_settings(wallet.wallet.user)
        if not cyberherd_settings:
            return {"herd_wallet_id": None, "feeder_trigger_sats": None}
        
        return {
            "herd_wallet_id": cyberherd_settings.get("herd_wallet"),
            "feeder_trigger_sats": cyberherd_settings.get("feeder_trigger_sats"),
        }
    except Exception as e:
        logger.error(f"Failed to get CyberHerd defaults: {e}")
        return {"herd_wallet_id": None, "feeder_trigger_sats": None}


@lightning_goats_api_router.get("/api/v1/settings")
async def get_settings_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
) -> LightningGoatsSettings:
    """Get Lightning Goats settings for current user.
    
    Returns stored settings if they exist, otherwise returns default settings.
    This ensures the UI always has settings to work with.
    """
    settings = await get_settings(wallet.wallet.user)
    # get_settings now always returns a settings object (never None)
    return settings


@lightning_goats_api_router.put("/api/v1/settings")
async def update_settings_route(
    payload: LightningGoatsSettingsUpdate,
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """Create or update Lightning Goats settings."""
    logger.info(f"Lightning Goats: Received settings update request for user {wallet.wallet.user}")
    
    # Convert payload to dict - Pydantic v2 compatible
    payload_dict: dict[str, Any]
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        payload_dict = cast(dict[str, Any], model_dump())
    else:
        payload_dict = cast(dict[str, Any], payload.dict())
    
    redacted_payload = {**payload_dict, "openhab_auth": "***" if payload_dict.get("openhab_auth") else ""}
    logger.debug(f"Lightning Goats: Payload dict: {redacted_payload}")
    
    settings = LightningGoatsSettings(user_id=wallet.wallet.user, **payload_dict)
    
    logger.debug(f"Lightning Goats: Created settings object: user_id={settings.user_id}, openhab_url={settings.openhab_url}")
    
    # Validate OpenHAB URL only if provided
    if settings.openhab_url:
        try:
            settings.openhab_url = ensure_outbound_url_allowed(settings.openhab_url, "OpenHAB URL")
        except OutboundURLPolicyError as exc:
            logger.warning("Lightning Goats: invalid OpenHAB URL")
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Validate weather station URL if provided
    if settings.weather_station_url:
        try:
            settings.weather_station_url = ensure_outbound_url_allowed(
                settings.weather_station_url, "weather station URL"
            )
        except OutboundURLPolicyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    await ensure_user_wallet(wallet.wallet.user, settings.herd_wallet_id)

    # Validate minimum sats
    if settings.minimum_sats < 1 or settings.minimum_sats > 100_000_000:
        logger.warning(f"Lightning Goats: Invalid minimum sats: {settings.minimum_sats}")
        raise HTTPException(status_code=400, detail="Minimum sats must be between 1 and 100,000,000")

    # Validate feeder trigger amount
    if settings.feeder_trigger_sats < 1 or settings.feeder_trigger_sats > 100_000_000:
        logger.warning(f"Lightning Goats: Invalid feeder trigger amount: {settings.feeder_trigger_sats}")
        raise HTTPException(status_code=400, detail="Feeder trigger must be between 1 and 100,000,000 sats")
    
    try:
        result = await upsert_settings(settings)
        logger.info(f"Lightning Goats: Settings updated successfully for user {wallet.wallet.user}")
        return result
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to update settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to update settings")


@lightning_goats_api_router.delete("/api/v1/settings")
async def delete_settings_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """Delete Lightning Goats settings for current user."""
    try:
        await delete_settings(wallet.wallet.user)
        logger.info(f"Settings deleted for user {wallet.wallet.user}")
        return {"success": True, "message": "Settings deleted"}
    except Exception as e:
        logger.error(f"Failed to delete settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete settings")


@lightning_goats_api_router.get("/api/v1/balance")
async def get_balance_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """
    Get herd wallet balance.
    
    Uses internal LNbits wallet function instead of HTTP call.
    Returns balance from configured herd_wallet if available.
    """
    try:
        # Get settings to find herd wallet
        settings = await get_settings(wallet.wallet.user)
        
        # Use herd wallet if configured, otherwise fall back to admin wallet
        wallet_obj = (
            await ensure_user_wallet(wallet.wallet.user, settings.herd_wallet_id)
            if settings and settings.herd_wallet_id
            else await get_wallet(wallet.wallet.id)
        )
        if not wallet_obj:
            raise HTTPException(status_code=404, detail="Wallet not found")
        return {"balance": wallet_obj.balance_msat, "balance_sats": wallet_obj.balance_msat // 1000}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get balance: {e}")
        raise HTTPException(status_code=500, detail="Failed to get balance")


@lightning_goats_api_router.get("/api/v1/trigger_amount")
async def get_trigger_amount_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """Get feeder trigger threshold from settings."""
    settings = await get_settings(wallet.wallet.user)
    if not settings:
        # Return default from CyberHerd or fallback
        from .crud import get_default_feeder_trigger_sats
        default_trigger = await get_default_feeder_trigger_sats(wallet.wallet.user)
        return {"trigger_amount": default_trigger}
    return {"trigger_amount": settings.feeder_trigger_sats}


@lightning_goats_api_router.get("/api/v1/cyberherd/active_count")
async def get_active_count_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """
    Get active CyberHerd member count.
    
    Uses internal cyberherd function instead of HTTP call.
    """
    try:
        count = await _get_cyberherd_active_count(wallet.wallet.user)
        return {"active_count": count}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get active count: {e}")
        raise HTTPException(status_code=500, detail="Failed to get active member count")


@lightning_goats_api_router.get("/api/v1/bitcoin_data")
async def get_bitcoin_data_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
) -> BitcoinPriceData:
    """
    Get Bitcoin price and 24h change from OpenHAB.
    """
    settings = await get_settings(wallet.wallet.user)
    if not is_operationally_configured(settings):
        raise HTTPException(status_code=404, detail="Settings not configured")
    
    openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
    
    try:
        return await get_bitcoin_price_data(openhab)
    except Exception as e:
        logger.error(f"Failed to get Bitcoin data: {e}")
        raise HTTPException(status_code=500, detail="Failed to get Bitcoin price data")
    finally:
        await openhab.close()


@lightning_goats_api_router.post("/api/v1/trigger_feeder")
async def trigger_feeder_manual(
    request: FeederTriggerRequest,
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """
    Manually trigger the feeder.
    
    Optionally bypass the override check.
    """
    settings = await get_settings(wallet.wallet.user)
    if not is_operationally_configured(settings):
        raise HTTPException(status_code=404, detail="Settings not configured")
    
    openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
    
    try:
        # Check override unless bypassed
        if not request.override_check:
            if await openhab.is_feeder_override_enabled():
                raise HTTPException(
                    status_code=400,
                    detail="Feeder override is enabled. Disable override or use override_check=true"
                )
        
        # Trigger feeder
        success = await openhab.trigger_feeder(settings.openhab_feeder_rule_id)
        if success:
            logger.info(f"Manual feeder trigger by user {wallet.wallet.user}")
            
            # Send notification
            from .services.messaging import send_manual_feeder_notification
            await send_manual_feeder_notification(user_id=wallet.wallet.user)
            
            return {"success": True, "message": "Feeder triggered successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to trigger feeder")
            
    finally:
        await openhab.close()


@lightning_goats_api_router.get("/api/v1/herd_wallets")
async def get_herd_wallets_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """
    Get available herd wallets from CyberHerd settings.
    
    Used to populate wallet dropdown in settings UI.
    """
    try:
        # Import here to avoid issues if cyberherd not installed
        from lnbits.extensions.cyberherd.crud import get_settings as get_ch_settings

        settings = await get_ch_settings(wallet.wallet.user)
        herd_wallet_id = getattr(settings, "herd_wallet", None)
        if not herd_wallet_id:
            return {"wallets": []}
        
        # Return herd wallet info
        wallets = []
        wallet_obj = await get_wallet(herd_wallet_id)
        if wallet_obj:
            wallets.append({
                "id": wallet_obj.id,
                "name": wallet_obj.name or "Herd Wallet",
                "balance": wallet_obj.balance_msat // 1000,
            })
        
        return {"wallets": wallets}
        
    except ImportError:
        logger.warning("CyberHerd extension not available")
        return {"wallets": []}
    except Exception as e:
        logger.error(f"Failed to get herd wallets: {e}")
        return {"wallets": []}


@lightning_goats_api_router.get("/api/v1/status")
async def get_status_route(
    wallet: WalletTypeInfo = Depends(check_extension_enabled),
):
    """
    Get Lightning Goats status information.
    
    Combines multiple data sources for dashboard display.
    """
    try:
        # Get settings
        settings = await get_settings(wallet.wallet.user)
        
        # Get balance from herd wallet if configured, otherwise from admin wallet
        balance_sats = 0
        if settings and settings.herd_wallet_id:
            herd_wallet = await ensure_user_wallet(wallet.wallet.user, settings.herd_wallet_id)
            balance_sats = herd_wallet.balance_msat // 1000 if herd_wallet else 0
        else:
            wallet_obj = await get_wallet(wallet.wallet.id)
            balance_sats = wallet_obj.balance_msat // 1000 if wallet_obj else 0
        
        # Get trigger amount
        trigger_amount = settings.feeder_trigger_sats if settings else 1000
        
        # Get active member count from CyberHerd if available
        active_count = 0
        try:
            active_count = await _get_cyberherd_active_count(wallet.wallet.user)
        except HTTPException as exc:
            if exc.status_code == 503:
                logger.debug("CyberHerd active count unavailable: %s", exc.detail)
            else:
                raise
        except Exception as e:
            logger.debug(f"Could not get active member count from CyberHerd: {e}")
        
        # Get Bitcoin price and override status from OpenHAB
        btc_price = None
        btc_change = None
        override_enabled = False
        if is_operationally_configured(settings):
            openhab = OpenHABService(settings.openhab_url, settings.openhab_auth)
            try:
                price_data = await get_bitcoin_price_data(openhab)
                btc_price = price_data.btc_usd_price
                btc_change = price_data.btc_price_24h_percent_change
                try:
                    override_enabled = await openhab.is_feeder_override_enabled()
                except Exception as e:
                    logger.debug(f"Could not check override status: {e}")
            except Exception as e:
                logger.debug(f"Could not get Bitcoin price: {e}")
            finally:
                await openhab.close()

        return {
            "configured": is_operationally_configured(settings),
            "balance_sats": balance_sats,
            "trigger_amount": trigger_amount,
            "progress_percent": min(100, int((balance_sats / trigger_amount) * 100)) if trigger_amount > 0 else 0,
            "active_members": active_count,
            "btc_price_usd": btc_price,
            "btc_24h_change": btc_change,
            "override_enabled": override_enabled,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get status")
