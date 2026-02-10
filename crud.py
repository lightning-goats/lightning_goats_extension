"""Database operations for Lightning Goats extension."""

from typing import Optional, List

from lnbits.db import Database
from loguru import logger

from .models import LightningGoatsSettings


def _settings_table(db: Database) -> str:
    """Return settings table name with schema prefix where required."""
    return f"{db.references_schema}settings"

db = Database("ext_lightning_goats")

def _processed_payments_table(db: Database) -> str:
    """Return processed_payments table name with schema prefix where required."""
    return f"{db.references_schema}processed_payments"


async def try_claim_payment(
    *,
    payment_hash: str,
    wallet_id: str,
    amount_msat: int,
    preimage: str,
    checking_id: str | None = None,
) -> bool:
    """Atomically claim a payment for processing.

    Returns True exactly once for a given payment_hash (idempotency key).
    Subsequent calls return False.

    We persist the payment proof (preimage) so we can (a) audit, and (b)
    verify authenticity when available.
    """
    table = _processed_payments_table(db)
    try:
        await db.execute(
            f"""
            INSERT INTO {table}
                (payment_hash, checking_id, wallet_id, amount_msat, preimage, status)
            VALUES
                (:payment_hash, :checking_id, :wallet_id, :amount_msat, :preimage, 'processing')
            """,
            {
                "payment_hash": payment_hash,
                "checking_id": checking_id,
                "wallet_id": wallet_id,
                "amount_msat": amount_msat,
                "preimage": preimage,
            },
        )
        return True
    except Exception as e:
        # SQLite: "UNIQUE constraint failed", Postgres: "duplicate key value"
        msg = str(e).lower()
        if "unique" in msg or "duplicate" in msg or "constraint" in msg or "already exists" in msg:
            return False
        raise


async def mark_payment_processed(payment_hash: str) -> None:
    """Mark a claimed payment as fully processed."""
    table = _processed_payments_table(db)
    await db.execute(
        f"UPDATE {table} SET status='processed', error=NULL WHERE payment_hash=:payment_hash",
        {"payment_hash": payment_hash},
    )


async def mark_payment_failed(payment_hash: str, error: str) -> None:
    """Mark a claimed payment as failed (we still keep it idempotent)."""
    table = _processed_payments_table(db)
    await db.execute(
        f"UPDATE {table} SET status='failed', error=:error WHERE payment_hash=:payment_hash",
        {"payment_hash": payment_hash, "error": error[:5000]},
    )


async def was_payment_processed(payment_hash: str) -> bool:
    """Fast check (non-atomic). Prefer try_claim_payment for race-free behavior."""
    table = _processed_payments_table(db)
    row = await db.fetchone(
        f"SELECT 1 FROM {table} WHERE payment_hash=:payment_hash",
        {"payment_hash": payment_hash},
    )
    return row is not None



# Settings CRUD
async def get_settings(user_id: str, auto_populate: bool = True) -> LightningGoatsSettings:
    """Get settings for a user.
    
    Returns stored settings if they exist, otherwise returns default settings.
    If auto_populate=True and no settings exist, will fetch defaults from CyberHerd
    and automatically save them.
    
    Args:
        user_id: User ID to fetch settings for
        auto_populate: If True, auto-populate and save defaults from CyberHerd on first load
        
    Returns:
        LightningGoatsSettings instance (never None)
    """
    try:
        table = _settings_table(db)
        row = await db.fetchone(
            f"SELECT * FROM {table} WHERE user_id = :user_id",
            {"user_id": user_id},
        )
        if row:
            logger.debug(f"Lightning Goats: Found settings for user {user_id}")
            # Convert row to dict and handle boolean conversions
            row_dict = dict(row)
            # Convert SQLite integers to Python booleans
            if "weather_broadcast_enabled" in row_dict:
                row_dict["weather_broadcast_enabled"] = bool(row_dict["weather_broadcast_enabled"])
            if "interface_messages_enabled" in row_dict:
                row_dict["interface_messages_enabled"] = bool(row_dict["interface_messages_enabled"])
            return LightningGoatsSettings(**row_dict)
        
        # No settings found - get defaults from CyberHerd if available
        logger.debug(f"Lightning Goats: No settings found for user {user_id}")
        
        # Try to get CyberHerd settings for defaults
        herd_wallet_id = None
        feeder_trigger_sats = 1000
        minimum_sats = 10
        
        if auto_populate:
            cyberherd_settings = await get_cyberherd_settings(user_id)
            if cyberherd_settings:
                logger.info(f"Lightning Goats: Found CyberHerd settings for user {user_id}")
                herd_wallet_id = cyberherd_settings.get("herd_wallet")
                
                ch_trigger_sats = cyberherd_settings.get("feeder_trigger_sats")
                if ch_trigger_sats and ch_trigger_sats > 0:
                    feeder_trigger_sats = ch_trigger_sats
                
                ch_min_sats = cyberherd_settings.get("minimum_sats")
                if ch_min_sats is not None:
                    minimum_sats = ch_min_sats
                
                logger.info(f"Lightning Goats: Auto-populated herd_wallet_id={herd_wallet_id}, feeder_trigger_sats={feeder_trigger_sats}, minimum_sats={minimum_sats}")
        
        # Create default settings
        default_settings = LightningGoatsSettings(
            user_id=user_id,
            openhab_url="",
            openhab_auth="",
            openhab_feeder_rule_id="88bd9ec4de",
            herd_wallet_id=herd_wallet_id,
            feeder_trigger_sats=feeder_trigger_sats,
            weather_station_url=None,
            weather_broadcast_enabled=True,
            interface_messages_enabled=True,
            minimum_sats=minimum_sats,
        )
        
        # If auto_populate is enabled and we got values from CyberHerd, save them
        if auto_populate and (herd_wallet_id or feeder_trigger_sats != 1000 or minimum_sats != 10):
            logger.info(f"Lightning Goats: Auto-saving defaults from CyberHerd for user {user_id}")
            try:
                await upsert_settings(default_settings)
            except Exception as save_error:
                logger.error(f"Lightning Goats: Failed to auto-save defaults: {save_error}")
                # Continue anyway - return the defaults even if save fails
        
        return default_settings
        
    except Exception as e:
        # If table doesn't exist yet, return defaults
        msg = str(e).lower()
        if "no such table" in msg or "does not exist" in msg or "undefinedtable" in msg:
            logger.warning(f"Lightning Goats: Settings table doesn't exist yet (migrations not run?): {e}")
            return LightningGoatsSettings(
                user_id=user_id,
                openhab_url="",
                openhab_auth="",
                openhab_feeder_rule_id="88bd9ec4de",
                herd_wallet_id=None,
                feeder_trigger_sats=1000,
                weather_station_url=None,
                weather_broadcast_enabled=True,
                interface_messages_enabled=True,
                minimum_sats=10,
            )
        # For other errors, re-raise
        raise


async def get_all_settings() -> List[LightningGoatsSettings]:
    """Return all persisted Lightning Goats settings records.

    Returns an empty list when the settings table has not been created yet
    (e.g. before migrations run) to keep background jobs resilient.
    """

    table = _settings_table(db)

    try:
        rows = await db.fetchall(f"SELECT * FROM {table}")
    except Exception as exc:
        message = str(exc).lower()
        if "no such table" in message or "does not exist" in message or "undefinedtable" in message:
            logger.debug("Lightning Goats: settings table missing when listing all settings")
            return []
        raise

    settings_list: List[LightningGoatsSettings] = []
    for row in rows:
        row_dict = dict(row)
        if "weather_broadcast_enabled" in row_dict:
            row_dict["weather_broadcast_enabled"] = bool(row_dict["weather_broadcast_enabled"])
        if "interface_messages_enabled" in row_dict:
            row_dict["interface_messages_enabled"] = bool(row_dict["interface_messages_enabled"])
        settings_list.append(LightningGoatsSettings(**row_dict))

    return settings_list


async def upsert_settings(settings: LightningGoatsSettings) -> LightningGoatsSettings:
    """Create or update settings.
    
    Args:
        settings: Settings object to save
        
    Returns:
        The saved settings object
        
    Raises:
        Exception: If database operation fails
    """
    table = _settings_table(db)
    
    try:
        logger.info(f"Lightning Goats: Upserting settings for user {settings.user_id}")
        logger.debug(f"Lightning Goats: Settings data: openhab_url={settings.openhab_url}, feeder_trigger_sats={settings.feeder_trigger_sats}")
        
        # Convert boolean values to integers for SQLite
        params = {
            "user_id": settings.user_id,
            "openhab_url": settings.openhab_url,
            "openhab_auth": settings.openhab_auth,
            "openhab_feeder_rule_id": settings.openhab_feeder_rule_id,
            "herd_wallet_id": settings.herd_wallet_id,
            "feeder_trigger_sats": settings.feeder_trigger_sats,
            "weather_station_url": settings.weather_station_url,
            "weather_broadcast_enabled": int(settings.weather_broadcast_enabled),
            "interface_messages_enabled": int(settings.interface_messages_enabled),
            "minimum_sats": settings.minimum_sats,
        }
        
        await db.execute(
            f"""
            INSERT INTO {table} 
            (user_id, openhab_url, openhab_auth, openhab_feeder_rule_id, herd_wallet_id, 
             feeder_trigger_sats, weather_station_url, weather_broadcast_enabled, 
             interface_messages_enabled, minimum_sats)
            VALUES (:user_id, :openhab_url, :openhab_auth, :openhab_feeder_rule_id, :herd_wallet_id, 
                    :feeder_trigger_sats, :weather_station_url, :weather_broadcast_enabled, 
                    :interface_messages_enabled, :minimum_sats)
            ON CONFLICT(user_id) DO UPDATE SET
                openhab_url = :openhab_url,
                openhab_auth = :openhab_auth,
                openhab_feeder_rule_id = :openhab_feeder_rule_id,
                herd_wallet_id = :herd_wallet_id,
                feeder_trigger_sats = :feeder_trigger_sats,
                weather_station_url = :weather_station_url,
                weather_broadcast_enabled = :weather_broadcast_enabled,
                interface_messages_enabled = :interface_messages_enabled,
                minimum_sats = :minimum_sats
            """,
            params,
        )
        logger.info(f"Lightning Goats: Successfully saved settings for user {settings.user_id}")
        return settings
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to upsert settings for user {settings.user_id}: {e}")
        raise


async def delete_settings(user_id: str) -> None:
    """Delete settings for a user."""
    table = _settings_table(db)
    await db.execute(
        f"DELETE FROM {table} WHERE user_id = :user_id",
        {"user_id": user_id}
    )


async def get_cyberherd_settings(user_id: str) -> Optional[dict]:
    """
    Fetch CyberHerd settings for a user.
    
    This retrieves settings from the CyberHerd extension, including
    feeder_trigger_sats and herd_wallet which are used as defaults for Lightning Goats.
    
    Args:
        user_id: User ID to fetch settings for
        
    Returns:
        Dictionary with CyberHerd settings, or None if not available
    """
    try:
        from lnbits.extensions.cyberherd.crud import get_settings as get_ch_settings
        
        settings = await get_ch_settings(user_id)
        if not settings:
            return None
            
        # Convert Pydantic model to dict
        # Try Pydantic v2 method first, then v1
        try:
            return settings.model_dump()  # Pydantic v2  # type: ignore[attr-defined]
        except AttributeError:
            try:
                return settings.dict()  # Pydantic v1
            except AttributeError:
                # Last resort: manually convert using model fields
                result = {}
                for field_name in settings.__fields__:
                    result[field_name] = getattr(settings, field_name, None)
                return result
            
    except ImportError:
        logger.warning("CyberHerd extension not available")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch CyberHerd settings: {e}")
        return None


async def get_default_feeder_trigger_sats(user_id: str) -> int:
    """
    Get default feeder trigger amount from CyberHerd settings.
    
    Falls back to FALLBACK_FEEDER_TRIGGER_SATS if CyberHerd is not available
    or doesn't have the setting.
    
    Args:
        user_id: User ID to fetch settings for
        
    Returns:
        Feeder trigger amount in sats
    """
    from .config import FALLBACK_FEEDER_TRIGGER_SATS
    
    cyberherd_settings = await get_cyberherd_settings(user_id)
    
    if cyberherd_settings and isinstance(cyberherd_settings, dict):
        trigger_sats = cyberherd_settings.get("feeder_trigger_sats")
        if isinstance(trigger_sats, int) and trigger_sats > 0:
            logger.info(f"Using feeder_trigger_sats from CyberHerd: {trigger_sats}")
            return trigger_sats
    
    logger.info(f"Using fallback feeder_trigger_sats: {FALLBACK_FEEDER_TRIGGER_SATS}")
    return FALLBACK_FEEDER_TRIGGER_SATS
