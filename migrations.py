"""Database migrations for Lightning Goats extension."""

from lnbits.db import Connection


def _settings_table(db: Connection) -> str:
    """Get the properly prefixed table name for settings."""
    return f"{db.references_schema}settings"


def _processed_payments_table(db: Connection) -> str:
    """Get the properly prefixed table name for processed payments."""
    return f"{db.references_schema}processed_payments"


async def m001_initial(db: Connection):
    """Create initial settings table with complete schema.
    
    Creates the settings table with all columns in their final form.
    Uses INTEGER for booleans (0/1) for SQLite/PostgreSQL compatibility.
    Safe to run on both fresh installs and existing databases.
    """
    table = _settings_table(db)
    
    # Create schema for PostgreSQL (ignored on SQLite)
    try:
        await db.execute("CREATE SCHEMA IF NOT EXISTS lightning_goats;")
    except Exception:
        pass  # SQLite doesn't support schemas
    
    await db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            user_id TEXT PRIMARY KEY,
            openhab_url TEXT DEFAULT '',
            openhab_auth TEXT DEFAULT '',
            openhab_feeder_rule_id TEXT NOT NULL DEFAULT '88bd9ec4de',
            herd_wallet_id TEXT,
            feeder_trigger_sats INTEGER NOT NULL DEFAULT 1000,
            weather_station_url TEXT,
            weather_broadcast_enabled INTEGER NOT NULL DEFAULT 1,
            interface_messages_enabled INTEGER NOT NULL DEFAULT 1
        );
        """
    )


async def m002_reset_settings_schema(db: Connection):
    """No-op migration - replaced by proper m001 initial schema.
    
    This migration previously dropped and recreated the table, which
    would destroy data in production. Now it's a no-op placeholder
    to maintain migration numbering consistency.
    """
    pass  # Table structure is correct from m001


async def m003_make_openhab_fields_optional(db: Connection):
    """No-op migration - openhab_url and openhab_auth already optional in m001.
    
    This migration previously dropped and recreated the table using
    SQLite-specific logic. The m001 migration now creates the table
    with optional openhab fields from the start.
    """
    pass  # Schema is correct from m001


async def m004_add_minimum_sats(db: Connection):
    """Add minimum_sats column to settings table."""
    table = _settings_table(db)
    
    # Check if column already exists
    try:
        # SQLite doesn't support IF NOT EXISTS for ADD COLUMN
        # We try to add it and catch the error if it already exists
        await db.execute(f"ALTER TABLE {table} ADD COLUMN minimum_sats INTEGER NOT NULL DEFAULT 10;")
    except Exception as e:
        # If column already exists, ignore the error
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            pass
        else:
            raise


async def m005_create_processed_payments(db: Connection):
    """Create processed_payments table for exactly-once payment processing.

    We store payment_hash as the idempotency key and persist the payment proof
    (preimage) for auditing/verification.
    """
    table = _processed_payments_table(db)

    # Create schema for PostgreSQL (ignored on SQLite)
    try:
        await db.execute("CREATE SCHEMA IF NOT EXISTS lightning_goats;")
    except Exception:
        pass

    await db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            payment_hash TEXT PRIMARY KEY,
            checking_id TEXT,
            wallet_id TEXT NOT NULL,
            amount_msat INTEGER NOT NULL,
            preimage TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'processing',
            error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Best-effort uniqueness for checking_id as well.
    try:
        await db.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_lg_processed_payments_checking_id ON {table} (checking_id);")
    except Exception:
        # SQLite versions without IF NOT EXISTS may throw; ignore.
        pass
