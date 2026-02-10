"""Configuration constants for Lightning Goats extension."""

# Extension metadata
EXTENSION_NAME = "lightning_goats"
EXTENSION_TITLE = "Lightning Goats"
EXTENSION_DESCRIPTION = "IoT integration for Lightning Goats feeding automation"

# Default values
DEFAULT_GOAT_NAMES = ["Dexter", "Rowan", "Nova", "Cosmo", "Newton"]

# Weather station defaults
DEFAULT_WEATHER_URL = "http://192.168.1.161:5000/get_received_data"
DEFAULT_WEATHER_BROADCAST_INTERVAL = 60  # seconds
DEFAULT_WEATHER_BROADCAST_PROBABILITY = 0.4  # 40% chance per interval

# OpenHAB defaults
# Note: These can be overridden in settings
DEFAULT_OPENHAB_FEEDER_RULE_ID = "88bd9ec4de"
DEFAULT_OPENHAB_FEEDER_OVERRIDE_ITEM = "FeederOverride"

# Feeder trigger defaults
# Note: DEFAULT_FEEDER_TRIGGER_SATS is fetched from CyberHerd extension settings
# If CyberHerd is not available or doesn't have feeder_trigger_sats set, fall back to this:
FALLBACK_FEEDER_TRIGGER_SATS = 1000
