"""Pydantic models for Lightning Goats extension."""

from typing import Optional
from pydantic import BaseModel, Field


class LightningGoatsSettingsBase(BaseModel):
    """Base fields shared by stored settings and incoming payload."""

    openhab_url: str = ""  # Allow empty string, validate only when actually used
    openhab_auth: str = Field(default="", repr=False)  # repr=False prevents credential leak in logs
    openhab_feeder_rule_id: str = "88bd9ec4de"  # OpenHAB rule ID for feeder
    herd_wallet_id: Optional[str] = None  # From cyberherd settings
    feeder_trigger_sats: int = 1000  # From cyberherd settings or user override
    weather_station_url: Optional[str] = None
    weather_broadcast_enabled: bool = True
    interface_messages_enabled: bool = True
    minimum_sats: int = 10



class LightningGoatsSettings(LightningGoatsSettingsBase):
    """Persisted Lightning Goats settings."""

    user_id: str


class LightningGoatsSettingsUpdate(LightningGoatsSettingsBase):
    """Incoming payload for updating settings (user_id derived from auth)."""

    pass


class BitcoinPriceData(BaseModel):
    """Bitcoin price data with 24h change."""
    
    btc_usd_price: float
    btc_price_24h_percent_change: Optional[float] = None
    last_updated: int  # Unix timestamp


class FeederTriggerRequest(BaseModel):
    """Manual feeder trigger request."""
    
    override_check: bool = False


class WeatherData(BaseModel):
    """Weather station data."""
    
    temperature: int
    humidity: int
    wind_speed: int
    wind_direction: str
    uv_index: int
