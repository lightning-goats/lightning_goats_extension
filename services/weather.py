"""Weather station integration service."""

import httpx
from typing import Optional, Dict, Any
from loguru import logger


def _first_number(data: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_string(data: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


async def fetch_weather_data(url: str) -> Optional[Dict[str, Any]]:
    """
    Fetch weather data from station.
    
    Args:
        url: Weather station URL
        
    Returns:
        Dictionary with weather data, or None if error
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=3.0)
            response.raise_for_status()
            data = response.json()
            
            # Handle both dict and list responses
            latest = data if isinstance(data, dict) else (data[-1] if data else {})
            
            # Extract weather fields with multiple possible keys
            temperature = _first_number(
                latest,
                "AmbientWeatherWS2902A_WeatherDataWs2902a_Temperature",
                "tempf",
            )
            humidity = _first_number(
                latest,
                "AmbientWeatherWS2902A_WeatherDataWs2902a_RelativeHumidity",
                "humidity",
            )
            wind_speed = _first_number(
                latest,
                "AmbientWeatherWS2902A_WindSpeed",
                "windspeedmph",
            )
            uv_index = _first_number(
                latest,
                "AmbientWeatherWS2902A_UVIndex",
                "uv",
            )

            weather_data: Dict[str, Any] = {
                "temperature": int(round(temperature)) if temperature is not None else 0,
                "humidity": int(round(humidity)) if humidity is not None else 0,
                "wind_speed": int(round(wind_speed)) if wind_speed is not None else 0,
                "wind_direction": _first_string(
                    latest,
                    "AmbientWeatherWS2902A_WindDirectionCardinal",
                    "winddir",
                )
                or "unknown",
                "uv_index": int(round(uv_index)) if uv_index is not None else 0,
            }

            apparent_temp = _first_number(
                latest,
                "AmbientWeatherWS2902A_ApparentTemperature",
                "feelslike",
                "feelslikef",
            )
            if apparent_temp is not None:
                weather_data["apparent_temperature"] = round(apparent_temp, 1)

            wind_gust = _first_number(
                latest,
                "AmbientWeatherWS2902A_WindGust",
                "windgustmph",
            )
            if wind_gust is not None:
                weather_data["wind_gust"] = round(wind_gust, 1)

            pressure_relative = _first_number(
                latest,
                "AmbientWeatherWS2902A_WeatherDataWs2902A_PressureRelative",
                "baromrelin",
                "baromrelinin",
            )
            if pressure_relative is not None:
                weather_data["pressure_relative"] = round(pressure_relative, 2)

            pressure_trend = _first_string(
                latest,
                "AmbientWeatherWS2902A_PressureTrend",
                "pressuretrend",
            )
            if pressure_trend:
                weather_data["pressure_trend"] = pressure_trend

            rain_hourly = _first_number(
                latest,
                "AmbientWeatherWS2902A_RainFallHourlyRate",
                "hourlyrainin",
            )
            if rain_hourly is not None:
                weather_data["rain_hourly"] = round(rain_hourly, 2)

            rain_daily = _first_number(
                latest,
                "AmbientWeatherWS2902A_RainFallDay",
                "dailyrainin",
            )
            if rain_daily is not None:
                weather_data["rain_daily"] = round(rain_daily, 2)

            solar_radiation = _first_number(
                latest,
                "AmbientWeatherWS2902A_SolarRadiation",
                "solarradiation",
            )
            if solar_radiation is not None:
                weather_data["solar_radiation"] = round(solar_radiation, 1)
            
            logger.debug(f"Fetched weather data: {weather_data['temperature']}°F")
            return weather_data
            
    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching weather data: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        return None


def format_weather_message(weather: Dict[str, Any]) -> str:
    """Format weather data into a descriptive message for websocket clients."""

    temperature = weather.get("temperature", 0)
    humidity = weather.get("humidity", 0)
    wind_speed = weather.get("wind_speed", 0)
    wind_direction = weather.get("wind_direction", "unknown")
    uv_index = weather.get("uv_index", 0)

    primary = []

    apparent = weather.get("apparent_temperature")
    if apparent is not None:
        primary.append(f"{temperature}°F (feels like {apparent:.1f}°F)")
    else:
        primary.append(f"{temperature}°F")

    primary.append(f"{humidity}% humidity")

    wind_text = f"{wind_speed} mph wind from {wind_direction}"
    wind_gust = weather.get("wind_gust")
    if wind_gust and wind_gust > wind_speed:
        wind_text += f", gusts up to {wind_gust:.1f} mph"
    primary.append(wind_text)

    primary.append(f"UV index {uv_index}")

    extras = []

    pressure_relative = weather.get("pressure_relative")
    pressure_trend = weather.get("pressure_trend")
    if pressure_relative is not None and pressure_trend:
        extras.append(
            f"{pressure_trend.capitalize()} pressure {pressure_relative:.2f} inHg"
        )
    elif pressure_relative is not None:
        extras.append(f"Pressure {pressure_relative:.2f} inHg")
    elif pressure_trend:
        extras.append(f"{pressure_trend.capitalize()} pressure")

    rain_hourly = weather.get("rain_hourly")
    rain_daily = weather.get("rain_daily")
    if rain_hourly and rain_hourly > 0:
        extras.append(f"Rain {rain_hourly:.2f}\" per hour")
    elif rain_daily and rain_daily > 0:
        extras.append(f"Rain today {rain_daily:.2f}\"")

    solar_radiation = weather.get("solar_radiation")
    if solar_radiation and solar_radiation > 0:
        extras.append(f"Solar {solar_radiation:.0f} W/m²")

    message_parts = [", ".join(primary)]
    if extras:
        message_parts.append("; ".join(extras))

    return "🌤️ Weather Update: " + ". ".join(message_parts)
