"""Bitcoin price service using OpenHAB."""

import asyncio
import time
from typing import Optional
from loguru import logger

from .openhab import OpenHABService
from ..models import BitcoinPriceData


async def get_bitcoin_price_data(openhab: OpenHABService) -> BitcoinPriceData:
    """
    Get current Bitcoin price and 24h change from OpenHAB.
    
    Args:
        openhab: OpenHAB service instance
    
    Returns:
        BitcoinPriceData with current price and 24h change percentage
    """
    try:
        # Get Bitcoin data from OpenHAB items
        price_state, change_state = await asyncio.gather(
            openhab.get_item_state("BTC_USD_Price"),
            openhab.get_item_state("BTC_Price_24h_PercentChange"),
        )
        
        current_time = int(time.time())
        
        # Parse price
        price = None
        if price_state and price_state not in {"NULL", "UNDEF"}:
            try:
                price = float(price_state)
            except ValueError:
                logger.error(f"Invalid price value from OpenHAB: {price_state}")
        
        # Parse 24h change
        percent_change = None
        if change_state and change_state not in {"NULL", "UNDEF"}:
            try:
                percent_change = float(change_state)
            except ValueError:
                logger.error(f"Invalid 24h change value from OpenHAB: {change_state}")
        
        if price is None:
            raise ValueError("Failed to get valid BTC price from OpenHAB")
        
        if percent_change is not None:
            logger.debug(
                f"BTC price: ${price:.2f}, "
                f"24h change: {percent_change:+.2f}%"
            )
        else:
            logger.debug(f"BTC price: ${price:.2f}, no 24h change available")
        
        return BitcoinPriceData(
            btc_usd_price=price,
            btc_price_24h_percent_change=percent_change,
            last_updated=current_time
        )
    except Exception as e:
        logger.error(f"Failed to get Bitcoin price data from OpenHAB: {e}")
        raise
