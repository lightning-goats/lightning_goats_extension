"""
Lightning Goats LNbits Extension.

IoT integration for automated goat feeding using Lightning Network payments.
Replaces the standalone main.nozaps.py middleware with native LNbits integration.

Features:
- Payment listener (replaces WebSocket monitoring)
- OpenHAB integration for feeder control
- CyberHerd integration for payment distribution
- CyberHerd Messaging integration for notifications
- Bitcoin price tracking with 24h change calculation
- Weather station integration
- Automated informational messages
"""

from fastapi import APIRouter
from loguru import logger

# Import database for LNbits migration system
from .crud import db

# Import routers
from .views import lightning_goats_router
from .views_api import lightning_goats_api_router

# Import tasks
from .tasks import (
    start_payment_listener,
    start_background_tasks,
    stop_background_tasks,
)

# Extension metadata
from .config import EXTENSION_NAME, EXTENSION_TITLE, EXTENSION_DESCRIPTION


# Create extension router
lightning_goats_ext = APIRouter(
    prefix="/lightning_goats",
    tags=["lightning_goats"],
)

# Include sub-routers
lightning_goats_ext.include_router(lightning_goats_router)
lightning_goats_ext.include_router(lightning_goats_api_router)


# Static files configuration
lightning_goats_static_files = [
    {
        "path": "/lightning_goats/static",
        "name": "lightning_goats_static",
    }
]


def lightning_goats_start():
    """
    LNbits extension start hook.
    
    Called by LNbits core during extension initialization after routes are registered.
    This is the standard lifecycle hook that LNbits uses for extensions.
    """
    try:
        logger.info("Lightning Goats: Starting extension initialization")
        
        # Start payment listener using LNbits task system
        listener = start_payment_listener()
        if listener:
            logger.info("Lightning Goats: Payment listener registered successfully")
        else:
            logger.warning("Lightning Goats: Payment listener failed to register")
        
        # Start background tasks
        start_background_tasks()
        logger.info("Lightning Goats: Background tasks started successfully")
        
        logger.info("Lightning Goats: Extension initialization completed")
        
    except Exception as e:
        logger.error(f"Lightning Goats: Initialization failed: {e}", exc_info=True)


async def lightning_goats_stop():
    """
    Stop Lightning Goats extension.
    
    Called by LNbits during shutdown to clean up resources.
    This is the standard lifecycle hook for cleanup.
    """
    try:
        logger.info("Lightning Goats: Stopping extension")
        await stop_background_tasks()
        logger.info("Lightning Goats: Extension stopped successfully")
    except Exception as e:
        logger.error(f"Lightning Goats: Error during shutdown: {e}", exc_info=True)


# Export symbols for LNbits
__all__ = [
    "lightning_goats_ext",
    "lightning_goats_static_files",
    "lightning_goats_start",
    "lightning_goats_stop",
    "db",
]
