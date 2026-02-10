"""Messaging integration with cyberherd_messaging."""

import random
from typing import Dict, Any, Optional
from loguru import logger

from ..config import DEFAULT_GOAT_NAMES

DEFAULT_WEBSOCKET_TOPIC = "cyberherd"
INTERFACE_INFO_FALLBACK_MESSAGE = "Lightning Goats interface update available."


async def _broadcast_websocket_message(topic: str, payload: Dict[str, Any]) -> bool:
    """Send a websocket payload using the cyberherd_messaging helper."""

    try:
        from lnbits.extensions.cyberherd_messaging.services import send_to_websocket_clients
    except ImportError as exc:
        logger.warning(f"Lightning Goats: cyberherd_messaging websocket helper unavailable: {exc}")
        return False

    try:
        await send_to_websocket_clients(topic, payload)
        return True
    except Exception as exc:
        logger.warning(f"Lightning Goats: websocket broadcast failed for topic '{topic}': {exc}")
        return False


async def _pick_template_content(category: str, user_id: Optional[str]) -> Optional[str]:
    """Return randomized template content from cyberherd_messaging when available."""

    try:
        from lnbits.extensions.cyberherd_messaging import crud as msg_crud
    except ImportError as exc:
        logger.debug(f"Lightning Goats: messaging templates unavailable ({exc})")
        return None

    try:
        if user_id:
            templates = await msg_crud.get_message_templates(user_id, category)
        else:
            templates = []
        if not templates:
            templates = await msg_crud.get_message_templates(None, category)
        if not templates:
            return None
        choice = random.choice(templates)
        content = getattr(choice, "content", None)
        return str(content) if content else None
    except Exception as exc:
        logger.debug(f"Lightning Goats: failed reading templates for category '{category}': {exc}")
        return None


async def _get_user_private_key(user_id: str) -> Optional[str]:
    """Get the user's Nostr private key hex from CyberHerd settings.
    
    Args:
        user_id: The LNbits user ID
        
    Returns:
        The private key hex string, or None if not set
    """
    try:
        from lnbits.extensions.cyberherd.crud import get_settings
        settings = await get_settings(user_id)
        if settings and hasattr(settings, 'nostr_private_key') and settings.nostr_private_key:
            return str(settings.nostr_private_key)
    except Exception as e:
        logger.debug(f"Lightning Goats: Could not get private key hex for user {user_id}: {e}")
    return None


async def _resolve_websocket_topic(user_id: Optional[str]) -> str:
    """Return the herd wallet invoice key used as websocket topic."""
    if not user_id:
        return DEFAULT_WEBSOCKET_TOPIC

    # Prefer Lightning Goats persisted settings to avoid extra lookups
    try:
        from ..crud import get_settings  # Lazy import to dodge circular refs

        settings = await get_settings(user_id)
        herd_wallet_id = getattr(settings, "herd_wallet_id", None)
        if herd_wallet_id:
            from lnbits.core.crud import get_wallet

            wallet = await get_wallet(herd_wallet_id)
            if wallet and getattr(wallet, "inkey", None):
                return wallet.inkey
    except Exception as e:
        logger.debug(f"Lightning Goats: failed resolving websocket topic from LG settings: {e}")

    # Fallback to CyberHerd settings when Lightning Goats defaults are missing
    try:
        from ..crud import get_cyberherd_settings
        from lnbits.core.crud import get_wallet

        ch_settings = await get_cyberherd_settings(user_id)
        herd_wallet_id = ch_settings.get("herd_wallet") if ch_settings else None
        if herd_wallet_id:
            wallet = await get_wallet(herd_wallet_id)
            if wallet and getattr(wallet, "inkey", None):
                return wallet.inkey
    except Exception as e:
        logger.debug(f"Lightning Goats: failed resolving websocket topic from CyberHerd settings: {e}")

    # Final fallback to user's first wallet inkey if possible
    try:
        from lnbits.core.crud import get_user
        user = await get_user(user_id)
        if user and user.wallets:
            return user.wallets[0].inkey
    except Exception:
        pass

    return DEFAULT_WEBSOCKET_TOPIC


async def send_feeder_message(
    balance_sats: int,
    payment_amount: int,
    user_id: Optional[str] = None,
):
    """
    Send feeder trigger message via CyberHerd messaging.
    
    Uses CyberHerd's publish_event_message which handles:
    - Random template selection
    - Nostr publishing
    - WebSocket broadcasting
    - Fallback message generation
    
    Args:
        balance_sats: Total balance that triggered feeder
        payment_amount: Amount of the payment that triggered it
        user_id: User ID for template selection (optional)
    """
    try:
        # Import CyberHerd messaging service
        from lnbits.extensions.cyberherd.services.messaging import publish_event_message
        
        # Get private key for Nostr publishing
        private_key = await _get_user_private_key(user_id) if user_id else None
        
        # Pick a random goat name
        goat_name = random.choice(DEFAULT_GOAT_NAMES)
        
        # Prepare values for template rendering
        values = {
            "name": goat_name,
            "new_amount": payment_amount,
            "difference": 0,
            "difference_message": f"{balance_sats} sats collected and distributed.",
        }
        
        # Use CyberHerd's publish_event_message which handles templates + fallback
        websocket_topic = await _resolve_websocket_topic(user_id)
        logger.info(f"Lightning Goats: publishing feeder_triggered message to topic {websocket_topic} for user {user_id}")
        
        success = await publish_event_message(
            event_type="feeder_triggered",
            owner_user_id=user_id,
            values=values,
            private_key=private_key,
            websocket_topic=websocket_topic,
        )
        
        if success:
            logger.info(f"Lightning Goats: Successfully published feeder trigger message")
        else:
            logger.warning(f"Lightning Goats: Failed to publish feeder trigger message")
        
        return success
        
    except ImportError as e:
        logger.warning(f"Lightning Goats: CyberHerd messaging not available: {e}")
        return False
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to send feeder message: {e}", exc_info=True)
        return False


async def send_manual_feeder_notification(user_id: Optional[str] = None) -> bool:
    """
    Send manual feeder trigger message via WebSocket broadcast.
    
    This message is restricted to websockets only to provide real-time
    feedback in the Admin UI without publishing to Nostr.
    
    Args:
        user_id: User ID for resolving websocket topic
    """
    try:
        # Resolve topic
        websocket_topic = await _resolve_websocket_topic(user_id)
        
        # Prepare "phantom" payload to trigger lightning effect without scrolling text
        # - message: " " (space) will be trimmed to empty string in client and skip display
        # - goats: ["."] ensures displayGoats logic is hit to set messageQueued=true
        payload = {
            "type": "feeder_trigger",
            "message": " ",
            "goats": ["."],
        }
        
        logger.info(f"Lightning Goats: broadcasting minimal manual feeder trigger to topic {websocket_topic}")
        
        # Use internal websocket broadcast helper
        return await _broadcast_websocket_message(websocket_topic, payload)
        
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to send manual feeder websocket message: {e}")
        return False


async def send_payment_received_message(
    amount: int,
    balance: int,
    trigger_threshold: int,
    user_id: Optional[str] = None,
):
    """
    Send payment received message via CyberHerd messaging.
    
    Uses CyberHerd's publish_event_message which:
    - Selects random template from "sats_received" category
    - Renders template with provided values
    - Publishes to Nostr
    - Broadcasts to WebSocket
    - Falls back to generated message if no template exists
    
    Args:
        amount: Amount received in sats
        balance: Current balance in sats
        trigger_threshold: Sats needed to trigger feeder
        user_id: User ID for template selection (optional)
    """
    try:
        # Import CyberHerd messaging service
        from lnbits.extensions.cyberherd.services.messaging import publish_event_message
        
        # Get private key for Nostr publishing
        private_key = await _get_user_private_key(user_id) if user_id else None
        
        # Pick a random goat name for the message
        goat_name = random.choice(DEFAULT_GOAT_NAMES)
        
        # Calculate how many more sats needed
        difference = max(0, trigger_threshold - balance)
        
        # Create difference message
        if difference > 0:
            difference_message = f"{difference} sats until feeder activation."
        else:
            difference_message = "Feeder ready!"
        
        # Prepare values for template rendering
        values = {
            "name": goat_name,
            "new_amount": amount,
            "difference": difference,
            "difference_message": difference_message,
        }
        
        # Use CyberHerd's publish_event_message for unified handling
        websocket_topic = await _resolve_websocket_topic(user_id)
        logger.info(f"Lightning Goats: publishing sats_received message to topic {websocket_topic} for user {user_id}")
        
        success = await publish_event_message(
            event_type="sats_received",
            owner_user_id=user_id,
            values=values,
            private_key=private_key,
            websocket_topic=websocket_topic,
        )
        
        if success:
            logger.info(f"Lightning Goats: Successfully published payment received message")
        else:
            logger.warning(f"Lightning Goats: Failed to publish payment received message")
        
        return success
        
    except ImportError as e:
        logger.warning(f"Lightning Goats: CyberHerd messaging not available: {e}")
        return False
    except Exception as e:
        logger.error(f"Lightning Goats: Failed to send payment received message: {e}", exc_info=True)
        return False


async def send_interface_info_message(user_id: Optional[str] = None) -> bool:
    """Broadcast interface info updates to websocket clients only."""

    websocket_topic = await _resolve_websocket_topic(user_id)
    message = await _pick_template_content("interface_info", user_id)
    if not message:
        message = INTERFACE_INFO_FALLBACK_MESSAGE

    payload = {"type": "interface_info", "message": message}
    sent = await _broadcast_websocket_message(websocket_topic, payload)
    if sent:
        logger.debug(
            f"Lightning Goats: interface info message broadcast to websocket topic {websocket_topic}"
        )
    else:
        logger.warning(
            f"Lightning Goats: failed to broadcast interface info message to topic {websocket_topic}"
        )
    return sent


async def send_weather_message(weather_data: Dict[str, Any], user_id: Optional[str] = None) -> bool:
    """
    Send weather update message.
    
    Args:
        weather_data: Dictionary containing weather information
    """
    from .weather import format_weather_message

    message = format_weather_message(weather_data)
    websocket_topic = await _resolve_websocket_topic(user_id)
    payload = {
        "type": "weather_status",
        "message": message,
        "data": weather_data,
    }
    sent = await _broadcast_websocket_message(websocket_topic, payload)
    if sent:
        logger.debug(
            f"Lightning Goats: weather message sent via websocket fallback (topic={websocket_topic})"
        )
    return sent
