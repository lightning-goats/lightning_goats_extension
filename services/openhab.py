"""OpenHAB integration service."""

import httpx
from typing import Optional
from loguru import logger

from ..config import DEFAULT_OPENHAB_FEEDER_RULE_ID, DEFAULT_OPENHAB_FEEDER_OVERRIDE_ITEM
from .url_validation import ensure_outbound_url_allowed


class OpenHABService:
    """Service for interacting with OpenHAB REST API."""
    
    def __init__(self, base_url: str, auth_token: str):
        """
        Initialize OpenHAB service.
        
        Args:
            base_url: OpenHAB base URL (e.g., http://192.168.1.100:8080)
            auth_token: OpenHAB authentication token
        """
        self.base_url = ensure_outbound_url_allowed(base_url, "OpenHAB URL").rstrip("/")
        self.auth = (auth_token, "")
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=5.0)
        return self._client
    
    @staticmethod
    def _sanitize_path_segment(segment: str) -> str:
        """Sanitize a path segment to prevent path traversal."""
        # Strip path separators and parent references
        sanitized = segment.replace("/", "").replace("\\", "").replace("..", "")
        if not sanitized:
            raise ValueError(f"Invalid path segment: {segment!r}")
        return sanitized

    async def get_item_state(self, item_name: str) -> Optional[str]:
        """
        Get state of an OpenHAB item.

        Args:
            item_name: Name of the OpenHAB item

        Returns:
            Item state as string, or None if error
        """
        try:
            safe_name = self._sanitize_path_segment(item_name)
            client = await self._get_client()
            response = await client.get(
                f"{self.base_url}/rest/items/{safe_name}/state",
                auth=self.auth
            )
            response.raise_for_status()
            return response.text.strip()
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching OpenHAB item '{item_name}': {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching OpenHAB item '{item_name}': {e}")
            return None
    
    async def trigger_feeder(self, rule_id: str = DEFAULT_OPENHAB_FEEDER_RULE_ID) -> bool:
        """
        Trigger the feeder rule.
        
        Args:
            rule_id: OpenHAB rule ID to trigger
            
        Returns:
            True if successful, False otherwise
        """
        try:
            safe_rule_id = self._sanitize_path_segment(rule_id)
            client = await self._get_client()
            response = await client.post(
                f"{self.base_url}/rest/rules/{safe_rule_id}/runnow",
                auth=self.auth
            )
            response.raise_for_status()
            logger.info(f"Feeder triggered successfully (rule: {rule_id})")
            return response.status_code == 200
        except httpx.HTTPError as e:
            logger.error(f"HTTP error triggering feeder: {e}")
            return False
        except Exception as e:
            logger.error(f"Error triggering feeder: {e}")
            return False
    
    async def is_feeder_override_enabled(
        self, 
        item_name: str = DEFAULT_OPENHAB_FEEDER_OVERRIDE_ITEM
    ) -> bool:
        """
        Check if feeder override is enabled.
        
        Args:
            item_name: Name of the override item
            
        Returns:
            True if override is enabled, False otherwise
        """
        try:
            state = await self.get_feeder_override_state(item_name)
            if state is None:
                logger.warning("Feeder override state is unknown; failing closed")
                return True
            is_enabled = state
            if is_enabled:
                logger.info("Feeder override is enabled")
            return is_enabled
        except Exception as e:
            logger.error(f"Error checking feeder override: {e}")
            return True

    async def get_feeder_override_state(
        self,
        item_name: str = DEFAULT_OPENHAB_FEEDER_OVERRIDE_ITEM,
    ) -> Optional[bool]:
        """Return override state, or None when it cannot be determined."""

        state = await self.get_item_state(item_name)
        if state is None:
            return None
        return state == "ON"
    
    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
