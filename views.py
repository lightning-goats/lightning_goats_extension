"""Web UI routes for Lightning Goats extension."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from lnbits.core.models import User
from lnbits.decorators import check_user_exists
from lnbits.helpers import template_renderer


lightning_goats_router = APIRouter()


def lightning_goats_renderer():
    """Create template renderer with Lightning Goats templates path."""
    return template_renderer(["lightning_goats/templates"])


@lightning_goats_router.get("/", response_class=HTMLResponse)
async def index(request: Request, user: User = Depends(check_user_exists)):
    """Main Lightning Goats dashboard page."""
    return lightning_goats_renderer().TemplateResponse(
        "lightning_goats/index.html",
        {"request": request, "user": user.json()},
    )
