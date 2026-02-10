# Lightning Goats Extension

IoT integration for automated goat feeding using Lightning Network payments.

## Overview

Lightning Goats replaces the standalone `main.nozaps.py` middleware with a native LNbits extension that provides:

- **Automated Feeder Control**: Triggers IoT feeder when payment threshold is reached
- **Payment Distribution**: Integrates with CyberHerd extension to distribute funds to active members
- **Real-time Notifications**: Uses CyberHerd Messaging for WebSocket and Nostr notifications
- **Bitcoin Price Tracking**: Real-time price monitoring from OpenHAB with 24-hour change percentage
- **Weather Integration**: Optional weather station data broadcasting
- **Payment Listener**: Native LNbits payment monitoring (replaces WebSocket complexity)

## Features

### Payment Processing
- Native LNbits payment listener for reliable payment monitoring
- No WebSocket connection management needed
- Automatic feeder triggering at configurable threshold
- CyberHerd member payment distribution

### OpenHAB Integration
- REST API integration for IoT control
- Feeder trigger automation
- Override check for manual control
- Configurable rule IDs

### Messaging
- Feeder trigger notifications
- Payment received messages
- Weather status updates
- Interface information broadcasts
- Integration with CyberHerd Messaging extension

### Bitcoin Price
- Fetches price data from OpenHAB items:
  - `BTC_USD_Price` - Current Bitcoin price in USD
  - `BTC_Price_24h_PercentChange` - 24-hour percentage change
- Real-time price display on dashboard
- No additional storage or calculation needed

## Requirements

### Required Extensions
- **CyberHerd**: Member management and payment distribution
- **CyberHerd Messaging**: Nostr publishing and WebSocket broadcasts

### External Services

- **OpenHAB**: For IoT feeder control and Bitcoin price data (required)
  - OpenHAB items needed:
    - Feeder control rule (default ID: `88bd9ec4de`)
    - `FeederOverride` item for manual override
    - `BTC_USD_Price` item for current Bitcoin price
    - `BTC_Price_24h_PercentChange` item for 24-hour change
- **Weather Station**: For weather data (optional)

## Installation

1. The extension should be automatically available in your LNbits installation
2. Enable it in the Extension Manager
3. Navigate to `/lightning_goats` to configure

## Configuration

### Initial Setup

1. **OpenHAB Settings**
   - Enter your OpenHAB URL (e.g., `http://192.168.1.100:8080`)
   - Enter your OpenHAB authentication token
   - Configure OpenHAB Feeder Rule ID (default: `88bd9ec4de`)

2. **Feeder Trigger**
   - Set the satoshi threshold for automatic feeder activation
   - If not configured, defaults to CyberHerd's `feeder_trigger_sats` setting
   - Fallback default: 1000 sats if CyberHerd setting unavailable

3. **Weather Station** (Optional)
   - Enter weather station URL if available
   - Toggle weather broadcasts on/off

4. **Messaging**
   - Toggle interface info messages
   - Configure via CyberHerd Messaging templates

### Advanced Configuration

Settings can be found in `config.py`:

```python
DEFAULT_FEEDER_TRIGGER_SATS = 1000  # Fallback if CyberHerd not available
DEFAULT_GOAT_NAMES = ["Dexter", "Rowan", "Nova", "Cosmo", "Newton"]
DEFAULT_OPENHAB_FEEDER_RULE_ID = "88bd9ec4de"
DEFAULT_WEATHER_BROADCAST_INTERVAL = 60  # seconds
DEFAULT_WEATHER_BROADCAST_PROBABILITY = 0.3  # 30% chance per interval
```

## Usage

### Dashboard

The main dashboard (`/lightning_goats`) shows:
- Current balance and progress toward feeder trigger
- Active CyberHerd member count
- Current Bitcoin price with 24-hour change
- Manual feeder trigger button

### API Endpoints

All endpoints require authentication with wallet admin key.

#### Settings
- `GET /api/v1/settings` - Get current settings
- `PUT /api/v1/settings` - Update settings
- `DELETE /api/v1/settings` - Delete settings

#### Status
- `GET /api/v1/status` - Get complete status (dashboard data)
- `GET /api/v1/balance` - Get wallet balance
- `GET /api/v1/trigger_amount` - Get feeder trigger threshold
- `GET /api/v1/cyberherd/active_count` - Get active member count
- `GET /api/v1/bitcoin_data` - Get Bitcoin price and 24h change

#### Actions
- `POST /api/v1/trigger_feeder` - Manually trigger feeder

### Payment Flow

1. **Payment Received**
   - LNbits payment listener detects incoming payment
   - Lightning Goats processes payment amount

2. **Threshold Check**
   - Compares current balance to trigger threshold
   - Checks if OpenHAB feeder override is enabled

3. **Feeder Trigger**
   - Activates OpenHAB feeder rule
   - Sends notification via CyberHerd Messaging

4. **Payment Distribution**
   - Calls CyberHerd extension to distribute funds
   - Splits payment among active members

5. **Notifications**
   - WebSocket broadcast to connected clients
   - Nostr note published (if configured)
   - Balance reset after distribution

## Architecture

### Key Improvements Over main.nozaps.py

1. **No WebSocket Management** (~150 lines removed)
   - Uses LNbits native payment listener
   - Guaranteed delivery, no reconnection logic
   - Simplified error handling

2. **Internal Function Calls** (not HTTP)
   - Direct imports from CyberHerd extension
   - Direct imports from CyberHerd Messaging extension
   - No HTTP overhead, better performance

3. **Bitcoin Price from OpenHAB**
   - Fetches price and 24h change directly from OpenHAB items
   - No calculation or history storage needed
   - Real-time accuracy from your OpenHAB setup

4. **Settings in UI** (not environment variables)
   - User-friendly configuration form
   - Per-user settings support
   - No server restart required

### File Structure

```
lightning_goats/
├── __init__.py              # Extension setup and initialization
├── config.py                # Configuration constants
├── models.py                # Pydantic data models
├── crud.py                  # Database operations
├── migrations.py            # Database migrations
├── views.py                 # Web UI routes
├── views_api.py             # REST API routes
├── tasks.py                 # Payment listener and background tasks
├── services/
│   ├── __init__.py
│   ├── openhab.py          # OpenHAB REST API integration
│   ├── bitcoin_price.py    # Bitcoin price from OpenHAB
│   ├── messaging.py        # CyberHerd Messaging integration
│   └── weather.py          # Weather station integration
├── static/
│   └── js/
│       └── index.js        # Frontend JavaScript
└── templates/
    └── lightning_goats/
        └── index.html       # Dashboard UI
```

## Development

### Testing Payment Listener

```bash
# Send a test payment to your wallet
# Check LNbits logs for payment processing:
tail -f logs/lnbits.log | grep "Lightning Goats"
```

### Testing OpenHAB Integration

```bash
# Verify OpenHAB connectivity
curl -u "YOUR_TOKEN:" http://YOUR_OPENHAB_URL/rest/items/FeederOverride/state
```

### Debugging

Enable debug logging in `config.py` or via environment:

```bash
export LOG_LEVEL=DEBUG
```

## Migration from main.nozaps.py

### Comparison

| Feature | main.nozaps.py | lightning_goats |
|---------|---------------|-----------------|
| Payment Monitoring | Custom WebSocket | Native listener |
| Configuration | Environment vars | UI settings |
| CyberHerd Integration | HTTP calls | Internal imports |
| Bitcoin Price | Custom calculation | OpenHAB items |
| Code Complexity | ~800 lines | ~600 lines |
| Setup | Manual service | Extension install |

### Migration Steps

1. Deploy Lightning Goats extension
2. Configure settings in UI (copy from .env)
3. Test functionality in parallel
4. Stop main.nozaps.py service
5. Verify all features working
6. Archive old service files

## Troubleshooting

### Payment listener not working

- Check extension is enabled in LNbits
- Verify wallet ID in settings
- Check logs: `grep "ext_lightning_goats" logs/lnbits.log`

### Feeder not triggering

- Verify OpenHAB URL and auth token
- Check feeder override is not enabled
- Test manual trigger from dashboard
- Check OpenHAB rule ID matches config

### Messages not sending

- Ensure CyberHerd Messaging extension is installed
- Verify message templates exist
- Check logs for messaging errors

### Bitcoin price not updating

- Verify OpenHAB items exist: `BTC_USD_Price` and `BTC_Price_24h_PercentChange`
- Check OpenHAB connectivity from LNbits
- Ensure OpenHAB authentication is correct
- Verify items contain valid numeric data (not NULL/UNDEF)

## Support

For issues or questions:

1. Check LNbits logs
2. Review extension configuration
3. Verify required extensions are installed
4. Test external service connectivity

## License

Same as LNbits core

## Credits

Built for the Lightning Goats project by integrating:

- LNbits core payment system
- CyberHerd member management
- CyberHerd Messaging notification system
- OpenHAB IoT automation platform
