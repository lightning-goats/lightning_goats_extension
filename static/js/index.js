window.app = Vue.createApp({
  el: '#vue',
  mixins: [windowMixin],
  data() {
    return {
      settings: {
        openhab_url: '',
        openhab_auth: '',
        openhab_feeder_rule_id: '88bd9ec4de',
        herd_wallet_id: '',
        feeder_trigger_sats: 1000,
        weather_station_url: '',
        weather_broadcast_enabled: true,
        interface_messages_enabled: true
      },
      walletOptions: [],
      status: {
        configured: false,
        balance_sats: 0,
        trigger_amount: 1000,
        progress_percent: 0,
        active_members: 0,
        btc_price_usd: null,
        btc_24h_change: null
      },
      loading: false,
      triggerFeederDialog: false,
      triggerOverrideCheck: false,
      triggerLoading: false,
      statusInterval: null
    }
  },
  computed: {
    priceChangeColor() {
      if (this.status.btc_24h_change === null || this.status.btc_24h_change === undefined) {
        return 'text-grey'
      }
      return this.status.btc_24h_change >= 0 ? 'text-positive' : 'text-negative'
    }
  },
  methods: {
    formatPrice(value) {
      return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }).format(value)
    },
    async loadWallets() {
      try {
        const response = await LNbits.api.request(
          'GET',
          '/lightning_goats/api/v1/wallets',
          this.g.user.wallets[0].adminkey
        )
        if (response.data) {
          this.walletOptions = response.data
        }
      } catch (error) {
        console.error('Failed to load wallets:', error)
      }
    },
    async loadCyberherdDefaults() {
      try {
        const response = await LNbits.api.request(
          'GET',
          '/lightning_goats/api/v1/cyberherd_defaults',
          this.g.user.wallets[0].adminkey
        )
        if (response.data) {
          // Set defaults if values are empty/not set
          if (!this.settings.herd_wallet_id && response.data.herd_wallet_id) {
            this.settings.herd_wallet_id = response.data.herd_wallet_id
          }
          if (response.data.feeder_trigger_sats) {
            this.settings.feeder_trigger_sats = response.data.feeder_trigger_sats
          }
        }
      } catch (error) {
        console.error('Failed to load CyberHerd defaults:', error)
      }
    },
    async loadSettings() {
      try {
        const response = await LNbits.api.request(
          'GET',
          '/lightning_goats/api/v1/settings',
          this.g.user.wallets[0].adminkey
        )
        if (response.data) {
          console.log('Lightning Goats: Loaded settings from server:', response.data)
          // Populate settings, including auto-populated values from CyberHerd
          this.settings = {
            openhab_url: response.data.openhab_url || '',
            openhab_auth: response.data.openhab_auth || '',
            openhab_feeder_rule_id: response.data.openhab_feeder_rule_id || '88bd9ec4de',
            herd_wallet_id: response.data.herd_wallet_id || null,
            feeder_trigger_sats: response.data.feeder_trigger_sats || 1000,
            weather_station_url: response.data.weather_station_url || '',
            // Convert to boolean properly - handles true/false/1/0, defaults to true if undefined
            weather_broadcast_enabled: response.data.weather_broadcast_enabled !== undefined
              ? Boolean(response.data.weather_broadcast_enabled)
              : true,
            interface_messages_enabled: response.data.interface_messages_enabled !== undefined
              ? Boolean(response.data.interface_messages_enabled)
              : true
          }
          console.log('Lightning Goats: Settings populated, herd_wallet_id:', this.settings.herd_wallet_id)
          console.log('Lightning Goats: Booleans - weather:', this.settings.weather_broadcast_enabled, 'interface:', this.settings.interface_messages_enabled)
        }
      } catch (error) {
        // Settings don't exist yet, that's ok - we'll use defaults
        console.log('No saved settings found, using defaults')
      }
    },
    async updateSettings() {
      this.loading = true
      console.log('Lightning Goats: updateSettings called')
      console.log('Lightning Goats: Settings to save:', JSON.stringify(this.settings, null, 2))
      console.log('Lightning Goats: herd_wallet_id value:', this.settings.herd_wallet_id)
      console.log('Lightning Goats: herd_wallet_id type:', typeof this.settings.herd_wallet_id)
      console.log('Lightning Goats: interface_messages_enabled BEFORE save:', this.settings.interface_messages_enabled)

      // Validate we have an admin key
      if (!this.g || !this.g.user || !this.g.user.wallets || !this.g.user.wallets[0] || !this.g.user.wallets[0].adminkey) {
        console.error('Lightning Goats: No admin key available!', {
          hasG: !!this.g,
          hasUser: !!(this.g && this.g.user),
          hasWallets: !!(this.g && this.g.user && this.g.user.wallets),
          walletsLength: this.g && this.g.user && this.g.user.wallets ? this.g.user.wallets.length : 0
        })
        this.$q.notify({
          type: 'negative',
          message: 'No admin key available. Please refresh the page.'
        })
        this.loading = false
        return
      }

      try {
        const response = await LNbits.api.request(
          'PUT',
          '/lightning_goats/api/v1/settings',
          this.g.user.wallets[0].adminkey,
          this.settings
        )
        console.log('Lightning Goats: Settings saved successfully', response)
        console.log('Lightning Goats: Response data:', response.data)

        // Update local settings with the server response to ensure consistency
        if (response.data) {
          this.settings = {
            openhab_url: response.data.openhab_url || '',
            openhab_auth: response.data.openhab_auth || '',
            openhab_feeder_rule_id: response.data.openhab_feeder_rule_id || '88bd9ec4de',
            herd_wallet_id: response.data.herd_wallet_id || null,
            feeder_trigger_sats: response.data.feeder_trigger_sats || 1000,
            weather_station_url: response.data.weather_station_url || '',
            weather_broadcast_enabled: response.data.weather_broadcast_enabled !== undefined
              ? Boolean(response.data.weather_broadcast_enabled)
              : true,
            interface_messages_enabled: response.data.interface_messages_enabled !== undefined
              ? Boolean(response.data.interface_messages_enabled)
              : true
          }
          console.log('Lightning Goats: Local settings updated from server response')
        }

        console.log('Lightning Goats: interface_messages_enabled AFTER save:', this.settings.interface_messages_enabled)
        this.$q.notify({
          type: 'positive',
          message: 'Settings saved successfully'
        })
        await this.loadStatus()
        console.log('Lightning Goats: interface_messages_enabled AFTER loadStatus:', this.settings.interface_messages_enabled)
      } catch (error) {
        console.error('Lightning Goats: Failed to save settings:', error)
        LNbits.utils.notifyApiError(error)
      } finally {
        this.loading = false
      }
    },
    startStatusPolling() {
      // Poll status every 5 seconds
      logger.debug('Starting status polling (5s interval)')
      this.statusInterval = setInterval(async () => {
        // Run loadStatus quietly to avoid UI flicker
        await this.loadStatus(true)
      }, 5000)
    },
    stopStatusPolling() {
      if (this.statusInterval) {
        clearInterval(this.statusInterval)
        this.statusInterval = null
        logger.debug('Stopped status polling')
      }
    },
    async refreshStatus() {
      // Manual refresh
      this.loading = true
      await this.loadStatus()
      this.loading = false
      this.$q.notify({
        type: 'positive',
        message: 'Status refreshed',
        timeout: 1000
      })
    },
    async triggerFeeder() {
      this.triggerLoading = true
      try {
        await LNbits.api.request(
          'POST',
          '/lightning_goats/api/v1/trigger_feeder',
          this.g.user.wallets[0].adminkey,
          { override_check: this.triggerOverrideCheck }
        )
        this.$q.notify({
          type: 'positive',
          message: 'Feeder triggered successfully'
        })
        this.triggerFeederDialog = false
        await this.loadStatus()
      } catch (error) {
        LNbits.utils.notifyApiError(error)
      } finally {
        this.triggerLoading = false
      }
    },
    // Updated loadStatus to support quiet mode
    async loadStatus(quiet = false) {
      try {
        const response = await LNbits.api.request(
          'GET',
          '/lightning_goats/api/v1/status',
          this.g.user.wallets[0].adminkey
        )
        if (response.data) {
          this.status = response.data
        }
      } catch (error) {
        if (!quiet) console.error('Failed to load status:', error)
      }
    }
  },
  async created() {
    await this.loadWallets()
    await this.loadCyberherdDefaults()
    await this.loadSettings()
    await this.loadStatus()
    this.startStatusPolling()

    // Initialize WebSocket listener for real-time feedback
    console.log('Lightning Goats: Initializing WebSocket listener')

    // Function to setup socket on a specific inkey
    const setupSocket = (inkey) => {
      console.log('Lightning Goats: Subscribing to topic:', inkey)
      LNbits.api.createSocket(inkey, message => {
        console.log('Lightning Goats: Received WebSocket message:', message)

        // Show notification if it's a relevant event
        if (message.type === 'feeder_triggered' || message.type === 'sats_received' || message.type === 'nostr_message') {
          const msgText = message.message || (message.values ? message.values.content : 'Message received')
          this.$q.notify({
            type: 'info',
            message: msgText,
            caption: message.type,
            icon: message.type === 'feeder_triggered' ? 'restaurant' : 'bolt',
            timeout: 10000
          })
          // Refresh status when feeder is triggered
          if (message.type === 'feeder_triggered') {
            this.loadStatus()
          }
        }
      })
    }

    // Determine the best inkey to use for the topic
    let topicInkey = null

    // 1. Try to find the inkey for the configured herd_wallet_id
    if (this.settings.herd_wallet_id && this.walletOptions.length > 0) {
      const herdWallet = this.walletOptions.find(w => w.id === this.settings.herd_wallet_id)
      if (herdWallet && herdWallet.inkey) {
        topicInkey = herdWallet.inkey
        console.log('Lightning Goats: Using Herd Wallet inkey for WebSocket')
      }
    }

    // 2. Fallback to the user's first wallet inkey if herd wallet not found or not set
    if (!topicInkey && this.g.user && this.g.user.wallets && this.g.user.wallets.length > 0) {
      topicInkey = this.g.user.wallets[0].inkey
      console.log('Lightning Goats: Falling back to default wallet inkey for WebSocket')
    }

    if (topicInkey) {
      setupSocket(topicInkey)
    } else {
      console.warn('Lightning Goats: Could not determine WebSocket topic (no inkey available)')
    }
  },
  unmounted() {
    this.stopStatusPolling()
  }
})
