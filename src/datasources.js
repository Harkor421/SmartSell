// Real-time data sources: pump.fun MC polling + PumpPortal WS for trade events.

import WebSocket from 'ws';

const PUMP_FRONTEND = 'https://frontend-api-v3.pump.fun/coins-v2';

// Fetch the token's current MC (USD + SOL) and migration status from pump.fun's frontend API.
// No auth, no rate-limit hassles. Returns null on failure.
export async function fetchPumpMC(mint) {
  try {
    const res = await fetch(`${PUMP_FRONTEND}/${mint}`);
    if (!res.ok) return null;
    const data = await res.json();
    return {
      mint: data.mint,
      name: data.name,
      symbol: data.symbol,
      marketCapSol: typeof data.market_cap === 'number' ? data.market_cap : 0,
      marketCapUsd: typeof data.usd_market_cap === 'number' ? data.usd_market_cap : 0,
      complete: !!data.complete,
      pumpSwapPool: data.pump_swap_pool || null,
    };
  } catch {
    return null;
  }
}

// PumpPortal WS — keeps one connection open and multiplexes subscribeTokenTrade
// across multiple token mints. Emits "trade" events for each incoming buy/sell.
export class PumpPortalClient {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.ws = null;
    this.subscribed = new Set();
    this.listeners = new Set();
    this.reconnectTimer = null;
    this.connected = false;
  }

  on(handler) { this.listeners.add(handler); }
  off(handler) { this.listeners.delete(handler); }

  emit(event) {
    for (const fn of this.listeners) {
      try { fn(event); } catch {}
    }
  }

  connect() {
    if (this.ws && this.ws.readyState <= 1) return; // connecting or open
    const url = `wss://pumpportal.fun/api/data?api-key=${this.apiKey}`;
    this.ws = new WebSocket(url);

    this.ws.on('open', () => {
      this.connected = true;
      this.emit({ type: 'connected' });
      // Re-subscribe to all known mints on (re)connect
      if (this.subscribed.size > 0) {
        this.ws.send(JSON.stringify({
          method: 'subscribeTokenTrade',
          keys: Array.from(this.subscribed),
        }));
      }
    });

    this.ws.on('message', (data) => {
      const text = data.toString();
      let msg;
      try { msg = JSON.parse(text); } catch {
        // Non-JSON message — surface raw text for debugging
        this.emit({ type: 'raw_text', text: text.slice(0, 500) });
        return;
      }
      // ALWAYS emit a raw event so callers can see traffic regardless of shape
      this.emit({ type: 'raw_msg', payload: msg });
      // PumpPortal trade events look like:
      // { txType: "buy" | "sell", mint, traderPublicKey, solAmount, tokenAmount, pool, ... }
      // Subscription confirmations: { message: "Successfully subscribed to ..." }
      if (msg && (msg.txType === 'buy' || msg.txType === 'sell') && msg.mint) {
        this.emit({ type: 'trade', mint: msg.mint, raw: msg });
      } else if (msg?.message) {
        this.emit({ type: 'info', message: msg.message });
      } else if (msg?.errors || msg?.error) {
        this.emit({ type: 'error', error: JSON.stringify(msg.errors || msg.error) });
      } else {
        // Anything we don't recognize — surface it for debugging
        this.emit({ type: 'unknown', raw: msg });
      }
    });

    this.ws.on('error', (err) => {
      this.emit({ type: 'error', error: err?.message || String(err) });
    });

    this.ws.on('close', () => {
      this.connected = false;
      this.emit({ type: 'disconnected' });
      // Reconnect after 5s
      if (!this.reconnectTimer) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          if (this.subscribed.size > 0) this.connect();
        }, 5000);
      }
    });
  }

  subscribeMint(mint) {
    if (this.subscribed.has(mint)) return;
    this.subscribed.add(mint);
    this.connect();
    if (this.ws?.readyState === 1) {
      this.ws.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [mint] }));
    }
  }

  unsubscribeMint(mint) {
    if (!this.subscribed.has(mint)) return;
    this.subscribed.delete(mint);
    if (this.ws?.readyState === 1) {
      this.ws.send(JSON.stringify({ method: 'unsubscribeTokenTrade', keys: [mint] }));
    }
    if (this.subscribed.size === 0) {
      this.close();
    }
  }

  close() {
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.ws) {
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
    this.connected = false;
  }
}
