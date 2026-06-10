// Smart Sell backend — single WS endpoint that runs MC-armed buy-reactive sells.
// Sessions are per-WS-connection. Each session holds: token, wallets, MC threshold,
// reactor params, and a running monitor loop.
//
// Wire protocol (client → server):
//   { type: "setup", tokenMint, walletKeys[], mcThresholdUsd, minBuySol, sellPctOfBuy,
//     slippageBps, maxTriggers, cooldownMs }
//   { type: "start" }
//   { type: "stop" }
//
// Server → client events:
//   { type: "log", message, level }
//   { type: "state", token: {name,symbol,marketCapUsd,marketCapSol,complete}, armed, wallets:[{pub,balance,supplyPct}] }
//   { type: "armed" }   — when MC threshold first crossed
//   { type: "trade", trade: {trader, solAmount, tokenAmount, txType} }   — incoming trade
//   { type: "sell_fired", wallet, tokenAmount, expectedSolOut }
//   { type: "sell_result", wallet, success, signature, error, actualSolOut }
//   { type: "stopped" }

import { WebSocketServer } from 'ws';
import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import bs58 from 'bs58';
import dotenv from 'dotenv';

import { fetchPumpMC, PumpPortalClient } from './datasources.js';
import {
  detectTokenProgram, getSellerTokenBalance,
  buildSellTxCached, submitSellTx, SellContext,
} from './sell.js';

dotenv.config();

const PORT = parseInt(process.env.PORT) || 4099;
const RPC_URL = process.env.RPC_URL;
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY;

if (!RPC_URL) { console.error('Missing RPC_URL in env'); process.exit(1); }

const connection = new Connection(RPC_URL, 'confirmed');

// Single shared PumpPortal client across all WS sessions. Avoids opening multiple
// connections (PumpPortal will ban IPs that open many sockets).
const portal = PUMPPORTAL_API_KEY ? new PumpPortalClient(PUMPPORTAL_API_KEY) : null;

const wss = new WebSocketServer({
  host: '127.0.0.1',
  port: PORT,
  verifyClient: (info, cb) => {
    const remote = info.req.socket.remoteAddress || '';
    const isLoopback = remote === '127.0.0.1' || remote === '::1' || remote === '::ffff:127.0.0.1';
    if (!isLoopback) return cb(false, 403, 'Forbidden');
    const allowed = new Set(['http://localhost:5180', 'http://127.0.0.1:5180', '', 'null']);
    const origin = info.origin || '';
    if (!allowed.has(origin)) return cb(false, 403, 'Forbidden');
    cb(true);
  },
});
console.log(`SmartSell WS listening on ws://127.0.0.1:${PORT} (loopback-only, origin-checked)`);
if (!portal) console.warn('[WARN] No PUMPPORTAL_API_KEY in .env — buy-reactive task will be disabled');

wss.on('connection', (ws) => {
  console.log('Client connected');

  /** @type {Session | null} */
  let session = null;
  const send = (type, data = {}) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type, ...data })); };
  const log = (message, level = 'info') => { send('log', { message, level }); console.log(`[${level}] ${message}`); };

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { send('error', { message: 'Invalid JSON' }); return; }

    try {
      if (msg.type === 'setup') { await handleSetup(msg); return; }
      if (msg.type === 'start') { await handleStart(); return; }
      if (msg.type === 'stop') { handleStop(); return; }
      send('error', { message: `Unknown message type: ${msg.type}` });
    } catch (err) {
      log(`Handler error: ${err.message}`, 'error');
      console.error(err);
    }
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    handleStop();
  });

  async function handleSetup(msg) {
    const {
      tokenMint, walletKeys, mcThresholdUsd, minBuySol, sellPctOfBuy,
      slippageBps = 1500, maxTriggers = 0, cooldownMs = 3000,
      priorityFeeSol = 0.005,
    } = msg;

    if (!tokenMint) return send('error', { message: 'Missing tokenMint' });
    if (!Array.isArray(walletKeys) || walletKeys.length === 0) return send('error', { message: 'walletKeys[] required' });

    let mintPub;
    try { mintPub = new PublicKey(tokenMint); } catch { return send('error', { message: 'Invalid tokenMint' }); }

    const wallets = [];
    for (let i = 0; i < walletKeys.length; i++) {
      const k = walletKeys[i];
      let kp;
      try { kp = Keypair.fromSecretKey(bs58.decode(k)); }
      catch { return send('error', { message: `Invalid wallet #${i + 1} private key` }); }
      wallets.push({ kp, pub: kp.publicKey.toBase58(), sells: 0, soldSol: 0 });
    }
    const priorityFeeLamports = Math.max(0, Math.floor((Number(priorityFeeSol) || 0) * 1e9));

    handleStop(); // clean any prior session

    session = {
      mint: mintPub,
      wallets,
      mcThresholdUsd: Number(mcThresholdUsd) || 0,
      minBuySol: Number(minBuySol) || 0,
      sellPctOfBuy: Number(sellPctOfBuy) || 0,  // e.g. 30 for 30%
      slippageBps: Number(slippageBps) || 1500,
      maxTriggers: Number(maxTriggers) || 0,    // 0 = unlimited
      cooldownMs: Number(cooldownMs) || 3000,
      priorityFeeLamports,
      // Runtime
      armed: false,
      triggerCount: 0,
      lastSellAt: 0,
      stopRequested: false,
      mcInterval: null,
      tradeListener: null,
      ctx: null, // SellContext — populated lazily on first sell to avoid extra startup RPC
      lastWalletStateAt: 0,
      lastMcUsd: 0,
    };

    log(`Setup OK: mint=${tokenMint.slice(0, 6)}…, ${wallets.length} wallets, MC threshold=$${session.mcThresholdUsd}, react: ≥${session.minBuySol} SOL → sell ${session.sellPctOfBuy}%`);
    await pushState({ skipBalances: true });
  }

  // pushState by default rate-limits wallet balance fetches to once per 30s to avoid
  // hammering Helius. Pass { force: true } after a sell to force a fresh balance read.
  async function pushState(opts = {}) {
    if (!session) return;
    const mc = await fetchPumpMC(session.mint.toBase58());
    const skipBalances = opts.skipBalances && !opts.force;
    const tooSoon = Date.now() - session.lastWalletStateAt < 30_000;
    let walletState = [];
    if (!skipBalances && (!tooSoon || opts.force)) {
      // Lazily ensure ctx exists so we get the cached tokenProgramId without a duplicate fetch
      if (!session.ctx) session.ctx = await new SellContext(connection, session.mint).init();
      for (const w of session.wallets) {
        let bal = 0n;
        try {
          bal = await getSellerTokenBalance(connection, session.mint, w.kp.publicKey, session.ctx.tokenProgramId);
        } catch {}
        const TOTAL = 1_000_000_000_000_000n; // 1B * 1e6
        const supplyPct = Number(bal * 10000n / TOTAL) / 100;
        walletState.push({
          pub: w.pub, balance: bal.toString(), supplyPct,
          sells: w.sells || 0, soldSol: w.soldSol || 0,
        });
      }
      session.lastWalletStateAt = Date.now();
    } else {
      // Even when skipping balance fetches, surface the latest sells / soldSol counts
      for (const w of session.wallets) {
        walletState.push({
          pub: w.pub, balance: null, supplyPct: null,
          sells: w.sells || 0, soldSol: w.soldSol || 0,
        });
      }
    }
    send('state', {
      token: mc ? {
        name: mc.name, symbol: mc.symbol,
        marketCapSol: mc.marketCapSol, marketCapUsd: mc.marketCapUsd,
        complete: mc.complete,
      } : null,
      armed: session.armed,
      triggerCount: session.triggerCount,
      wallets: walletState,
    });
  }

  async function handleStart() {
    if (!session) return send('error', { message: 'Run setup first' });
    if (session.mcInterval) return log('Already started', 'warn');

    log('Starting monitor…');
    session.stopRequested = false;

    // 1) MC poll loop — every 3 seconds (was 2s; pump.fun frontend has occasional 429s)
    session.mcInterval = setInterval(async () => {
      if (!session) return;
      const mc = await fetchPumpMC(session.mint.toBase58());
      if (!mc) return;
      session.lastMcUsd = mc.marketCapUsd;
      session.lastMcSol = mc.marketCapSol;
      send('state_mc', {
        marketCapSol: mc.marketCapSol,
        marketCapUsd: mc.marketCapUsd,
        complete: mc.complete,
      });
      if (!session.armed && mc.marketCapUsd >= session.mcThresholdUsd && session.mcThresholdUsd > 0) {
        session.armed = true;
        log(`Armed at $${Math.round(mc.marketCapUsd / 1000)}K MC (threshold $${Math.round(session.mcThresholdUsd / 1000)}K)`, 'success');
        send('armed', { mcUsd: mc.marketCapUsd });
        // Subscribe to PumpPortal trades for this mint AFTER arming
        if (portal && session.minBuySol > 0 && session.sellPctOfBuy > 0) {
          ensureTradeListener();
        }
      }
    }, 3000);

    await pushState({ skipBalances: true });
  }

  let msgCount = 0;
  let lastHeartbeat = Date.now();
  let heartbeatInterval = null;

  function ensureTradeListener() {
    if (!session || session.tradeListener || !portal) return;
    portal.subscribeMint(session.mint.toBase58());
    msgCount = 0;
    lastHeartbeat = Date.now();
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    heartbeatInterval = setInterval(() => {
      if (!session) return;
      log(`PumpPortal heartbeat: ${msgCount} msgs in last 10s`, 'info');
      msgCount = 0;
    }, 10_000);

    const handler = (event) => {
      if (!session) return;
      // Count every event PumpPortal sends so we can tell whether the WS is silent
      if (event.type === 'raw_msg' || event.type === 'raw_text') msgCount++;

      if (event.type === 'connected') log('PumpPortal connected', 'info');
      if (event.type === 'disconnected') log('PumpPortal disconnected', 'warn');
      if (event.type === 'error') log(`PumpPortal error: ${event.error}`, 'warn');
      if (event.type === 'info') log(`PumpPortal: ${event.message}`, 'info');
      if (event.type !== 'trade') return;
      if (event.mint !== session.mint.toBase58()) return;
      const t = event.raw;

      // Be liberal in field-name detection (camelCase vs snake_case, "user" vs "trader")
      const txType = t.txType || t.tx_type || t.type;
      const trader =
        t.traderPublicKey || t.trader_public_key ||
        t.user || t.userAccount || t.user_account || t.signer || '';
      const solAmount = Number(
        t.solAmount ?? t.sol_amount ?? t.solIn ?? t.sol_in ?? t.amountSol ?? t.amount_sol ?? 0
      );
      const tokenAmount =
        t.tokenAmount ?? t.token_amount ?? t.tokensOut ?? t.tokens_out ?? null;

      // ALWAYS surface every buy/sell to the frontend so you can see what's happening,
      // regardless of whether it qualifies for a trigger.
      send('trade_seen', {
        txType, trader, solAmount, tokenAmount, mint: event.mint,
        // Include the raw payload too — handy for spotting field-name issues
        raw: t,
      });

      if (txType !== 'buy') return;
      const isOurWallet = session.wallets.some((w) => w.pub === trader);
      if (isOurWallet) return;
      if (!Number.isFinite(solAmount) || solAmount <= 0) {
        log(`Trade with no parseable solAmount — keys: ${Object.keys(t).join(',')}`, 'warn');
        return;
      }
      if (solAmount < session.minBuySol) {
        // below threshold — already surfaced via trade_seen
        return;
      }
      send('trade', { trade: { trader, solAmount, tokenAmount, txType } });
      maybeReactSell(solAmount);
    };
    session.tradeListener = handler;
    portal.on(handler);
  }

  // Decide whether to fire sells. Each wallet sells `sellPctOfBuy`% of the buyer's SOL value
  // (converted into a token amount based on the wallet's current balance and SOL/token price).
  // Triggers are gated only by cooldownMs — never by an in-flight sell, so a slow RPC on one
  // sell can't make us miss the next qualifying buy.
  function maybeReactSell(buyerSolAmount) {
    if (!session) return;
    if (session.maxTriggers > 0 && session.triggerCount >= session.maxTriggers) {
      log(`Max triggers (${session.maxTriggers}) reached — ignoring further trades`, 'info');
      return;
    }
    const now = Date.now();
    if (now - session.lastSellAt < session.cooldownMs) return;
    session.lastSellAt = now;
    session.triggerCount++;

    const targetSolPerWallet = buyerSolAmount * (session.sellPctOfBuy / 100);
    log(`Trigger #${session.triggerCount}: buyer put in ${buyerSolAmount.toFixed(4)} SOL → each wallet sells ~${targetSolPerWallet.toFixed(4)} SOL worth`);

    // Fire-and-forget: don't await sell completion, otherwise we'd block the next trigger.
    // Per-wallet concurrency is handled inside sellForWallet via walletEntry.inflight.
    Promise.allSettled(session.wallets.map((w) => sellForWallet(w, targetSolPerWallet)))
      .finally(() => { pushState({ force: true }).catch(() => {}); });
  }

  // Convert target SOL into token amount using cached MC, then sell. Uses SellContext
  // so we don't refetch tokenProgramId / migration / global / blockhash on every trigger.
  async function sellForWallet(walletEntry, targetSolAmount) {
    if (!session) return;
    const { kp, pub } = walletEntry;
    // Per-wallet inflight guard — prevents two sells on the same wallet from racing on
    // the cached blockhash. Other wallets are free to fire in parallel.
    if (walletEntry.inflight) {
      log(`[${pub.slice(0, 4)}…] Skip: previous sell still in flight`, 'warn');
      return;
    }
    walletEntry.inflight = true;
    try {
      if (!session.ctx) session.ctx = await new SellContext(connection, session.mint).init();
      const ctx = session.ctx;

      const tokenBal = await getSellerTokenBalance(connection, session.mint, kp.publicKey, ctx.tokenProgramId);
      if (tokenBal === 0n) {
        log(`[${pub.slice(0, 4)}…] Skip: no balance`, 'warn');
        return;
      }

      // Use cached MC (no extra HTTP per wallet) — last MC poll fills session.lastMcUsd / lastMcSol
      const mcSol = session.lastMcSol || 0;
      const pricePerToken = mcSol > 0 ? mcSol / 1_000_000_000 : 0;
      let tokenAmountToSell = 0n;
      if (pricePerToken > 0) {
        const tokensFloat = targetSolAmount / pricePerToken;
        tokenAmountToSell = BigInt(Math.floor(tokensFloat * 1_000_000));
      }
      if (tokenAmountToSell <= 0n || tokenAmountToSell > tokenBal) tokenAmountToSell = tokenBal;

      const built = await buildSellTxCached(ctx, kp, tokenAmountToSell, session.slippageBps, session.priorityFeeLamports);
      const expectedSolFloat = Number(built.expectedSolOut) / 1e9;
      send('sell_fired', { wallet: pub, tokenAmount: tokenAmountToSell.toString(), expectedSolOut: built.expectedSolOut.toString(), migrated: built.migrated });
      log(`[${pub.slice(0, 4)}…] Selling ${tokenAmountToSell} tokens (~${expectedSolFloat.toFixed(4)} SOL)…`);

      const r = await submitSellTx(connection, built);
      if (r.success) {
        log(`[${pub.slice(0, 4)}…] Sold ✓ ${r.signature}`, 'success');
        // Bump per-wallet counters so frontend can render Sells / SOL gained
        walletEntry.sells = (walletEntry.sells || 0) + 1;
        walletEntry.soldSol = (walletEntry.soldSol || 0) + expectedSolFloat;
      } else {
        log(`[${pub.slice(0, 4)}…] Sell failed: ${JSON.stringify(r.error)}`, 'error');
      }
      send('sell_result', {
        wallet: pub,
        success: r.success,
        signature: r.signature,
        error: r.success ? null : JSON.stringify(r.error),
        sells: walletEntry.sells || 0,
        soldSol: walletEntry.soldSol || 0,
      });
    } catch (e) {
      log(`[${pub.slice(0, 4)}…] Build/send error: ${e.message}`, 'error');
      send('sell_result', { wallet: pub, success: false, signature: null, error: e.message });
    } finally {
      walletEntry.inflight = false;
    }
  }

  function handleStop() {
    if (!session) return;
    if (session.mcInterval) { clearInterval(session.mcInterval); session.mcInterval = null; }
    if (heartbeatInterval) { clearInterval(heartbeatInterval); heartbeatInterval = null; }
    if (session.tradeListener && portal) {
      portal.off(session.tradeListener);
      portal.unsubscribeMint(session.mint.toBase58());
    }
    session.tradeListener = null;
    log('Stopped');
    send('stopped');
    session = null;
  }
});
