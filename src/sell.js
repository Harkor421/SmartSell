// Sell helpers — same logic as WalletCleaner's buildBondingCurveBundle / buildPumpSwapBundle,
// but produces a single sell-only TX (no buy, no Jito tip) and submits via plain RPC.
//
// IMPORTANT (rate-limit budget): every sell call previously fetched ~6 RPC accounts,
// which blew through Helius free-tier limits during a hot trade stream. We now cache
// the slow-changing pieces in a per-session SellContext: tokenProgramId, migrated flag,
// global + feeConfig (~10s TTL), and reuse one blockhash for ~10s. Each sell ends up
// at ~2 RPC calls (sellState + sendTransaction) instead of 6.

import {
  PublicKey,
  VersionedTransaction,
  TransactionMessage,
  ComputeBudgetProgram,
} from '@solana/web3.js';
import { getAssociatedTokenAddressSync, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID } from '@solana/spl-token';
import BN from 'bn.js';
import {
  OnlinePumpSdk,
  PumpSdk,
  getSellSolAmountFromTokenAmount,
} from '@pump-fun/pump-sdk';
import {
  OnlinePumpAmmSdk,
  PumpAmmSdk,
  canonicalPumpPoolPda,
  sellBaseInput,
} from '@pump-fun/pump-swap-sdk';

export async function detectTokenProgram(connection, mint) {
  const info = await connection.getAccountInfo(mint, 'confirmed');
  if (!info) return TOKEN_2022_PROGRAM_ID;
  return info.owner.equals(TOKEN_2022_PROGRAM_ID) ? TOKEN_2022_PROGRAM_ID : TOKEN_PROGRAM_ID;
}

// True if the token has migrated to PumpSwap (bonding curve complete)
export async function isMigrated(connection, mint) {
  const onlinePumpSdk = new OnlinePumpSdk(connection);
  try {
    const bc = await onlinePumpSdk.fetchBondingCurve(mint);
    return !!bc.complete;
  } catch {
    // No bonding curve account — likely migrated and account closed, or token isn't pump.fun
    return true;
  }
}

// Get the seller's current token balance (raw, no decimals applied)
export async function getSellerTokenBalance(connection, mint, sellerPubkey, tokenProgramId) {
  const ata = getAssociatedTokenAddressSync(mint, sellerPubkey, false, tokenProgramId);
  try {
    const bal = await connection.getTokenAccountBalance(ata, 'confirmed');
    return BigInt(bal.value.amount);
  } catch {
    return 0n;
  }
}

// Build a SELL-only versioned tx for the bonding curve (pre-migration).
// tokenAmount is raw (with decimals).
export async function buildBondingCurveSellTx(connection, seller, mint, tokenProgramId, tokenAmount, slippageBps = 1500) {
  const onlinePumpSdk = new OnlinePumpSdk(connection);
  const pumpSdk = new PumpSdk();
  const tokenAmountBN = new BN(tokenAmount.toString());
  const slippagePct = slippageBps / 100;

  const [global, feeConfig, sellState, { blockhash, lastValidBlockHeight }] = await Promise.all([
    onlinePumpSdk.fetchGlobal(),
    onlinePumpSdk.fetchFeeConfig(),
    onlinePumpSdk.fetchSellState(mint, seller.publicKey, tokenProgramId),
    connection.getLatestBlockhash('confirmed'),
  ]);

  const bondingCurve = sellState.bondingCurve;
  const sellSolAmount = getSellSolAmountFromTokenAmount({
    global, feeConfig, mintSupply: bondingCurve.tokenTotalSupply, bondingCurve, amount: tokenAmountBN,
  });

  const sellIxs = await pumpSdk.sellInstructions({
    global,
    bondingCurveAccountInfo: sellState.bondingCurveAccountInfo,
    bondingCurve,
    mint,
    user: seller.publicKey,
    amount: tokenAmountBN,
    solAmount: sellSolAmount,
    slippage: slippagePct,
    tokenProgram: tokenProgramId,
    mayhemMode: bondingCurve.isMayhemMode,
    cashback: bondingCurve.isCashbackCoin,
  });

  const tx = new VersionedTransaction(
    new TransactionMessage({
      payerKey: seller.publicKey,
      recentBlockhash: blockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }),
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 200_000 }),
        ...sellIxs,
      ],
    }).compileToV0Message()
  );
  tx.sign([seller]);

  return { tx, blockhash, lastValidBlockHeight, expectedSolOut: BigInt(sellSolAmount.toString()) };
}

// Build a SELL-only versioned tx on PumpSwap (post-migration).
export async function buildPumpSwapSellTx(connection, seller, mint, tokenAmount, slippageBps = 1500) {
  const onlineSdk = new OnlinePumpAmmSdk(connection);
  const pumpAmmSdk = new PumpAmmSdk();
  const poolKey = canonicalPumpPoolPda(mint);
  const tokenAmountBN = new BN(tokenAmount.toString());
  const slippage = slippageBps / 10000;

  const [sellerSwapState, { blockhash, lastValidBlockHeight }] = await Promise.all([
    onlineSdk.swapSolanaState(poolKey, seller.publicKey),
    connection.getLatestBlockhash('confirmed'),
  ]);

  const sellQuote = sellBaseInput({
    base: tokenAmountBN, slippage,
    baseReserve: sellerSwapState.poolBaseAmount, quoteReserve: sellerSwapState.poolQuoteAmount,
    globalConfig: sellerSwapState.globalConfig, baseMintAccount: sellerSwapState.baseMintAccount,
    baseMint: sellerSwapState.baseMint, coinCreator: sellerSwapState.pool.coinCreator,
    creator: sellerSwapState.pool.creator, feeConfig: sellerSwapState.feeConfig,
  });

  const sellIxs = await pumpAmmSdk.sellBaseInput(sellerSwapState, tokenAmountBN, slippage);

  const tx = new VersionedTransaction(
    new TransactionMessage({
      payerKey: seller.publicKey,
      recentBlockhash: blockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }),
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 200_000 }),
        ...sellIxs,
      ],
    }).compileToV0Message()
  );
  tx.sign([seller]);

  return { tx, blockhash, lastValidBlockHeight, expectedSolOut: BigInt(sellQuote.internalQuoteAmountOut.toString()) };
}

// Single entry point that picks the right builder based on migration state
export async function buildSellTx(connection, seller, mint, tokenAmount, slippageBps = 1500) {
  const tokenProgramId = await detectTokenProgram(connection, mint);
  const migrated = await isMigrated(connection, mint);
  if (migrated) {
    const r = await buildPumpSwapSellTx(connection, seller, mint, tokenAmount, slippageBps);
    return { ...r, migrated };
  }
  const r = await buildBondingCurveSellTx(connection, seller, mint, tokenProgramId, tokenAmount, slippageBps);
  return { ...r, migrated };
}

/**
 * Per-session cache for the slow-changing values that the sell pipeline normally re-fetches
 * on every call. Created once at session setup, refreshed lazily as needed.
 */
export class SellContext {
  constructor(connection, mint) {
    this.connection = connection;
    this.mint = mint;
    this.tokenProgramId = null;       // detected once
    this.migrated = null;              // re-checked occasionally if pre-bond
    this.global = null;                // cached ~30s
    this.feeConfig = null;             // cached ~30s
    this.globalCachedAt = 0;
    this.lastMigrationCheckAt = 0;
    this.blockhash = null;             // cached ~8s
    this.lastValidBlockHeight = null;
    this.blockhashCachedAt = 0;
  }

  async init() {
    this.tokenProgramId = await detectTokenProgram(this.connection, this.mint);
    this.migrated = await isMigrated(this.connection, this.mint);
    await this.refreshGlobalIfStale(true);
    return this;
  }

  async refreshGlobalIfStale(force = false) {
    const now = Date.now();
    if (!force && this.global && this.feeConfig && now - this.globalCachedAt < 30_000) return;
    if (this.migrated) return; // PumpSwap path doesn't need pump-sdk global
    const onlinePumpSdk = new OnlinePumpSdk(this.connection);
    const [g, fc] = await Promise.all([
      onlinePumpSdk.fetchGlobal(),
      onlinePumpSdk.fetchFeeConfig(),
    ]);
    this.global = g;
    this.feeConfig = fc;
    this.globalCachedAt = now;
  }

  async refreshMigrationIfStale() {
    if (this.migrated) return; // can't un-migrate
    const now = Date.now();
    if (now - this.lastMigrationCheckAt < 15_000) return;
    this.lastMigrationCheckAt = now;
    const m = await isMigrated(this.connection, this.mint);
    if (m && !this.migrated) {
      this.migrated = true;
      this.global = null; this.feeConfig = null; // not needed anymore
    }
  }

  async getBlockhash(ttlMs = 8000) {
    const now = Date.now();
    if (this.blockhash && now - this.blockhashCachedAt < ttlMs) {
      return { blockhash: this.blockhash, lastValidBlockHeight: this.lastValidBlockHeight };
    }
    const r = await this.connection.getLatestBlockhash('confirmed');
    this.blockhash = r.blockhash;
    this.lastValidBlockHeight = r.lastValidBlockHeight;
    this.blockhashCachedAt = now;
    return r;
  }

  // Per-seller sellState cache. Bonding curve state changes per trade, but for the purpose
  // of building a sell tx within a few seconds, the staleness is fine — the SDK applies
  // 15% slippage so price drift is absorbed. Big RPC saver under heavy trade load.
  async getSellStateCached(sellerPubkey, ttlMs = 5000) {
    const key = sellerPubkey.toBase58();
    if (!this._sellStateCache) this._sellStateCache = new Map();
    const entry = this._sellStateCache.get(key);
    const now = Date.now();
    if (entry && now - entry.at < ttlMs) return entry.data;
    const onlinePumpSdk = new OnlinePumpSdk(this.connection);
    const data = await onlinePumpSdk.fetchSellState(this.mint, sellerPubkey, this.tokenProgramId);
    this._sellStateCache.set(key, { data, at: now });
    return data;
  }
}

// Cached versions: take a SellContext to skip redundant fetches.
// `priorityFeeLamports` is the total priority fee paid in lamports (e.g. 5_000_000 = 0.005 SOL).
// Compute-unit price is derived from it given a fixed CU limit of 200k.
export async function buildSellTxCached(ctx, seller, tokenAmount, slippageBps = 1500, priorityFeeLamports = 5_000_000) {
  await ctx.refreshMigrationIfStale();
  if (ctx.migrated) {
    return await buildPumpSwapSellTxCached(ctx, seller, tokenAmount, slippageBps, priorityFeeLamports);
  }
  return await buildBondingCurveSellTxCached(ctx, seller, tokenAmount, slippageBps, priorityFeeLamports);
}

// Convert a target priority fee (in lamports) into a compute-unit price (micro-lamports/CU)
// given a fixed CU limit. priority_fee_lamports = cu_limit * cu_price_microlamports / 1e6
function microLamportsForFee(priorityFeeLamports, cuLimit) {
  const v = Math.floor((Number(priorityFeeLamports) * 1_000_000) / cuLimit);
  return Math.max(1, v);
}

async function buildBondingCurveSellTxCached(ctx, seller, tokenAmount, slippageBps, priorityFeeLamports) {
  await ctx.refreshGlobalIfStale();
  const pumpSdk = new PumpSdk();
  const tokenAmountBN = new BN(tokenAmount.toString());
  const slippagePct = slippageBps / 100;
  const CU_LIMIT = 200_000;
  const cuPrice = microLamportsForFee(priorityFeeLamports ?? 5_000_000, CU_LIMIT);

  // sellState cached per-seller for ~5s so back-to-back triggers don't re-fetch.
  const [sellState, blockInfo] = await Promise.all([
    ctx.getSellStateCached(seller.publicKey),
    ctx.getBlockhash(),
  ]);

  const bondingCurve = sellState.bondingCurve;
  const sellSolAmount = getSellSolAmountFromTokenAmount({
    global: ctx.global, feeConfig: ctx.feeConfig,
    mintSupply: bondingCurve.tokenTotalSupply, bondingCurve, amount: tokenAmountBN,
  });

  const sellIxs = await pumpSdk.sellInstructions({
    global: ctx.global,
    bondingCurveAccountInfo: sellState.bondingCurveAccountInfo,
    bondingCurve,
    mint: ctx.mint,
    user: seller.publicKey,
    amount: tokenAmountBN,
    solAmount: sellSolAmount,
    slippage: slippagePct,
    tokenProgram: ctx.tokenProgramId,
    mayhemMode: bondingCurve.isMayhemMode,
    cashback: bondingCurve.isCashbackCoin,
  });

  const tx = new VersionedTransaction(
    new TransactionMessage({
      payerKey: seller.publicKey,
      recentBlockhash: blockInfo.blockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: CU_LIMIT }),
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: cuPrice }),
        ...sellIxs,
      ],
    }).compileToV0Message()
  );
  tx.sign([seller]);
  return { tx, blockhash: blockInfo.blockhash, lastValidBlockHeight: blockInfo.lastValidBlockHeight, expectedSolOut: BigInt(sellSolAmount.toString()), migrated: false };
}

async function buildPumpSwapSellTxCached(ctx, seller, tokenAmount, slippageBps, priorityFeeLamports) {
  const onlineSdk = new OnlinePumpAmmSdk(ctx.connection);
  const pumpAmmSdk = new PumpAmmSdk();
  const poolKey = canonicalPumpPoolPda(ctx.mint);
  const tokenAmountBN = new BN(tokenAmount.toString());
  const slippage = slippageBps / 10000;
  const CU_LIMIT = 200_000;
  const cuPrice = microLamportsForFee(priorityFeeLamports ?? 5_000_000, CU_LIMIT);

  const [sellerSwapState, blockInfo] = await Promise.all([
    onlineSdk.swapSolanaState(poolKey, seller.publicKey),
    ctx.getBlockhash(),
  ]);

  const sellQuote = sellBaseInput({
    base: tokenAmountBN, slippage,
    baseReserve: sellerSwapState.poolBaseAmount, quoteReserve: sellerSwapState.poolQuoteAmount,
    globalConfig: sellerSwapState.globalConfig, baseMintAccount: sellerSwapState.baseMintAccount,
    baseMint: sellerSwapState.baseMint, coinCreator: sellerSwapState.pool.coinCreator,
    creator: sellerSwapState.pool.creator, feeConfig: sellerSwapState.feeConfig,
  });
  const sellIxs = await pumpAmmSdk.sellBaseInput(sellerSwapState, tokenAmountBN, slippage);

  const tx = new VersionedTransaction(
    new TransactionMessage({
      payerKey: seller.publicKey,
      recentBlockhash: blockInfo.blockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: CU_LIMIT }),
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: cuPrice }),
        ...sellIxs,
      ],
    }).compileToV0Message()
  );
  tx.sign([seller]);
  return { tx, blockhash: blockInfo.blockhash, lastValidBlockHeight: blockInfo.lastValidBlockHeight, expectedSolOut: BigInt(sellQuote.internalQuoteAmountOut.toString()), migrated: true };
}

// Submit a sell tx via plain RPC and wait for confirmation. skipPreflight=true so we
// don't burn an extra simulation RPC call AND don't reject when our cached sellState
// is slightly stale — slippage absorbs price drift on actual execution.
export async function submitSellTx(connection, built) {
  const sig = await connection.sendTransaction(built.tx, { skipPreflight: true, maxRetries: 3 });
  const conf = await connection.confirmTransaction(
    { signature: sig, blockhash: built.blockhash, lastValidBlockHeight: built.lastValidBlockHeight },
    'confirmed'
  );
  return { signature: sig, success: !conf.value.err, error: conf.value.err };
}
