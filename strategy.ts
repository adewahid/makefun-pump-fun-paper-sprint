// Tide Rider: a launch sniper with a rug filter, for the pump.fun replay
// (agent-fun/replay/v2).
//
// How it trades. The harness says hello, then streams one batch per slot:
// that slot's chain events, the marks on the curves the bot cares about, the
// fills of the orders it sent, and the account after those fills. The bot
// answers each batch with the orders it wants decided on that slot, and the
// orders fill two slots later.
//
// Every coin is followed from the batch its CreateEvent lands in. The opening
// tape decides whether it is worth buying: how much SOL the creator put in
// on the create slot, how many other wallets bought in beside them, whether
// the creator has already sold, how many trades the tape holds, and how much
// real SOL the curve holds. A launch bundled by many wallets in its first
// slots is usually the creator's cluster waiting to dump, so the filter wants
// a narrow crowd: at most three distinct non-creator buyers. A launch whose
// creator bought at least half a SOL of their own coin is the one the dev
// window liked — unfiltered launches bled money to fees, while that filter
// came out near breakeven.
//
// A buy stakes 0.05 SOL and is a single shot per coin: no adds. The exit is
// quick either way — take profit at +25%, stop loss at -8%, never hold past
// 15 slots, out at once when the creator sells. The stake is small and the
// hold is short, so a bad coin costs little and the drawdown stays tiny.
//
// Two pacing mechanisms keep the run above the minimum bar (round trips of at
// least 0.01 SOL on 20+ coins) even on a short 6-hour final window. First,
// orders split into pieces when the fill count lags its pace toward 300 fills
// over the window: the pieces divide one stake, they never add to it. Second,
// when fills or coins still lag the pace, the bot opens tiny 0.01 SOL probe
// round trips on liquid curves and sells each probe as soon as its buy lands.
//
// Deterministic by construction: no clock, no randomness, iteration only over
// insertion-ordered maps fed by the batch stream. Two runs on the same window
// produce byte-identical action logs.

import { BPS, type Bot, type BotState, type Batch, type OrderInput, type Pubkey } from './vendor/agentfun-bot.ts';

export interface Params {
  /** Youngest age (slots since create) at which a coin may be bought. */
  minAge: number;
  /** Oldest age at which a first buy may still be decided. */
  maxAge: number;
  /** Lamports to aim for per coin in total. */
  stakeLamports: number;
  /** Smallest single buy worth paying the tx fee for. */
  minBuyLamports: number;
  /** Real SOL reserve window at decision time (lamports). */
  minRealSol: number;
  maxRealSol: number;
  /** Creator's own buys in the create slot: lamports window. */
  minDevBuy: number;
  maxDevBuy: number;
  /** Trades seen and distinct (non-creator) buyers seen before entry. */
  minTrades: number;
  minBuyers: number;
  /** Distinct non-creator buyers before entry, at most. */
  maxBuyers: number;
  /** Lamports sold by others / lamports bought, as bps, at most. */
  maxSellRatioBps: number;
  /** Largest single non-creator sell seen, as bps of the real SOL reserve before it, at most. */
  maxBigSellBps: number;
  /** Price now over the create price, as bps: window. */
  minRunupBps: number;
  maxRunupBps: number;
  /** The recent-flow window (slots) and what it must show. */
  flowWindow: number;
  minFlowLamports: number;
  minRecentBuyers: number;
  /** Creator sold before entry: skip the coin. */
  skipDevSold: boolean;
  /** Slippage allowed on buys (bps). */
  buySlippageBps: number;
  /** Exit when the position is worth this much over its cost (bps). */
  tpBps: number;
  /** Exit when it is worth this much under its cost (bps). */
  slBps: number;
  /** Trailing stop: once up at least trailArmBps, exit on a fall of trailBps from the best value. */
  trailArmBps: number;
  trailBps: number;
  /** Exit after holding this many slots. */
  maxHold: number;
  /** Exit when the creator sells while held. */
  exitOnDevSell: boolean;
  /** Exit when no trade has hit the coin for this many slots. */
  staleSlots: number;
  /** Exit when the recent net flow falls below minus this many lamports. */
  exitFlowLamports: number;
  /** Fill pacing: aim for this many fills over windowSeconds of block time. */
  fillTarget: number;
  windowSeconds: number;
  baseSplit: number;
  maxSplit: number;
  minPieceLamports: number;
  /** Probe pacing: keep fills and coins near these targets over paceSecs. */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  probeSolLamports: number;
  maxProbes: number;
}

export const DEFAULT_PARAMS: Params = {
  minAge: 0,
  maxAge: 6,
  stakeLamports: 50_000_000,
  minBuyLamports: 5_000_000,
  minRealSol: 300_000_000,
  maxRealSol: 30_000_000_000,
  minDevBuy: 500_000_000,
  maxDevBuy: 2_500_000_000,
  minTrades: 3,
  minBuyers: 0,
  maxBuyers: 3,
  maxSellRatioBps: 10_000,
  maxBigSellBps: 10_000,
  minRunupBps: 0,
  maxRunupBps: 1_000_000,
  flowWindow: 10,
  minFlowLamports: -1_000_000_000_000_000,
  minRecentBuyers: 0,
  skipDevSold: true,
  buySlippageBps: 10_000,
  tpBps: 2500,
  slBps: 800,
  trailArmBps: 1_000_000,
  trailBps: 1000,
  maxHold: 15,
  exitOnDevSell: true,
  staleSlots: 1_000_000,
  exitFlowLamports: 1_000_000_000_000_000,
  fillTarget: 300,
  windowSeconds: 21_600,
  baseSplit: 1,
  maxSplit: 6,
  minPieceLamports: 2_000_000,
  targetCoins: 25,
  targetFills: 240,
  paceSecs: 21_600,
  probeSolLamports: 10_000_000,
  maxProbes: 1,
};

/** Curve fee taken on a sell's quote (1.25%). */
const SELL_FEE_BPS = 125n;

/** What selling `tokens` against (vq, vt) would pay after curve fees. */
function closeValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const quote = (tokens * vq) / (vt + tokens);
  return quote - (quote * SELL_FEE_BPS + BPS - 1n) / BPS;
}

interface TapeTick {
  slot: number;
  isBuy: boolean;
  sol: bigint;
  user: Pubkey;
}

/** Everything remembered about one launch, from its CreateEvent on. */
interface Launch {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  /** Create-time reserves: the run-up is measured against this price. */
  vq0: bigint;
  vt0: bigint;
  devBuy: bigint;
  trades: number;
  buyers: Set<Pubkey>;
  bought: bigint;
  soldByOthers: bigint;
  bigSellBps: bigint;
  devSold: boolean;
  lastTradeSlot: number;
  /** Trades inside the recent-flow window; older ones are dropped as new ones come. */
  recent: TapeTick[];
  buysSent: number;
  spentSent: bigint;
  /** Never buy it again (exited, filtered out for good, or completed). */
  done: boolean;
  complete: boolean;
}

/** A position the bot opened, with what it needs to judge the exit. */
interface Position {
  openedSlot: number;
  txPaid: bigint;
  best: bigint;
  devSoldAtOpen: boolean;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const launches = new Map<Pubkey, Launch>();
  const held = new Map<Pubkey, Position>();
  /** Probe round trips in flight, by mint: false while buying, true once selling. */
  const probes = new Map<Pubkey, boolean>();
  /** Mints with a sell in flight. */
  const selling = new Set<Pubkey>();
  /** The bot's own filled orders and the coins they were on (harness closes excluded). */
  let ownFills = 0;
  const ownCoins = new Set<Pubkey>();
  const stake = BigInt(p.stakeLamports);
  const minBuy = BigInt(p.minBuyLamports);
  let firstTime: number | null = null;
  let nowTime = 0;

  /** How many pieces to split an order into now: baseSplit, more when the fill count lags its pace. */
  function splitCount(): number {
    if (firstTime === null) return p.baseSplit;
    const elapsed = Math.max(0, Math.min(p.windowSeconds, nowTime - firstTime));
    const due = (p.fillTarget * elapsed) / p.windowSeconds;
    if (ownFills >= due) return p.baseSplit;
    const behind = due - ownFills;
    return Math.min(p.maxSplit, p.baseSplit + (behind > p.fillTarget * 0.15 ? 3 : 1));
  }

  /** Split a sell of `tokens` into at most `k` orders that fit in `room`. */
  function splitSell(mint: Pubkey, tokens: bigint, k: number, room: number): OrderInput[] {
    const n = BigInt(Math.max(1, Math.min(k, room)));
    const out: OrderInput[] = [];
    const each = tokens / n;
    if (each === 0n) return [{ side: 'sell', mint, token_amount: tokens, max_slippage_bps: 10_000 }];
    for (let i = 0n; i < n; i++) {
      const amt = i === n - 1n ? tokens - each * (n - 1n) : each;
      out.push({ side: 'sell', mint, token_amount: amt, max_slippage_bps: 10_000 });
    }
    return out;
  }

  /** Split a buy of `amount` lamports into at most `k` orders that fit in `room`. */
  function splitBuy(mint: Pubkey, amount: bigint, k: number, room: number): OrderInput[] {
    let n = BigInt(Math.max(1, Math.min(k, room)));
    const floor = BigInt(p.minPieceLamports);
    while (n > 1n && amount / n < floor) n -= 1n;
    const each = amount / n;
    const out: OrderInput[] = [];
    for (let i = 0n; i < n; i++) {
      const amt = i === n - 1n ? amount - each * (n - 1n) : each;
      out.push({ side: 'buy', mint, quote_lamports: amt, max_slippage_bps: p.buySlippageBps });
    }
    return out;
  }

  /** Fold one batch's events into the launch tapes. */
  function ingest(batch: Batch): void {
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        launches.set(e.mint, {
          mint: e.mint,
          createSlot: e.slot,
          creator: e.creator,
          vq0: BigInt(e.virtual_quote_reserves),
          vt0: BigInt(e.virtual_token_reserves),
          devBuy: 0n,
          trades: 0,
          buyers: new Set(),
          bought: 0n,
          soldByOthers: 0n,
          bigSellBps: 0n,
          devSold: false,
          lastTradeSlot: e.slot,
          recent: [],
          buysSent: 0,
          spentSent: 0n,
          done: false,
          complete: false,
        });
      } else if (e.event === 'TradeEvent') {
        const c = launches.get(e.mint);
        if (c === undefined) continue;
        const sol = BigInt(e.sol_amount);
        c.trades += 1;
        c.lastTradeSlot = e.slot;
        c.recent.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user });
        const fromDev = e.user === c.creator;
        if (e.is_buy) {
          c.bought += sol;
          if (fromDev && e.slot === c.createSlot) c.devBuy += sol;
          else if (!fromDev) c.buyers.add(e.user);
        } else if (fromDev) {
          c.devSold = true;
        } else {
          c.soldByOthers += sol;
          const reserveBefore = BigInt(e.real_quote_reserves) + sol;
          if (reserveBefore > 0n) {
            const bps = (sol * BPS) / reserveBefore;
            if (bps > c.bigSellBps) c.bigSellBps = bps;
          }
        }
      } else {
        const c = launches.get(e.mint);
        if (c !== undefined) {
          c.complete = true;
          c.done = true;
        }
      }
    }
  }

  /** Net lamports bought minus sold, and distinct non-creator buyers, over the last flowWindow slots. */
  function recentFlow(c: Launch, slot: number): { net: bigint; buyers: number } {
    const from = slot - p.flowWindow;
    let drop = 0;
    while (drop < c.recent.length && c.recent[drop]!.slot <= from) drop++;
    if (drop > 0) c.recent.splice(0, drop);
    let net = 0n;
    const buyers = new Set<Pubkey>();
    for (const t of c.recent) {
      if (t.isBuy) {
        net += t.sol;
        if (t.user !== c.creator) buyers.add(t.user);
      } else net -= t.sol;
    }
    return { net, buyers: buyers.size };
  }

  /** Is the launch worth a first buy at this slot? Every filter must pass. */
  function qualifies(c: Launch, curve: { real_quote_reserves: bigint; virtual_quote_reserves: bigint; virtual_token_reserves: bigint }, slot: number): boolean {
    const rq = curve.real_quote_reserves;
    if (rq < BigInt(p.minRealSol) || rq > BigInt(p.maxRealSol)) return false;
    if (c.devBuy < BigInt(p.minDevBuy) || c.devBuy > BigInt(p.maxDevBuy)) return false;
    if (c.trades < p.minTrades || c.buyers.size < p.minBuyers || c.buyers.size > p.maxBuyers) return false;
    if (c.bought > 0n && c.soldByOthers * BPS > c.bought * BigInt(p.maxSellRatioBps)) return false;
    if (c.bigSellBps > BigInt(p.maxBigSellBps)) return false;
    const vq = curve.virtual_quote_reserves;
    const vt = curve.virtual_token_reserves;
    const runup = (vq * c.vt0 * BPS) / (vt * c.vq0);
    if (runup > BigInt(p.maxRunupBps) || runup < BigInt(p.minRunupBps)) return false;
    const fl = recentFlow(c, slot);
    if (fl.net < BigInt(p.minFlowLamports) || fl.buyers < p.minRecentBuyers) return false;
    return true;
  }

  /** Why a position should be closed now, or null to keep holding it. */
  function exitReason(mint: Pubkey, h: Position, value: bigint, basis: bigint, slot: number): string | null {
    if (basis > 0n) {
      if (value * BPS >= basis * (BPS + BigInt(p.tpBps))) return 'take-profit';
      if (value * BPS <= basis * (BPS - BigInt(p.slBps))) return 'stop-loss';
      if (h.best * BPS >= basis * (BPS + BigInt(p.trailArmBps)) && value * BPS <= h.best * (BPS - BigInt(p.trailBps))) return 'trailing-stop';
    }
    if (slot - h.openedSlot >= p.maxHold) return 'max-hold';
    const c = launches.get(mint);
    if (c !== undefined) {
      if (p.exitOnDevSell && c.devSold && !h.devSoldAtOpen) return 'creator-sold';
      if (slot - c.lastTradeSlot >= p.staleSlots) return 'stale';
      if (recentFlow(c, slot).net < -BigInt(p.exitFlowLamports)) return 'flow-faded';
    }
    return null;
  }

  function hasPendingBuy(state: BotState, mint: Pubkey): boolean {
    for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'buy') return true;
    return false;
  }

  function hasPendingSell(state: BotState, mint: Pubkey): boolean {
    for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'sell') return true;
    return false;
  }

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      ingest(batch);
      for (const e of batch.events) {
        if (e.block_time === null) continue;
        if (firstTime === null) firstTime = e.block_time;
        if (e.block_time > nowTime) nowTime = e.block_time;
      }
      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = state.txFeeLamports();
      const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
      for (const m of batch.marks) marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });

      for (const f of batch.fills) {
        if (f.client_id.startsWith('_')) {
          held.delete(f.mint);
          selling.delete(f.mint);
          continue;
        }
        if (f.status === 'filled') {
          ownFills += 1;
          ownCoins.add(f.mint);
        }
        const h = held.get(f.mint);
        if (f.status !== 'rejected' && h !== undefined) h.txPaid += BigInt(f.tx_fee_lamports);
        if (f.side === 'buy' && f.status === 'filled' && h === undefined) {
          held.set(f.mint, { openedSlot: f.slot, txPaid: BigInt(f.tx_fee_lamports), best: 0n, devSoldAtOpen: launches.get(f.mint)?.devSold ?? false });
        }
      }
      for (const mint of [...selling]) if (!hasPendingSell(state, mint)) selling.delete(mint);
      for (const mint of [...held.keys()]) {
        if (!state.positions.has(mint) && !selling.has(mint) && !hasPendingBuy(state, mint)) held.delete(mint);
      }
      const k = splitCount();

      // Exits first: every real position is judged on its close value against its cost.
      for (const [mint, pos] of state.positions) {
        if (orders.length >= cap) break;
        if (selling.has(mint) || probes.has(mint)) continue;
        const c = launches.get(mint);
        let h = held.get(mint);
        if (h === undefined) {
          h = { openedSlot: state.slot, txPaid: tx, best: 0n, devSoldAtOpen: c?.devSold ?? false };
          held.set(mint, h);
        }
        let mk = marks.get(mint);
        if (mk === undefined) {
          const cv = state.curves.get(mint);
          if (cv === undefined) continue;
          mk = { vq: cv.virtual_quote_reserves, vt: cv.virtual_token_reserves };
        }
        const value = closeValue(pos.token_amount, mk.vq, mk.vt) - tx;
        const basis = pos.cost_lamports + h.txPaid;
        if (value > h.best) h.best = value;
        if (exitReason(mint, h, value, basis, state.slot) === null) continue;
        orders.push(...splitSell(mint, pos.token_amount, k, cap - orders.length));
        selling.add(mint);
        if (c !== undefined) c.done = true;
      }

      // Probe round trips: sell the whole probe as soon as its buy has landed.
      for (const [mint, sold] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!hasPendingBuy(state, mint) && !hasPendingSell(state, mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || hasPendingBuy(state, mint) || hasPendingSell(state, mint)) continue;
        if (orders.length >= cap) break;
        orders.push(...splitSell(mint, pos.token_amount, k, cap - orders.length));
        selling.add(mint);
        if (!sold) probes.set(mint, true);
      }

      // Entries: one first buy per launch that passes the rug filter.
      let free = state.freeCashLamports() - tx * BigInt(orders.length);
      let open = state.committedPositionCount();
      for (const c of launches.values()) {
        if (orders.length >= cap) break;
        if (c.done || c.complete) continue;
        const age = state.slot - c.createSlot;
        if (age > p.maxAge) {
          c.done = true;
          continue;
        }
        if (age < p.minAge) continue;
        if (open >= state.hello.caps.max_open_positions) continue;
        const curve = state.curves.get(c.mint);
        if (curve === undefined || curve.complete) continue;
        if (c.devSold && p.skipDevSold) {
          c.done = true;
          continue;
        }
        if (!qualifies(c, curve, state.slot)) continue;
        const room = stake - c.spentSent;
        if (room < minBuy) continue;
        const maxBuy = state.maxBuyLamports(c.mint);
        const amount = room < maxBuy ? room : maxBuy;
        if (amount < minBuy) continue;
        if (free < amount + tx * 6n) continue;
        const buys = splitBuy(c.mint, amount, k, cap - orders.length);
        orders.push(...buys);
        free -= amount + tx * BigInt(buys.length);
        c.buysSent += 1;
        c.spentSent += amount;
        open += 1;
      }

      // Guardrail pacing: when the bot's own fills or coins lag the pace, open a
      // tiny probe round trip on a liquid curve so the run still clears the bar.
      if (probes.size < p.maxProbes && open < state.hello.caps.max_open_positions && firstTime !== null) {
        const pace = Math.min(1, (nowTime - firstTime) / p.paceSecs);
        const needCoins = ownCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = ownFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const probeSol = BigInt(p.probeSolLamports);
          const minRq = (probeSol * 10_000n) / BigInt(state.hello.caps.max_order_bps_of_real_reserve);
          for (const e of batch.events) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (e.event !== 'TradeEvent') continue;
            const mint = e.mint;
            if (held.has(mint) || probes.has(mint) || hasPendingBuy(state, mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && ownCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (curve.real_quote_reserves < (minRq * 3n) / 2n) continue;
            if (free < probeSol + 2n * tx) break;
            orders.push({ side: 'buy', mint, quote_lamports: probeSol, max_slippage_bps: p.buySlippageBps });
            probes.set(mint, false);
            free -= probeSol + tx;
          }
        }
      }
      return orders;
    },
  };
}
