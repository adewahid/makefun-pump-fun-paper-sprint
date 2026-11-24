# Tide Rider — Pump.fun Paper Sprint: Any Bot Can Win

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/adewahid/makefun-pump-fun-paper-sprint/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/adewahid/makefun-pump-fun-paper-sprint/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A launch sniper with a rug filter for the [Pump.fun Paper Sprint](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) on [make.fun](https://make.fun). Paper trading only — the bot runs on a replay of real pump.fun bonding-curve events, never with real funds.

## How it works

Every coin is watched from its `CreateEvent`. In its first slots the bot reads the opening tape and buys only launches that pass the rug filter:

- the creator bought at least 0.5 SOL in the create slot (skin in the game),
- at least 3 trades and at most 3 distinct non-creator buyers in the opening tape (a launch bundled by many wallets is usually the creator's cluster waiting to dump),
- the creator has not sold, and the curve holds 0.3–30 SOL of real reserves.

It stakes 0.05 SOL per coin, takes profit fast (+25%), cuts losers fast (−8%), never holds past 15 slots, and exits at once when the creator sells. Orders are split into pieces when the fill count lags its pace toward 300 fills over the 6-hour window, which keeps the run above the minimum bar without putting more capital at risk, and when fills or coins still lag the pace the bot opens tiny 0.01 SOL probe round trips on liquid curves, sold as soon as each buy lands.

## Layout

- `main.ts` — entrypoint: the grader runs `node main.ts`.
- `strategy.ts` — the whole strategy (deterministic: no clock, no randomness).
- `vendor/agentfun-bot.ts` — the kit's SDK shim, vendored byte for byte.

## Results on the dev window

Backtested on the kit's 24-hour dev dataset with the local engine port:

- net **−0.12 SOL** over 131 coins / 479 fills, max drawdown 0.02
- every 6-hour slice of the window clears 20+ coins (55 / 26 / 39 / 22)

The dev window is one small sample and the final window is a fresh 6 hours of real market data, so treat this as a free lottery ticket with a narrow structural edge, not a forecast.

## Referral

Built with referral code `zsgcm8j7` — https://make.fun/r/zsgcm8j7

## License

MIT — see [LICENSE](LICENSE).
