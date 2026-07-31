//! Turning a swap into a trade.
//!
//! A Uniswap V3 `Swap` is one exchange: the trader gives one token and receives
//! the other. This module is the pure step that reads that off a `SwapRow` —
//! which token was bought, which was sold, in human units — and prices the trade
//! in USD from *our own* indexed data, no external feed.
//!
//! It is deliberately free of I/O. The pool's token metadata and the USD price
//! lookup are passed in, so the engine (#72) supplies real values from the
//! `pools` table and our candles, and tests supply hand-worked ones. That is
//! what keeps the whole cost-basis story hand-checkable against a block explorer
//! (the milestone exit).
//!
//! ## Direction
//! `amount0`/`amount1` are signed *from the pool's perspective*: a positive
//! amount flowed into the pool (the trader sold it), a negative amount left the
//! pool (the trader bought it). So exactly one leg is a sell and the other a buy.
//!
//! ## Wallet identity
//! The wallet is the swap's `recipient` — the address that received the output.
//! Caveat, documented not solved: when a swap is routed, `sender` is the router
//! and `recipient` is usually the true EOA, but some aggregators break this. Fully
//! attributing routed swaps is out of scope here.
//!
//! ## USD value
//! Priced off the *numeraire* leg — a token we can value from our own data
//! (stablecoins at $1, WETH from the WETH/USDC candle). The `price` closure
//! returns `Some(usd_per_token)` for such tokens and `None` otherwise, so the
//! numeraire set lives at the call site, not baked in here. A swap with a priced
//! leg is valued from it; a swap between two unpriceable tokens (rare in the top
//! pools) is returned as `Unpriceable` — counted by the engine, not valued.

use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use chainscope_core::types::{Address20, SwapRow};

/// Static token metadata for the pool a swap happened in. Supplied by the engine
/// from the `pools` row; the classifier never touches the database.
#[derive(Debug, Clone)]
pub struct PoolMeta {
    pub token0: Address20,
    pub token1: Address20,
    pub token0_decimals: u8,
    pub token1_decimals: u8,
}

/// One trade: a buy leg and a sell leg for one wallet, amounts in human units,
/// with the trade's USD value taken from the numeraire leg. The engine opens a
/// FIFO lot in `bought` at `value_usd / bought_qty` and realises `value_usd` of
/// proceeds against lots of `sold`.
#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub wallet: Address20,
    pub bought: Address20,
    pub bought_qty: BigDecimal,
    pub sold: Address20,
    pub sold_qty: BigDecimal,
    pub value_usd: BigDecimal,
}

/// The result of classifying a swap.
#[derive(Debug, Clone, PartialEq)]
pub enum Classified {
    /// Priced from a numeraire leg — folds into PnL.
    Priced(Trade),
    /// Neither leg is a token we can price from our own data. The position still
    /// moved, but there is no USD basis, so the engine counts it and moves on.
    Unpriceable {
        wallet: Address20,
        bought: Address20,
        sold: Address20,
    },
}

/// Scale a raw integer amount to human units: `raw / 10^decimals`, exactly.
///
/// `BigDecimal::new(m, s)` is `m * 10^-s`, so multiplying by `10^-decimals` is a
/// pure scale shift — no rounding, unlike a `/` that would impose a precision.
fn to_human(raw: &BigDecimal, decimals: u8) -> BigDecimal {
    raw * BigDecimal::new(BigInt::from(1), decimals as i64)
}

/// Classify one swap into a trade, pricing it with `price(token) -> usd_per_token`.
pub fn classify<F>(swap: &SwapRow, pool: &PoolMeta, price: F) -> Classified
where
    F: Fn(&Address20) -> Option<BigDecimal>,
{
    let zero = BigDecimal::from(0);
    // token0 flowed into the pool -> the trader sold token0, bought token1.
    let sold_token0 = swap.amount0 > zero;

    let (sold, sold_raw, sold_dec, bought, bought_raw, bought_dec) = if sold_token0 {
        (
            pool.token0,
            &swap.amount0,
            pool.token0_decimals,
            pool.token1,
            &swap.amount1,
            pool.token1_decimals,
        )
    } else {
        (
            pool.token1,
            &swap.amount1,
            pool.token1_decimals,
            pool.token0,
            &swap.amount0,
            pool.token0_decimals,
        )
    };

    let sold_qty = to_human(&sold_raw.abs(), sold_dec);
    let bought_qty = to_human(&bought_raw.abs(), bought_dec);
    let wallet = swap.recipient;

    // Prefer the sold leg: proceeds are what a sell realises, so valuing the
    // trade from the token leaving the wallet keeps realised PnL grounded in the
    // numeraire actually received/paid.
    let value_usd = match (price(&sold), price(&bought)) {
        (Some(p), _) => &sold_qty * p,
        (None, Some(p)) => &bought_qty * p,
        (None, None) => {
            return Classified::Unpriceable {
                wallet,
                bought,
                sold,
            }
        }
    };

    Classified::Priced(Trade {
        wallet,
        bought,
        bought_qty,
        sold,
        sold_qty,
        value_usd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const WETH: Address20 = [0xC0; 20];
    const USDC: Address20 = [0xA0; 20];
    const SHIB: Address20 = [0x5b; 20];

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    /// WETH/USDC pool: token0 = WETH (18), token1 = USDC (6).
    fn weth_usdc() -> PoolMeta {
        PoolMeta {
            token0: WETH,
            token1: USDC,
            token0_decimals: 18,
            token1_decimals: 6,
        }
    }

    /// Stables at $1, WETH at $3000, everything else unpriceable.
    fn prices(t: &Address20) -> Option<BigDecimal> {
        if *t == USDC {
            Some(bd("1"))
        } else if *t == WETH {
            Some(bd("3000"))
        } else {
            None
        }
    }

    fn swap(amount0: &str, amount1: &str, recipient: Address20) -> SwapRow {
        SwapRow {
            tx_hash: [0; 32],
            log_index: 0,
            pool: [0; 20],
            sender: [0xff; 20],
            recipient,
            amount0: bd(amount0),
            amount1: bd(amount1),
            sqrt_price_x96: bd("0"),
            liquidity: bd("0"),
            tick: 0,
        }
    }

    #[test]
    fn sell_weth_for_usdc() {
        // token0 (WETH) into the pool: +1e18. token1 (USDC) out: -3000e6.
        let wallet = [0x11; 20];
        let s = swap("1000000000000000000", "-3000000000", wallet);
        let Classified::Priced(t) = classify(&s, &weth_usdc(), prices) else {
            panic!("priced");
        };
        assert_eq!(t.wallet, wallet);
        assert_eq!(t.sold, WETH);
        assert_eq!(t.sold_qty, bd("1"));
        assert_eq!(t.bought, USDC);
        assert_eq!(t.bought_qty, bd("3000")); // 6-decimal scaling correct
        assert_eq!(t.value_usd, bd("3000")); // 1 WETH * $3000
    }

    #[test]
    fn buy_weth_with_usdc_is_the_mirror() {
        // token0 (WETH) out: -0.5e18. token1 (USDC) in: +1500e6.
        let wallet = [0x22; 20];
        let s = swap("-500000000000000000", "1500000000", wallet);
        let Classified::Priced(t) = classify(&s, &weth_usdc(), prices) else {
            panic!("priced");
        };
        assert_eq!(t.sold, USDC);
        assert_eq!(t.sold_qty, bd("1500"));
        assert_eq!(t.bought, WETH);
        assert_eq!(t.bought_qty, bd("0.5"));
        // Sold leg is the numeraire (USDC): 1500 * $1.
        assert_eq!(t.value_usd, bd("1500"));
    }

    #[test]
    fn priced_from_the_bought_leg_when_the_sold_one_is_unknown() {
        // Sell SHIB (unpriceable), buy WETH — value must come from the WETH leg.
        let pool = PoolMeta {
            token0: SHIB,
            token1: WETH,
            token0_decimals: 18,
            token1_decimals: 18,
        };
        // token0 (SHIB) into pool: +big. token1 (WETH) out: -2e18.
        let s = swap("42000000000000000000000000", "-2000000000000000000", [0x33; 20]);
        let Classified::Priced(t) = classify(&s, &pool, prices) else {
            panic!("priced");
        };
        assert_eq!(t.sold, SHIB);
        assert_eq!(t.bought, WETH);
        assert_eq!(t.bought_qty, bd("2"));
        assert_eq!(t.value_usd, bd("6000")); // 2 WETH * $3000
    }

    #[test]
    fn two_unknown_tokens_is_unpriceable() {
        let other: Address20 = [0x7c; 20];
        let pool = PoolMeta {
            token0: SHIB,
            token1: other,
            token0_decimals: 18,
            token1_decimals: 18,
        };
        let s = swap("1000000000000000000", "-2000000000000000000", [0x44; 20]);
        assert_eq!(
            classify(&s, &pool, prices),
            Classified::Unpriceable {
                wallet: [0x44; 20],
                bought: other,
                sold: SHIB,
            }
        );
    }
}
