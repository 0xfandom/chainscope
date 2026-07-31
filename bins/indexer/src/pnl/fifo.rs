//! FIFO cost-basis maths, pure.
//!
//! A wallet's position in one token is a queue of purchase lots, oldest first.
//! A buy pushes a lot. A sell consumes lots from the front, and the realised
//! profit is the proceeds minus the cost basis of exactly those consumed lots.
//! "First in, first out" — the oldest coins are the ones sold.
//!
//! This module knows nothing about the database or USD sourcing; it is the
//! arithmetic alone, so it can be hand-checked against a block explorer and
//! reused unchanged by the reorg reversal (#73) and the oracle in the exit test.
//!
//! ## Conservation and determinism
//! A sell's proceeds are split across the lots it touches. `cost` per lot is
//! exact (`qty * unit_cost`, no division). Realised PnL is reported per lot as
//! `proceeds_part - cost` and the position's stats delta is the *sum of those
//! per-lot values* — so whatever a reorg later subtracts (the stored ledger
//! rows) matches to the last digit what the forward fold added. The maths is
//! deterministic, so the exit oracle running the same code lands bit-identically.

use bigdecimal::BigDecimal;

/// One FIFO purchase lot. Serialised into `wallet_positions.lots`.
#[derive(Debug, Clone, PartialEq)]
pub struct Lot {
    pub qty: BigDecimal,
    /// Unit cost in USD at acquisition.
    pub price_usd: BigDecimal,
    pub block: u64,
}

/// A wallet's open position in one token: its lots, oldest first.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Position {
    pub lots: Vec<Lot>,
}

/// One lot drawdown a sell caused — becomes a `lot_consumptions` row, and the
/// exact record a reorg replays in reverse.
#[derive(Debug, Clone, PartialEq)]
pub struct Consumption {
    pub qty_consumed: BigDecimal,
    pub lot_unit_cost_usd: BigDecimal,
    pub lot_block: u64,
    pub proceeds_usd: BigDecimal,
    pub realized_pnl_usd: BigDecimal,
}

/// What a sell produced: the per-lot drawdowns and the totals to fold into stats.
#[derive(Debug, Clone, PartialEq)]
pub struct SellOutcome {
    pub consumptions: Vec<Consumption>,
    pub realized_pnl_usd: BigDecimal,
}

fn zero() -> BigDecimal {
    BigDecimal::from(0)
}

impl Position {
    /// Total tokens currently held.
    pub fn qty_held(&self) -> BigDecimal {
        self.lots.iter().fold(zero(), |acc, l| acc + &l.qty)
    }

    /// Total USD cost basis of the open lots.
    pub fn cost_basis_usd(&self) -> BigDecimal {
        self.lots
            .iter()
            .fold(zero(), |acc, l| acc + &l.qty * &l.price_usd)
    }

    /// Open a lot from a buy of `qty` tokens at `unit_cost_usd` each.
    pub fn buy(&mut self, qty: BigDecimal, unit_cost_usd: BigDecimal, block: u64) {
        self.lots.push(Lot {
            qty,
            price_usd: unit_cost_usd,
            block,
        });
    }

    /// Consume lots FIFO for a sell of `qty` tokens realising `proceeds` USD.
    ///
    /// A sell larger than the held lots (the wallet acquired the token before our
    /// window, or elsewhere) draws the uncovered remainder at **zero cost basis**
    /// — conservative: the proceeds count, but no fabricated loss. That drawdown
    /// is recorded too (block = the sell's block), so a reorg can still reverse it.
    pub fn sell(&mut self, qty: &BigDecimal, proceeds: &BigDecimal, sell_block: u64) -> SellOutcome {
        let mut remaining = qty.clone();
        let mut consumptions = Vec::new();

        while remaining > zero() && !self.lots.is_empty() {
            let front_qty = self.lots[0].qty.clone();
            let take = if front_qty <= remaining {
                front_qty.clone()
            } else {
                remaining.clone()
            };
            let unit = self.lots[0].price_usd.clone();
            let block = self.lots[0].block;

            // Proceeds allocated to this lot in proportion to the qty it covers.
            let proceeds_part = (proceeds.clone() * take.clone()) / qty.clone();
            let cost_part = take.clone() * unit.clone();
            let realized = proceeds_part.clone() - cost_part;

            consumptions.push(Consumption {
                qty_consumed: take.clone(),
                lot_unit_cost_usd: unit,
                lot_block: block,
                proceeds_usd: proceeds_part,
                realized_pnl_usd: realized,
            });

            if front_qty <= remaining {
                self.lots.remove(0);
            } else {
                self.lots[0].qty = front_qty - take.clone();
            }
            remaining -= take;
        }

        // Uncovered tail: sold more than we have a basis for.
        if remaining > zero() {
            let proceeds_part = (proceeds.clone() * remaining.clone()) / qty.clone();
            consumptions.push(Consumption {
                qty_consumed: remaining.clone(),
                lot_unit_cost_usd: zero(),
                lot_block: sell_block,
                proceeds_usd: proceeds_part.clone(),
                realized_pnl_usd: proceeds_part, // full proceeds, zero cost
            });
        }

        let realized_pnl_usd = consumptions
            .iter()
            .fold(zero(), |acc, c| acc + &c.realized_pnl_usd);

        SellOutcome {
            consumptions,
            realized_pnl_usd,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn buy_then_sell_all_at_a_profit() {
        let mut p = Position::default();
        // Buy 2 tokens at $100 each -> cost basis $200.
        p.buy(bd("2"), bd("100"), 10);
        assert_eq!(p.qty_held(), bd("2"));
        assert_eq!(p.cost_basis_usd(), bd("200"));

        // Sell all 2 for $300 -> realised $100, position emptied.
        let out = p.sell(&bd("2"), &bd("300"), 20);
        assert_eq!(out.realized_pnl_usd, bd("100"));
        assert_eq!(out.consumptions.len(), 1);
        assert_eq!(out.consumptions[0].qty_consumed, bd("2"));
        assert_eq!(out.consumptions[0].lot_unit_cost_usd, bd("100"));
        assert!(p.lots.is_empty());
    }

    #[test]
    fn sell_spans_two_lots_oldest_first() {
        let mut p = Position::default();
        p.buy(bd("1"), bd("100"), 10); // oldest
        p.buy(bd("1"), bd("200"), 11); // newer

        // Sell 1.5 for $450 -> $300/token. Consumes lot@100 fully, lot@200 by 0.5.
        let out = p.sell(&bd("1.5"), &bd("450"), 20);
        assert_eq!(out.consumptions.len(), 2);
        // First drawdown: the older $100 lot, full 1 token.
        assert_eq!(out.consumptions[0].lot_unit_cost_usd, bd("100"));
        assert_eq!(out.consumptions[0].qty_consumed, bd("1"));
        assert_eq!(out.consumptions[0].proceeds_usd, bd("300")); // 450 * 1 / 1.5
        assert_eq!(out.consumptions[0].realized_pnl_usd, bd("200")); // 300 - 100
        // Second: the $200 lot, half a token.
        assert_eq!(out.consumptions[1].lot_unit_cost_usd, bd("200"));
        assert_eq!(out.consumptions[1].qty_consumed, bd("0.5"));
        assert_eq!(out.consumptions[1].proceeds_usd, bd("150")); // 450 * 0.5 / 1.5
        assert_eq!(out.consumptions[1].realized_pnl_usd, bd("50")); // 150 - 100
        assert_eq!(out.realized_pnl_usd, bd("250"));
        // Remaining: 0.5 of the newer lot.
        assert_eq!(p.qty_held(), bd("0.5"));
        assert_eq!(p.lots[0].price_usd, bd("200"));
    }

    #[test]
    fn oversell_draws_the_tail_at_zero_cost() {
        let mut p = Position::default();
        p.buy(bd("1"), bd("100"), 10);

        // Sell 2 for $600 -> $300/token. 1 covered (basis $100), 1 uncovered.
        let out = p.sell(&bd("2"), &bd("600"), 20);
        assert_eq!(out.consumptions.len(), 2);
        assert_eq!(out.consumptions[0].realized_pnl_usd, bd("200")); // 300 - 100
        // Uncovered token: zero cost, full proceeds, marked at the sell block.
        assert_eq!(out.consumptions[1].lot_unit_cost_usd, bd("0"));
        assert_eq!(out.consumptions[1].lot_block, 20);
        assert_eq!(out.consumptions[1].realized_pnl_usd, bd("300"));
        assert_eq!(out.realized_pnl_usd, bd("500"));
        assert!(p.lots.is_empty());
    }

    #[test]
    fn a_loss_is_negative() {
        let mut p = Position::default();
        p.buy(bd("1"), bd("300"), 10);
        let out = p.sell(&bd("1"), &bd("100"), 20); // sold below cost
        assert_eq!(out.realized_pnl_usd, bd("-200"));
    }
}
