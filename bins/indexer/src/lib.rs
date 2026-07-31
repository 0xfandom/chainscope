//! chainscope indexer, as a library.
//!
//! The pipeline stages live here rather than inside `main.rs` for one concrete
//! reason: the crash-resumability harness in `tests/` has to drive the *real*
//! `Producer` and `Writer` against a synthetic chain, not a reimplementation of
//! them. A behavioural claim tested against a copy of the code proves nothing
//! about the code that ships. The binary is a thin `main` over these modules.

pub mod backfill;
pub mod chunker;
pub mod config;
pub mod consumer;
pub mod db;
pub mod finality;
pub mod maintenance;
pub mod pnl;
pub mod producer;
pub mod reorg;
pub mod retention;
pub mod sniffer;
pub mod supervisor;
pub mod testkit;
pub mod transformer;
