//! chainscope indexer, as a library.
//!
//! The pipeline stages live here rather than inside `main.rs` for one concrete
//! reason: the crash-resumability harness in `tests/` has to drive the *real*
//! `Producer` and `Writer` against a synthetic chain, not a reimplementation of
//! them. A behavioural claim tested against a copy of the code proves nothing
//! about the code that ships. The binary is a thin `main` over these modules.

pub mod config;
pub mod consumer;
pub mod db;
pub mod producer;
pub mod supervisor;
pub mod testkit;
pub mod transformer;
