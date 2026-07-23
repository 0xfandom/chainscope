//! chainscope-core — chain-agnostic domain types, cursor, and traits.
//!
//! This crate deliberately depends on no chain library. That is not a style
//! preference: it is the compile-time proof that the chain boundary is real. If
//! `alloy` ever appears in this crate's dependency list, the boundary has been
//! crossed and a second chain becomes a rewrite instead of a new impl.

pub mod transport;
pub mod types;

pub use transport::{
    build as build_transport, channel, ChannelSink, ChannelSource, Delivery, EventSink,
    EventSource, Receipt, TransportError, TransportKind,
};
pub use types::{Address20, BlockUnit, Hash32, LiqKind, LiqRow, RawLog, RowBatch, SwapRow};
