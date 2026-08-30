//! xtable-server: daemon binary that wires all crates together.

#![recursion_limit = "256"]

pub mod app;
pub mod config;
pub mod red_middleware;
pub mod shutdown;
pub mod structured;