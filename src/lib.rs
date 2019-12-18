#![recursion_limit="256"]

extern crate futures;
extern crate async_std;
extern crate async_trait;
extern crate bytes;

extern crate colored;

extern crate dsf_core;
extern crate dsf_rpc;

#[macro_use]
extern crate tracing;
extern crate tracing_futures;
extern crate tracing_subscriber;

pub mod daemon;
pub mod error;

mod io;