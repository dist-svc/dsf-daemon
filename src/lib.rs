#![recursion_limit = "512"]
#![allow(dead_code)]
extern crate async_std;

#[macro_use]
extern crate async_trait;
extern crate base64;
extern crate bytes;
extern crate chrono_humanize;
extern crate colored;
#[macro_use]
extern crate bitflags;

extern crate strum;

//#[macro_use]
//extern crate derive_builder;

#[macro_use]
extern crate diesel;

extern crate futures;

#[macro_use]
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate tracing;
extern crate tracing_futures;
extern crate tracing_subscriber;

#[cfg(feature = "profile")]
extern crate flame;

extern crate rand;

extern crate dsf_core;
extern crate dsf_rpc;
extern crate kad;

pub(crate) mod sync {
    pub(crate) type Arc<T> = std::sync::Arc<T>;

    pub(crate) type Mutex<T> = std::sync::Mutex<T>;

    pub(crate) type RwLock<T> = std::sync::RwLock<T>;
}


pub mod core;
pub mod daemon;
pub mod error;
pub mod plugins;
pub mod rpc;

pub mod engine;

pub mod io;

pub mod store;
