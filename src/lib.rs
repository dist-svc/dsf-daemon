#![recursion_limit = "512"]
#![allow(unused_imports)]
#![allow(dead_code)]

pub(crate) mod sync {
    pub(crate) type Arc<T> = std::sync::Arc<T>;

    pub(crate) type Mutex<T> = std::sync::Mutex<T>;
}

pub mod core;

pub mod error;

pub mod daemon;

pub mod rpc;

pub mod io;

pub mod store;

pub mod engine;

pub mod plugins;
