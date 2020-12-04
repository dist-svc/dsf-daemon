#![recursion_limit = "512"]

#[macro_use]
extern crate diesel;

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
