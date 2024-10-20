#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvClient;
pub use engines::{KvStore, KvsEngine};
pub use error::Result;
pub use messages::{Request, Response};
pub use server::KvServer;
pub use thread_pool::NaiveThreadPool;

mod client;
mod engines;
mod error;
mod messages;
mod server;
pub mod thread_pool;
