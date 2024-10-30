#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvClient;
pub use client_async::KvClientAsync;
pub use engines::{KvStore, KvStoreAsync, KvsEngine, KvsEngineAsync};
pub use error::Result;
pub use messages::{Request, Response};
pub use server::KvServer;
pub use server_async::KvServerAsync;
pub use thread_pool::ThreadPool;

mod client;
mod client_async;
mod engines;
mod error;
mod messages;
mod server;
mod server_async;
/// Thread pool
pub mod thread_pool;
