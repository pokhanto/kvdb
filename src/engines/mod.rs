//! This module provides various key value storage engines.

use serde::{Deserialize, Serialize};

use crate::Result;

/// Trait for a key value storage engine.
pub trait KvsEngine: Clone + Send + 'static {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    fn remove(&self, key: String) -> Result<()>;
}

/// Trait for a key value storage engine.
pub trait KvsEngineAsync: Clone + Send + Sync + 'static {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(
        &self,
        key: String,
        value: String,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send;

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(
        &self,
        key: String,
    ) -> impl std::future::Future<Output = Result<Option<String>>> + std::marker::Send;

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    fn remove(
        &self,
        key: String,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send;
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "command_type")]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug, Clone)]
pub struct CommandIndex {
    pos: u64,
    len: u64,
}

mod kvs;
mod kvs_async;

pub use self::kvs::KvStore;
pub use self::kvs_async::KvStoreAsync;
