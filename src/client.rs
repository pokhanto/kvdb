use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use tracing::info;

use crate::error::DbError;
use crate::messages::{Request, Response};
use crate::Result;

/// Server
pub struct KvClient {
    address: String,
}

impl KvClient {
    /// Create new server with given address
    pub fn new(address: String) -> Self {
        Self { address }
    }

    /// Get
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let request = Request::Get {
            key: key.to_string(),
        };

        match self.send(&request)? {
            Response::Value { value } => Ok(value),
            Response::Error { message } => Err(DbError::String(message)),
            _ => Err(DbError::Unknown),
        }
    }

    /// Set
    pub fn set(&self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };

        match self.send(&request)? {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(DbError::String(message)),
            _ => Err(DbError::Unknown),
        }
    }

    /// Remove
    pub fn remove(&self, key: String) -> Result<()> {
        let request = Request::Remove { key };

        match self.send(&request).unwrap() {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(DbError::String(message)),
            _ => Err(DbError::Unknown),
        }
    }

    /// Send request
    fn send(&self, request: &Request) -> Result<Response> {
        //info!("Sending {:?} request to {}", &request, &self.address);
        let mut stream = TcpStream::connect(&self.address)?;
        let _ = stream.set_nodelay(true);

        let message = request.serialize();
        stream.write_all(&message)?;

        let response = Response::deserialize(&mut stream)?;

        Ok(response)
    }
}
