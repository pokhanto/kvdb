use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

use crate::error::DbError;
use crate::messages::{Request, Response};
use crate::Result;

/// Asynchronous KvClient
pub struct KvClientAsync {
    address: String,
}

impl KvClientAsync {
    /// Create new client with the given address
    pub fn new(address: String) -> Self {
        Self { address }
    }

    /// Asynchronous Get
    pub async fn get(&self, key: String) -> Result<Option<String>> {
        let request = Request::Get {
            key: key.to_string(),
        };

        match self.send(&request).await? {
            Response::Value { value } => Ok(value),
            Response::Error { message } => Err(DbError::String(message)),
            _ => Err(DbError::Unknown),
        }
    }

    /// Asynchronous Set
    pub async fn set(&self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };

        match self.send(&request).await? {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(DbError::String(message)),
            _ => Err(DbError::Unknown),
        }
    }

    /// Asynchronous Remove
    pub async fn remove(&self, key: String) -> Result<()> {
        let request = Request::Remove { key };

        match self.send(&request).await? {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(DbError::String(message)),
            _ => Err(DbError::Unknown),
        }
    }

    /// Asynchronous Send request
    async fn send(&self, request: &Request) -> std::io::Result<Response> {
        info!("Sending {:?} request to {}", &request, &self.address);
        let mut stream = TcpStream::connect(&self.address).await?;

        let request_data = serde_json::to_vec(&request).unwrap();
        let request_length = (request_data.len() as u32).to_be_bytes();

        stream.write_all(&request_length).await?;
        stream.write_all(&request_data).await?;

        let mut length_buf = [0; 4];
        stream.read_exact(&mut length_buf).await?;
        let response_length = u32::from_be_bytes(length_buf) as usize;

        let mut response_buf = vec![0; response_length];
        stream.read_exact(&mut response_buf).await?;

        let response: Response = serde_json::from_slice(&response_buf).unwrap();
        info!("Received response: {:?}", response);

        Ok(response)
    }
}