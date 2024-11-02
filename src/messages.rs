use crate::Result;
use serde::{Deserialize, Serialize};
use std::{io::Read, net::TcpStream as StdTcpStream};
use tokio::{io::AsyncReadExt, net::TcpStream};

/// Request enum
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Get variant
    Get {
        /// Get variant key
        key: String,
    },
    /// Set variant
    Set {
        /// Set variant key
        key: String,
        /// Set variant value
        value: String,
    },
    /// Remove variant
    Remove {
        /// Remove variant key
        key: String,
    },
}

impl Request {
    /// Serialize request message
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Request::Set { key, value } => {
                let command_type = 1u8;
                let key_length = key.len() as u16;
                let value_length = value.len() as u16;
                let mut message = Vec::with_capacity(1 + 2 + key.len() + 2 + value.len());
                message.push(command_type);
                message.extend(&key_length.to_be_bytes());
                message.extend(key.as_bytes());
                message.extend(&value_length.to_be_bytes());
                message.extend(value.as_bytes());

                message
            }
            Request::Get { key } => {
                let command_type = 2u8;
                let key_length = key.len() as u16;
                let mut message = Vec::with_capacity(1 + 2 + key.len());
                message.push(command_type);
                message.extend(&key_length.to_be_bytes());
                message.extend(key.as_bytes());

                message
            }
            Request::Remove { key } => {
                let command_type = 3u8;
                let key_length = key.len() as u16;
                let mut message = Vec::with_capacity(1 + 2 + key.len());
                message.push(command_type);
                message.extend(&key_length.to_be_bytes());
                message.extend(key.as_bytes());

                message
            }
        }
    }

    /// Deserialize request message
    pub async fn deserialize(stream: &mut TcpStream) -> Result<Self> {
        let mut command_type = [0u8; 1];
        stream.read_exact(&mut command_type).await?;

        let mut key_length_buf = [0u8; 2];
        stream.read_exact(&mut key_length_buf).await?;
        let key_length = u16::from_be_bytes(key_length_buf) as usize;

        let mut key_buf = vec![0u8; key_length];
        stream.read_exact(&mut key_buf).await?;
        let key = String::from_utf8(key_buf).unwrap();

        match command_type[0] {
            1u8 => {
                let mut value_length_buf = [0u8; 2];
                stream.read_exact(&mut value_length_buf).await?;
                let value_length = u16::from_be_bytes(value_length_buf) as usize;

                let mut value_buf = vec![0u8; value_length];
                stream.read_exact(&mut value_buf).await?;
                let value = String::from_utf8(value_buf).unwrap();
                Ok(Request::Set { key, value })
            }
            2u8 => Ok(Request::Get { key }),
            3u8 => Ok(Request::Get { key }),
            _ => unreachable!(),
        }
    }
}

/// Response enum
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Ok
    Ok,
    /// Ok with value
    Value {
        /// Ok with value value
        value: Option<String>,
    },
    /// Error variant
    Error {
        /// Error variant message
        message: String,
    },
}

impl Response {
    /// Serialize response
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Ok => {
                vec![0u8]
            }
            Self::Value { value: None } => {
                vec![1u8]
            }
            Self::Value { value: Some(value) } => {
                let command_type = 2u8;
                let value_length = value.len() as u16;
                let mut response = Vec::with_capacity(1 + 2 + value.len());
                response.push(command_type);
                response.extend(&value_length.to_be_bytes());
                response.extend(value.as_bytes());

                response
            }
            Self::Error { message } => {
                let command_type = 3u8;
                let message_length = message.len() as u16;
                let mut response = Vec::with_capacity(1 + 2 + message.len());
                response.push(command_type);
                response.extend(&message_length.to_be_bytes());
                response.extend(message.as_bytes());

                response
            }
        }
    }

    /// Deserialize response
    pub fn deserialize(stream: &mut StdTcpStream) -> Result<Self> {
        let mut command_type = [0u8; 1];
        stream.read_exact(&mut command_type)?;
        match command_type[0] {
            0u8 => Ok(Self::Ok),
            1u8 => Ok(Self::Value { value: None }),
            2u8 => {
                let mut value_length_buf = [0u8; 2];
                stream.read_exact(&mut value_length_buf)?;
                let value_length = u16::from_be_bytes(value_length_buf) as usize;

                let mut value_buf = vec![0u8; value_length];
                stream.read_exact(&mut value_buf)?;
                let value = String::from_utf8(value_buf).unwrap();
                Ok(Self::Value { value: Some(value) })
            }
            3u8 => {
                let mut value_length_buf = [0u8; 2];
                stream.read_exact(&mut value_length_buf)?;
                let value_length = u16::from_be_bytes(value_length_buf) as usize;

                let mut value_buf = vec![0u8; value_length];
                stream.read_exact(&mut value_buf)?;
                let value = String::from_utf8(value_buf).unwrap();
                Ok(Self::Error { message: value })
            }
            _ => unreachable!(),
        }
    }
}
