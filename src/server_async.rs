use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, span, Level};

use crate::{KvsEngineAsync, Request, Response, Result};

/// Server
pub struct KvServerAsync<E: KvsEngineAsync> {
    engine: Arc<E>,
}

impl<E: KvsEngineAsync> KvServerAsync<E> {
    /// Create new server with given address
    pub fn new(engine: E) -> Self {
        Self {
            engine: Arc::new(engine),
        }
    }

    /// start server
    pub async fn start(&mut self, address: &str) -> Result<()> {
        let listener = TcpListener::bind(address).await?;

        loop {
            let (mut socket, _) = listener.accept().await?;
            let engine = Arc::clone(&self.engine);

            tokio::spawn(async move {
                let mut length_buf = [0; 4];

                socket
                    .read_exact(&mut length_buf)
                    .await
                    .expect("failed to read data from socket");

                let msg_length = u32::from_be_bytes(length_buf) as usize;

                let mut msg_buf = vec![0; msg_length];
                socket
                    .read_exact(&mut msg_buf)
                    .await
                    .expect("failed to read data from socket");

                let request: Request = serde_json::from_slice(&msg_buf).unwrap();
                // info!("Received request: {:?}", &request);

                let response = match request {
                    Request::Get { key } => match engine.get(key.to_owned()).await {
                        Ok(key) => Response::Value {
                            value: key.to_owned(),
                        },
                        Err(error) => Response::Error {
                            message: format!("{}", error),
                        },
                    },
                    Request::Set { key, value } => {
                        match engine.set(key.to_owned(), value.to_owned()).await {
                            Ok(..) => Response::Ok,
                            Err(error) => Response::Error {
                                message: format!("{}", error),
                            },
                        }
                    }
                    Request::Remove { key } => match engine.remove(key.to_owned()).await {
                        Ok(..) => Response::Ok,
                        Err(error) => Response::Error {
                            message: format!("{}", error),
                        },
                    },
                };

                let response_data = serde_json::to_vec(&response).unwrap();
                let response_length = (response_data.len() as u32).to_be_bytes();

                socket
                    .write_all(&response_length)
                    .await
                    .expect("failed to write data to socket");
                socket
                    .write_all(&response_data)
                    .await
                    .expect("failed to write data to socket");
            });
        }
    }
}
