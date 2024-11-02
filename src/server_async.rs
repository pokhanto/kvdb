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
            let _ = socket.set_nodelay(true);

            tokio::spawn(async move {
                let request = Request::deserialize(&mut socket).await.unwrap();

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

                let response = response.serialize();
                socket
                    .write_all(&response)
                    .await
                    .expect("failed to write data to socket");
            });
        }
    }
}
