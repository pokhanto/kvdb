use std::io::{Read, Write};
use std::net::TcpListener;
use tracing::{error, info};

use crate::engines::KvsEngine;
use crate::thread_pool::{DefaultThreadPool, ThreadPool};
use crate::{Request, Response, Result};

/// Server
pub struct KvServer<E: KvsEngine, P: ThreadPool> {
    thread_pool: P,
    engine: E,
}

impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    /// Create new server with given address
    pub fn new(engine: E, thread_pool: P) -> Self {
        Self {
            engine,
            thread_pool,
        }
    }

    /// start server
    pub fn start(&mut self, address: &str) -> Result<()> {
        println!("Start {}", address);
        let listener = TcpListener::bind(address)?;

        for stream in listener.incoming() {
            let engine = self.engine.clone();
            match stream {
                Ok(mut stream) => {
                    self.thread_pool.spawn(move || {
                        let mut length_buf = [0; 4];
                        stream.read_exact(&mut length_buf).unwrap();
                        let msg_length = u32::from_be_bytes(length_buf) as usize;

                        let mut msg_buf = vec![0; msg_length];
                        stream.read_exact(&mut msg_buf).unwrap();

                        let request: Request = serde_json::from_slice(&msg_buf).unwrap();
                        info!("Received request: {:?}", &request);

                        let response = match request {
                            Request::Get { key } => match engine.get(key.to_owned()) {
                                Ok(key) => Response::Value {
                                    value: key.to_owned(),
                                },
                                Err(error) => Response::Error {
                                    message: format!("{}", error),
                                },
                            },
                            Request::Set { key, value } => {
                                match engine.set(key.to_owned(), value.to_owned()) {
                                    Ok(..) => Response::Ok,
                                    Err(error) => Response::Error {
                                        message: format!("{}", error),
                                    },
                                }
                            }
                            Request::Remove { key } => match engine.remove(key.to_owned()) {
                                Ok(..) => Response::Ok,
                                Err(error) => Response::Error {
                                    message: format!("{}", error),
                                },
                            },
                        };

                        let response_data = serde_json::to_vec(&response).unwrap();
                        let response_length = (response_data.len() as u32).to_be_bytes();

                        stream.write_all(&response_length).unwrap();
                        stream.write_all(&response_data).unwrap();
                    });
                }
                Err(e) => {
                    error!("Connection failed: {}", e);
                }
            }
        }

        Ok(())
    }
}
