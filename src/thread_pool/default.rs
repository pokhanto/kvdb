use std::{
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread,
};

use crate::Result;

use super::ThreadPool;

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

/// Default thread pool
pub struct DefaultThreadPool {
    sender: Sender<ThreadPoolMessage>,
}

impl ThreadPool for DefaultThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = mpsc::channel::<ThreadPoolMessage>();
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..threads {
            let receiver = Arc::clone(&receiver);
            thread::spawn(move || {
                let receiver = receiver.lock().unwrap();
                let message = receiver.recv().unwrap();

                match message {
                    ThreadPoolMessage::RunJob(job) => {
                        job();
                    }
                    ThreadPoolMessage::Shutdown => {}
                }
            });
        }
        Ok(Self { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap();
    }
}

impl Drop for DefaultThreadPool {
    fn drop(&mut self) {
        self.sender.send(ThreadPoolMessage::Shutdown).unwrap();
    }
}
