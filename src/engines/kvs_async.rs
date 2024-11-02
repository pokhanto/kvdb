use std::{
    fs::{self},
    io::{self, SeekFrom},
    os::fd::AsFd,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use crossbeam_skiplist::SkipMap;
use nix::sys::uio::pread;
use pin_project::pin_project;
use serde_json::Deserializer;
use tokio::{
    fs::File,
    io::AsyncWrite,
    sync::{mpsc, oneshot, Mutex},
    task,
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::error::{DbError, Result};

use super::{Command, CommandIndex, KvsEngineAsync};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::open(path);
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Debug, Clone)]
pub struct KvStoreAsync {
    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, CommandIndex>>,
    reader: Arc<KvReader>,
    writer: Arc<Mutex<KvWriter>>,
}

impl KvsEngineAsync for KvStoreAsync {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    async fn set(&self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let mut writer = self.writer.lock().await;
        let (pos, new_pos) = writer.write(&command).await?;
        writer.flush().await?;

        if let Command::Set { key, .. } = command {
            let index = &self.index;
            // if command for given key already exists in index
            // add to uncompacted
            if let Some(old_command) = index.remove(&key) {
                writer.add_uncompacted(old_command.value().len);
            }
            index.insert(
                key,
                CommandIndex {
                    pos,
                    len: new_pos - pos,
                },
            );
        }

        if writer.get_uncompacted() > COMPACTION_THRESHOLD {
            self.compact().await?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    async fn get(&self, key: String) -> Result<Option<String>> {
        let index = &self.index;
        if let Some(index) = index.get(&key) {
            let value = index.value();

            match self.reader.read(value.pos, value.len).await {
                Ok(Command::Set { value, .. }) => {
                    return Ok(Some(value));
                }
                Err(error) => {
                    return Err(error);
                }
                _ => {
                    return Ok(None);
                }
            };
        }

        Ok(None)
    }

    /// Remove a given key.
    async fn remove(&self, key: String) -> Result<()> {
        let index = &self.index;
        if index.contains_key(&key) {
            let command = Command::Remove { key: key.clone() };
            let mut writer = self.writer.lock().await;
            let _ = writer.write(&command).await?;

            if let Command::Remove { key } = command {
                let old_command = index.remove(&key).expect("Key not found.");
                writer.add_uncompacted(old_command.value().len);
            }

            Ok(())
        } else {
            Err(DbError::KeyNotFound)
        }
    }
}

impl KvStoreAsync {
    /// Create new instance of KvStore based on file
    pub async fn open(path: &Path) -> Result<Self> {
        let mut buf_writer = create_writer(&path.join("data.log")).await?;
        let mut buf_reader = BufReader::new(File::open(path.join("data.log")).await?);

        let mut index: SkipMap<String, CommandIndex> = SkipMap::default();

        let uncompacted = load(&mut buf_reader, &mut buf_writer, &mut index).await?;
        let writer = KvWriter::new(buf_writer, uncompacted);

        let file = File::open(path.join("data.log")).await?;
        let reader = KvReader::new(file);

        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            index: Arc::new(index),
            writer: Arc::new(Mutex::new(writer)),
            reader: Arc::new(reader),
        })
    }
    /// compact data
    pub async fn compact(&self) -> Result<()> {
        let path = self.path.join("data.log");
        let temp_path = self.path.join("temp.log");
        fs::rename(&path, &temp_path)?;

        let new_buf_writer = create_writer(&path).await?;
        let index = &self.index;

        let mut new_writer = KvWriter::new(new_buf_writer, 0);

        for command_index in index.iter() {
            let value = command_index.value();
            let command = self.reader.read(value.pos, value.len).await?;
            new_writer.write(&command).await?;
        }

        new_writer.flush().await?;

        let mut writer = self.writer.lock().await;
        *writer = new_writer;

        fs::remove_file(&temp_path)?;

        Ok(())
    }
}

async fn create_writer(path: &PathBuf) -> Result<BufWriterWithPos<File>> {
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?,
    )
    .await?;

    Ok(writer)
}

async fn load(
    reader: &mut BufReader<File>,
    writer: &mut BufWriterWithPos<File>,
    index: &mut SkipMap<String, CommandIndex>,
) -> Result<u64> {
    let mut uncompacted = 0;
    let mut pos = reader.seek(SeekFrom::Start(0)).await?;

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;

    let mut stream = Deserializer::from_slice(&buffer).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_command) = index.remove(&key) {
                    uncompacted += old_command.value().len;
                }
                index.insert(
                    key,
                    CommandIndex {
                        pos,
                        len: new_pos - pos,
                    },
                );
            }
            Command::Remove { key } => {
                if let Some(old_command) = index.remove(&key) {
                    uncompacted += old_command.value().len;
                }
            }
        }
        pos = new_pos;
    }

    writer.seek(SeekFrom::Start(pos)).await?;
    Ok(uncompacted)
}

#[derive(Debug)]
struct KvReader {
    sender: mpsc::Sender<ReadQueueMessage>,
}

#[derive(Debug)]
enum ReadQueueMessage {
    Read {
        pos: u64,
        len: u64,
        response_sender: oneshot::Sender<Command>,
    },
    Shutdown,
}

impl KvReader {
    fn new(file: File) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ReadQueueMessage>(50);
        let file = Arc::new(file);

        tokio::spawn(async move {
            loop {
                let file = Arc::clone(&file);
                let mut messages: Vec<ReadQueueMessage> = Vec::with_capacity(50);
                let _count = receiver.recv_many(&mut messages, 50).await;

                let min_pos = messages
                    .iter()
                    .filter_map(|item| {
                        if let ReadQueueMessage::Read { pos, .. } = item {
                            Some(*pos)
                        } else {
                            None
                        }
                    })
                    .min()
                    .unwrap();
                let max_pos = messages
                    .iter()
                    .filter_map(|item| {
                        if let ReadQueueMessage::Read { pos, len, .. } = item {
                            Some(*pos + *len)
                        } else {
                            None
                        }
                    })
                    .max()
                    .unwrap();
                let len = max_pos - min_pos;
                let buffer = task::spawn_blocking(move || {
                    let mut buffer = vec![0; len as usize];
                    pread(file.as_fd(), &mut buffer, min_pos as i64).unwrap();

                    buffer
                })
                .await
                .unwrap();

                for message in messages {
                    match message {
                        ReadQueueMessage::Read {
                            pos,
                            len,
                            response_sender,
                        } => {
                            let current_pos = pos - min_pos;
                            let command_buffer =
                                &buffer[current_pos as usize..(current_pos + len) as usize];
                            let command: Command = serde_json::from_slice(command_buffer).unwrap();

                            let _ = response_sender.send(command);
                        }
                        ReadQueueMessage::Shutdown => {
                            break;
                        }
                    };
                }
            }
        });
        Self { sender }
    }

    async fn read(&self, pos: u64, len: u64) -> Result<Command> {
        let (response_sender, response_receiver) = oneshot::channel();
        let message = ReadQueueMessage::Read {
            pos,
            len,
            response_sender,
        };
        self.sender.send(message).await.unwrap();

        let command = response_receiver.await.unwrap();

        Ok(command)
    }
}

impl Drop for KvReader {
    fn drop(&mut self) {
        self.sender
            .blocking_send(ReadQueueMessage::Shutdown)
            .unwrap();
    }
}

#[derive(Debug)]
struct KvWriter {
    writer: BufWriterWithPos<File>,
    uncompacted: u64,
}

impl KvWriter {
    fn new(writer: BufWriterWithPos<File>, uncompacted: u64) -> Self {
        Self {
            writer,
            uncompacted,
        }
    }

    async fn write(&mut self, command: &Command) -> Result<(u64, u64)> {
        let pos = self.writer.pos;
        let serialized = serde_json::to_vec(&command)?;
        self.writer.write_all(&serialized).await?;
        self.writer.flush().await?;
        let new_pos = self.writer.pos;

        Ok((pos, new_pos))
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;

        Ok(())
    }

    fn add_uncompacted(&mut self, uncompacted: u64) {
        self.uncompacted += uncompacted;
    }

    fn get_uncompacted(&self) -> u64 {
        self.uncompacted
    }
}

#[pin_project]
#[derive(Debug)]
struct BufWriterWithPos<W: AsyncWrite + AsyncSeek> {
    #[pin]
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: AsyncWrite + AsyncSeek + Unpin> BufWriterWithPos<W> {
    async fn new(mut inner: W) -> Result<Self> {
        let pos = inner.stream_position().await?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: AsyncWrite + AsyncSeek + Unpin> AsyncWrite for BufWriterWithPos<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, io::Error>> {
        let mut this = self.project();

        match Pin::new(&mut this.writer).poll_write(cx, buf) {
            Poll::Ready(Ok(len)) => {
                *this.pos += len as u64;
                Poll::Ready(Ok(len))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let mut this = self.project();

        Pin::new(&mut this.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let mut this = self.project();

        Pin::new(&mut this.writer).poll_shutdown(cx)
    }
}

impl<R: AsyncWrite + AsyncSeek + Unpin> AsyncSeek for BufWriterWithPos<R> {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let mut this = self.project();

        Pin::new(&mut this.writer).start_seek(position)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<u64>> {
        let mut this = self.project();

        match Pin::new(&mut this.writer).poll_complete(cx) {
            Poll::Ready(Ok(pos)) => {
                *this.pos = pos;
                Poll::Ready(Ok(pos))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
