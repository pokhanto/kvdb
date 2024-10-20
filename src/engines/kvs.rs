use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::error::{DbError, Result};

use super::KvsEngine;

const COMPACTION_THRESHOLD: u64 = 128 * 128;

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
pub struct KvStore {
    path: PathBuf,
    index: Arc<Mutex<BTreeMap<String, CommandIndex>>>,
    reader: Arc<Mutex<KvReader>>,
    writer: Arc<Mutex<KvWriter>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "command_type")]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let mut writer = self.writer.lock().unwrap();
        let (pos, new_pos) = writer.write(&command)?;
        writer.flush()?;

        if let Command::Set { key, .. } = command {
            let mut index = self.index.lock().unwrap();
            // if command for given key already exists in index
            // add to uncompacted
            if let Some(old_command) = index.insert(
                key,
                CommandIndex {
                    pos,
                    len: new_pos - pos,
                },
            ) {
                writer.add_uncompacted(old_command.len);
            }
        }

        if writer.get_uncompacted() > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.lock().unwrap();
        if let Some(index) = index.get(&key) {
            let mut reader = self.reader.lock().unwrap();
            let command = reader.read(index.pos, index.len)?;

            if let Command::Set { value, .. } = command {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Remove a given key.
    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.index.lock().unwrap();
        if index.contains_key(&key) {
            let command = Command::Remove { key: key.clone() };
            let mut writer = self.writer.lock().unwrap();
            let _ = writer.write(&command)?;

            if let Command::Remove { key } = command {
                let old_command = index.remove(&key).expect("Key not found.");
                writer.add_uncompacted(old_command.len);
            }

            Ok(())
        } else {
            Err(DbError::KeyNotFound)
        }
    }
}

impl KvStore {
    /// Create new instance of KvStore based on file
    pub fn open(path: &Path) -> Result<Self> {
        let mut buf_writer = create_writer(&path.join("data.log"))?;
        let mut buf_reader = BufReaderWithPos::new(File::open(path.join("data.log"))?)?;

        let mut index: BTreeMap<String, CommandIndex> = BTreeMap::default();

        let uncompacted = load(&mut buf_reader, &mut buf_writer, &mut index)?;
        let writer = KvWriter::new(buf_writer, uncompacted);
        let reader = KvReader::new(buf_reader);

        Ok(Self {
            path: path.to_path_buf(),
            index: Arc::new(Mutex::new(index)),
            writer: Arc::new(Mutex::new(writer)),
            reader: Arc::new(Mutex::new(reader)),
        })
    }
    /// compact data
    pub fn compact(&self) -> Result<()> {
        let path = self.path.join("data.log");
        let temp_path = self.path.join("temp.log");
        fs::rename(&path, &temp_path)?;

        let new_buf_writer = create_writer(&path)?;
        let reader = Arc::clone(&self.reader);
        let mut reader = reader.lock().unwrap();
        let index = self.index.lock().unwrap();

        let mut new_writer = KvWriter::new(new_buf_writer, 0);

        for command_index in index.values() {
            let command = reader.read(command_index.pos, command_index.len)?;
            new_writer.write(&command);
        }

        new_writer.flush()?;

        let mut writer = self.writer.lock().unwrap();
        *writer = new_writer;

        fs::remove_file(&temp_path)?;

        Ok(())
    }
}

fn create_writer(path: &PathBuf) -> Result<BufWriterWithPos<File>> {
    let writer = BufWriterWithPos::new(OpenOptions::new().create(true).append(true).open(path)?)?;

    Ok(writer)
}

#[derive(Debug, Clone)]
struct CommandIndex {
    pos: u64,
    len: u64,
}

fn load(
    reader: &mut BufReaderWithPos<File>,
    writer: &mut BufWriterWithPos<File>,
    index: &mut BTreeMap<String, CommandIndex>,
) -> Result<u64> {
    let mut uncompacted = 0;
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_command) = index.insert(
                    key,
                    CommandIndex {
                        pos,
                        len: new_pos - pos,
                    },
                ) {
                    uncompacted += old_command.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_command) = index.remove(&key) {
                    uncompacted += old_command.len;
                }
            }
        }
        pos = new_pos;
    }
    writer.seek(SeekFrom::Start(pos));
    Ok(uncompacted)
}

#[derive(Debug)]
struct KvReader {
    reader: BufReaderWithPos<File>,
}

impl KvReader {
    fn new(reader: BufReaderWithPos<File>) -> Self {
        Self { reader }
    }

    fn read(&mut self, pos: u64, len: u64) -> Result<Command> {
        self.reader.seek(SeekFrom::Start(pos))?;

        let mut buffer = vec![0; len as usize];
        self.reader.read_exact(&mut buffer)?;
        let command: Command = serde_json::from_slice(&buffer)?;

        Ok(command)
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

    fn write(&mut self, command: &Command) -> Result<(u64, u64)> {
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &command)?;
        let new_pos = self.writer.pos;

        Ok((pos, new_pos))
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;

        Ok(())
    }

    fn add_uncompacted(&mut self, uncompacted: u64) {
        self.uncompacted += uncompacted;
    }

    fn get_uncompacted(&self) -> u64 {
        self.uncompacted
    }
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Debug)]
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
