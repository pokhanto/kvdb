use std::{env::current_dir, str::FromStr};

use clap::{crate_authors, crate_description, crate_version, Arg, Command};
use kvs::{thread_pool::ThreadPool, KvServer, KvStore};
use tracing::{info, Level};

const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: &str = "kvs";

/// Engine
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum EngineType {
    /// Kvs engine
    Kvs,
    /// Sled engine
    Sled,
}

impl FromStr for EngineType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(EngineType::Kvs),
            "sled" => Ok(EngineType::Sled),
            _ => Err(format!("'{}' is not valid engine type.", s)),
        }
    }
}

fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .disable_help_subcommand(true)
        .arg(
            Arg::new("addr")
                .long("addr")
                .required(false)
                .default_value(DEFAULT_ADDRESS),
        )
        .arg(
            Arg::new("engine")
                .long("engine")
                .required(false)
                .default_value(DEFAULT_ENGINE)
                .value_parser(clap::builder::EnumValueParser::<EngineType>::new()),
        )
        .get_matches();
    info!("Starting kvs-server version {}.", crate_version!());

    let address = matches.get_one::<String>("addr").unwrap();
    info!("Address: {}", address);
    let engine = matches.get_one::<EngineType>("engine").unwrap();
    info!("Engine: {:?}", engine);

    let thread_pool = ThreadPool::new(6).unwrap();

    let mut kv_server = match engine {
        EngineType::Kvs => {
            KvServer::<KvStore>::new(KvStore::open(&current_dir().unwrap()).unwrap(), thread_pool)
        }
        EngineType::Sled => {
            KvServer::<KvStore>::new(KvStore::open(&current_dir().unwrap()).unwrap(), thread_pool)
        }
    };

    kv_server.start(address).unwrap();
}
