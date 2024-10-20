use std::{env::current_dir, process::exit};

use clap::{crate_authors, crate_description, crate_name, crate_version, Arg, Command};
use kvs::{KvClient, Request};
use tracing::Level;
//use kvs::KvStore;

fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let matches = Command::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .subcommand_required(true)
        .arg_required_else_help(true)
        .disable_help_subcommand(true)
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::new("KEY").help("A string key").required(true))
                .arg(
                    Arg::new("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(Arg::new("KEY").help("A string key").required(true)),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a given key")
                .arg(Arg::new("KEY").help("A string key").required(true)),
        )
        .get_matches();

    let client = KvClient::new("127.0.0.1:4000".into());
    match matches.subcommand() {
        Some(("set", _matches)) => {
            let key = _matches.get_one::<String>("KEY").unwrap();
            let value = _matches.get_one::<String>("VALUE").unwrap();

            client.set(key.to_string(), value.to_string()).unwrap();
        }
        Some(("get", _matches)) => {
            let key = _matches.get_one::<String>("KEY").unwrap();

            match client.get(key.to_string()).unwrap() {
                Some(value) => println!("{value}"),
                None => println!("Noo"),
            }
        }
        Some(("rm", _matches)) => {
            let key = _matches.get_one::<String>("KEY").unwrap();

            client.remove(key.to_string()).unwrap();
        }
        _ => unreachable!(),
    }
}
