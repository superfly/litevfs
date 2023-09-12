mod build_npm;
mod build_wasm;

use clap::{Arg, Command};
use std::{error::Error, path::PathBuf};

type DynError = Box<dyn Error>;

const DEFAULT_SQLITE_VERSION: &str = "3430000";

fn main() -> Result<(), DynError> {
    let matches = Command::new("xtask")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("build-wasm")
                .about("Build SQLite3 + LiteVFS WASM distribution")
                .arg(
                    Arg::new("version")
                        .short('v')
                        .long("version")
                        .default_value(DEFAULT_SQLITE_VERSION)
                        .help("SQLite3 version"),
                ),
        )
        .subcommand(Command::new("build-npm-meta").about("Build LiteVFS NPM meta package"))
        .subcommand(
            Command::new("build-npm-binary")
                .about("Build LiteVFS binary NPM package")
                .arg(
                    Arg::new("lib")
                        .short('l')
                        .long("lib")
                        .required(true)
                        .value_parser(clap::builder::ValueParser::path_buf())
                        .help("Path to LiteVFS shared library"),
                )
                .arg(
                    Arg::new("cpu")
                        .short('c')
                        .long("cpu")
                        .required(true)
                        .help("CPU architecture"),
                )
                .arg(
                    Arg::new("os")
                        .short('o')
                        .long("os")
                        .required(true)
                        .help("Target OS"),
                )
                .arg(Arg::new("abi").short('a').long("abi").help("System ABI")),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("build-wasm", sub_matches)) => crate::build_wasm::build_wasm(
            sub_matches
                .get_one::<String>("version")
                .expect("`version` is required"),
        )?,
        Some(("build-npm-meta", _)) => crate::build_npm::build_npm_meta()?,
        Some(("build-npm-binary", sub_matches)) => {
            crate::build_npm::build_npm_binary(
                sub_matches.get_one::<PathBuf>("lib").cloned().unwrap(),
                sub_matches.get_one::<String>("cpu").cloned().unwrap(),
                sub_matches.get_one::<String>("os").cloned().unwrap(),
                sub_matches.get_one::<String>("abi").cloned(),
            )?;
        }
        _ => unreachable!(""),
    };

    Ok(())
}
