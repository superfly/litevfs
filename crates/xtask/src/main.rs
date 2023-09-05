mod build_wasm;

use std::{env, error::Error};

type DynError = Box<dyn Error>;

const SQLITE_VERSION: &'static str = "3430000";

pub mod tasks {
    use crate::DynError;

    pub fn build_wasm() -> Result<(), DynError> {
        crate::build_wasm::build_wasm(crate::SQLITE_VERSION)?;

        Ok(())
    }

    pub fn help() {
        println!(
            "
Usage: Run with `cargo xtask <task>, e.g. `cargo xtask build-wasm`.

    Tasks:
        build-wasm: Build WASM distribution of SQLite3 + LiteVFS 
    "
        );
    }
}

fn main() -> Result<(), DynError> {
    let task = env::args().nth(1);
    match task {
        None => tasks::help(),
        Some(t) => match t.as_str() {
            "help" => tasks::help(),
            "build-wasm" => tasks::build_wasm()?,
            invalid => return Err(format!("Invalid task name: {}", invalid).into()),
        },
    };

    Ok(())
}