use duct::cmd;
use std::{env, fs};

use crate::DynError;

pub fn build_wasm(version: &str) -> Result<(), DynError> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let sqlite_dir = metadata
        .target_directory
        .join(format!("sqlite-src-{}", version));
    let wasm_dir = metadata.target_directory.join("sqlite3-wasm");
    let zip_name = metadata
        .target_directory
        .join(format!("sqlite-src-{}.zip", version));

    cmd!(
        "cargo",
        "build",
        "--target",
        "wasm32-unknown-emscripten",
        "--package",
        "litevfs",
        "--release"
    )
    .run()?;

    if !zip_name.exists() {
        println!("Downloading SQLite v{}", version);
        cmd!(
            "curl",
            "-L",
            "-o",
            &zip_name,
            format!("https://sqlite.org/2023/sqlite-src-{}.zip", version)
        )
        .run()?;
    }

    if sqlite_dir.exists() {
        fs::remove_dir_all(&sqlite_dir)?;
    }

    cmd!("unzip", "-d", &metadata.target_directory, &zip_name).run()?;

    env::set_current_dir(sqlite_dir)?;

    cmd!("./configure", "--enable-all").run()?;
    cmd!("make", "sqlite3.c").run()?;

    env::set_current_dir("ext/wasm")?;

    cmd!(
        "make",
        format!(
            "sqlite3_wasm_extra_init.c={}/wasm32-unknown-emscripten/release/liblitevfs.a",
            metadata.target_directory
        ),
        "emcc.flags=-s EXTRA_EXPORTED_RUNTIME_METHODS=['ENV'] -s FETCH",
        "release"
    )
    .run()?;

    fs::create_dir_all(&wasm_dir)?;
    fs::copy("jswasm/sqlite3.js", wasm_dir.join("sqlite3.js"))?;
    fs::copy("jswasm/sqlite3.wasm", wasm_dir.join("sqlite3.wasm"))?;

    println!("!!!!!!!!!! DONE !!!!!!!!!!");
    println!("The artifacts are in {}", wasm_dir);

    Ok(())
}
