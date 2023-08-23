# LiteVFS - LiteFS VFS implementation for serverless environments (WIP)

## SQLite CLI

To test with SQLite CLI:

1) Build the extension:
```
$ cargo build --release
```

1) Provide LITEFS_CLOUD_TOKEN env variable and Load the extension

```
$ LITEFS_CLOUD_TOKEN=<your token> sqlite3
sqlite> .load target/release/liblitevfs.so
```

1) Open the database
```
sqlite> .open db1
```

That's it. It should work now. The database is stored under `tmp` in a random directory.

To enable debug logging, run `sqlite3` binary like this:

```
$ RUST_LOG=trace sqlite3
```

The following environment variable are handled by LiteVFS:

 - `LITEFS_CLOUD_TOKEN` - LiteFS Cloud token (mandatory)
 - `LITEFS_CLOUD_CLUSTER` - LiteFS Cloud cluster (optional for cluster-scoped tokens, mandatory otherwise)
 - `LITEFS_CLOUD_HOST` - LiteFS Cloud host (optional, defaults to https://litefs.fly.io)
 - `LITEVFS_CACHE_DIR` - cache directory for databases (optional, random directory under `/tmp` if not specified)

The same shared library can be loaded from any language using their SQLite bindings.

## Building LiteVFS for browsers

First, we need to build LiteVFS static library. To do this, make sure you have Emscripten toolchain installed
and activeted.
LiteVFS uses some SQLite APIs which are not available until the final link stage, so we need to tell
Emscripten compiler to ignore undefined symbols for now:

```
$ RUSTFLAGS="-C link-args=-sERROR_ON_UNDEFINED_SYMBOLS=0" cargo build --release --target wasm32-unknown-emscripten
```

The next step is to build SQLite and link it with LiteVFS. This mostly follows the official build process (https://sqlite.org/wasm/doc/trunk/building.md)
except to the last step where we also link with LiteVFS:

1) Download SQLite sources (not amalgamation!) and run the following:

```
$ ./configure --enable-all
$ make sqlite3.c
$ cd ext/wasm
$ make sqlite3_wasm_extra_init.c=<litevfs>/target/wasm32-unknown-emscripten/release/liblitevfs.a emcc.flags="-s 'EXTRA_EXPORTED_RUNTIME_METHODS=['ENV']' -s FETCH" release
```

At this point you should have `jswasm/sqlite3.js` and `jswasm/sqlite3.wasm` files which provide SQLite3 + LiteVFS for browsers.