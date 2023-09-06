# LiteVFS - LiteFS VFS implementation for serverless environments (WIP)

LiteVFS is a Virtual Filesystem extension for SQLite that uses [LiteFS Cloud][litefs-cloud] as a backing store.

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
 - `LITEVFS_LOG_FILE` - log into the given file instead of stderr

The same shared library can be loaded from any language using their SQLite bindings.

## Building LiteVFS for browsers

The build process uses Emscripten target, thus, Emscripten SDK needs to be installed and configured on the system.
Refer to Emscripted docs on how to do this. Alternatively, `devenv.nix` file in this repo includes all the
required dependencies.

To build simply do:

```sh
$ cargo xtask build-wasm
```

The command will build LiteVFS with Emscripten, download SQLite3 sources, build it with Emscripten and link it with LiteVFS.
At this point you should have `target/sqlite3-wasm/sqlite3.{js,wasm}` files.

Note that since LiteVFS uses synchronous Emscripten's FETCH API, SQLite3 can only be used from a Worker thread, not from the
main browser UI thread.

[litefs-cloud]: https://fly.io/docs/litefs/