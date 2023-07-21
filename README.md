# LiteVFS - LiteFS VFS implementation for serverless environments (WIP)

To test with SQLite CLI:

1) Build the extension:
```
$ cargo build --release
```

1) Load the extension

```
$ sqlite3
sqlite> .load target/release/liblitevfs.so
```

1) Open the database
```
sqlite> .open db1
```

1) (TEMPORARY) Switch to in-memory journal
```
sqlite> pragma journal_mode = "memory";
```

That's it. It should work now. The database is stored under `tmp` (`/tmp/db`) as a set of pages + LTX files.

To enable debug logging, run `sqlite3` binary like this:

```
$ RUST_LOG=trace sqlite3
```