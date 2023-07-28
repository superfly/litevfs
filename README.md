# LiteVFS - LiteFS VFS implementation for serverless environments (WIP)

## SQLite CLI

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

## Running SQLite tests

To run SQLite tests, `testfixture` (SQLite test drivers) needs to be linked with LiteVFS:

1) Build the extension in linkable mode:
```
$ cargo build --release --features linkable
```

1) Download SQLite sources and build `testfixture`:
```
$ curl -O https://www.sqlite.org/2023/sqlite-src-3420000.zip
$ unzip sqlite-src-3420000.zip
$ cd sqlite-src-3420000
$ ./configure
$ make OPTS="-DSQLITE_EXTRA_INIT=sqlite3_litevfs_init" LIBS="<litevfs>/target/release/liblitevfs.so" testfixture
```

1) Patch test lib to correctly remove database files, as LiteVFS places them under `/tmp` directoty by default:
```
$ patch -p1 < <litevfs>/misc/testrunner.diff
```

1) Run the tests:
```
$ ./testfixture test/all.test
```