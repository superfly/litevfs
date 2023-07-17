# LiteVFS - LiteFS VFS implementation for serverless environments (WIP)

To test with SQLite CLI:

1) Build the extension:
```
$ cargo build
```

1) Download SQLite sources:

```
$ curl -O https://www.sqlite.org/2023/sqlite-amalgamation-3420000.zip
$ unzip sqlite-amalgamation-3420000.zip
$ cd sqlite-amalgamation-3420000
```

1) Build CLI with extension linked in:
```
$ gcc -o sqlite shell.c sqlite3.c \
    -DSQLITE_EXTRA_INIT=sqlite3_litevfs_init \
    -L<litevfs directory>/target/debug \
    -llitevfs
```

Now `sqlite` binary is linked with LiteVFS and will use it by default.


To enable trace log run it as:
```
$ RUST_LOG=trace ./sqlite
```
