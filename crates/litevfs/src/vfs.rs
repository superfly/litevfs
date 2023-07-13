use crate::{connection::LiteConnection, locks::VfsLock};
use rand::Rng;
use sqlite_vfs::{OpenKind, OpenOptions, Vfs};
use std::{io, thread, time};

/// LiteVfs implements SQLite VFS ops.
pub struct LiteVfs {
    lock: VfsLock,
}

impl Vfs for LiteVfs {
    type Handle = LiteConnection;

    fn open(&self, _db: &str, opts: OpenOptions) -> io::Result<Self::Handle> {
        if opts.kind != OpenKind::MainDb {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only main database supported",
            ));
        }

        Ok(LiteConnection::new(self.lock.conn_lock()))
    }

    fn delete(&self, _db: &str) -> io::Result<()> {
        Ok(())
    }

    fn exists(&self, _db: &str) -> io::Result<bool> {
        // TODO: Check page cache and LFSC
        Ok(false)
    }

    fn temporary_name(&self) -> String {
        "main.db".into()
    }

    fn random(&self, buffer: &mut [i8]) {
        rand::thread_rng().fill(buffer);
    }

    fn sleep(&self, duration: time::Duration) -> time::Duration {
        // TODO: This will block JS runtime. Should be call back to JS here???
        let now = time::Instant::now();
        thread::sleep(duration);
        now.elapsed()
    }
}

impl LiteVfs {
    pub fn new() -> Self {
        LiteVfs {
            lock: VfsLock::new(),
        }
    }
}
