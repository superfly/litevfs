use crate::{
    database::{Database, DatabaseManager},
    locks::{ConnLock, VfsLock},
};
use rand::Rng;
use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs};
use std::{
    borrow::Cow,
    io,
    path::Path,
    sync::{Arc, Mutex, RwLock},
    thread, time,
};

/// LiteVfs implements SQLite VFS ops.
pub struct LiteVfs {
    lock: VfsLock,
    database_manager: Mutex<DatabaseManager>,
}

impl Vfs for LiteVfs {
    type Handle = LiteConnection;

    fn open(&self, db: &str, opts: OpenOptions) -> io::Result<Self::Handle> {
        if opts.kind != OpenKind::MainDb {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only main database supported",
            ));
        }

        let database = self
            .database_manager
            .lock()
            .unwrap()
            .get_database(self.database_name(db)?.as_ref(), opts.access)?;

        Ok(LiteConnection::new(database, self.lock.conn_lock()))
    }

    fn delete(&self, _db: &str) -> io::Result<()> {
        // TODO: We don't delete databases for now
        Ok(())
    }

    fn exists(&self, db: &str) -> io::Result<bool> {
        self.database_manager
            .lock()
            .unwrap()
            .database_exists(self.database_name(db)?)
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
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        LiteVfs {
            lock: VfsLock::new(),
            database_manager: Mutex::new(DatabaseManager::new(path)),
        }
    }

    fn database_name<'a>(&self, db: &'a str) -> io::Result<Cow<'a, str>> {
        Ok(Path::new(db)
            .file_name()
            .ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid database name",
            ))?
            .to_string_lossy()) // this is Ok, as LFSC only allows a small subset of chars in DB name
    }
}

pub struct LiteConnection {
    database: Arc<RwLock<Database>>,
    lock: ConnLock,
}

impl LiteConnection {
    pub(crate) fn new(database: Arc<RwLock<Database>>, lock: ConnLock) -> Self {
        LiteConnection { database, lock }
    }
}

impl DatabaseHandle for LiteConnection {
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> io::Result<u64> {
        self.database.read().unwrap().size()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.database.read().unwrap().read_at(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.database.write().unwrap().write_at(buf, offset)
    }

    fn sync(&mut self, _data_only: bool) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> io::Result<()> {
        self.database.write().unwrap().truncate(size)
    }

    fn lock(&mut self, lock: LockKind) -> io::Result<bool> {
        Ok(self.lock.acquire(lock))
    }

    fn reserved(&mut self) -> io::Result<bool> {
        Ok(self.lock.reserved())
    }

    fn current_lock(&self) -> io::Result<LockKind> {
        Ok(self.lock.state())
    }

    fn wal_index(&self, _readonly: bool) -> io::Result<Self::WalIndex> {
        Ok(sqlite_vfs::WalDisabled::default())
    }
}
