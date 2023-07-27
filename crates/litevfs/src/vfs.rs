use crate::{
    database::{Database, DatabaseManager},
    locks::ConnLock,
};
use rand::Rng;
use sqlite_vfs::{LockKind, OpenAccess, OpenKind, OpenOptions, Vfs};
use std::{
    fs, io,
    os::unix::prelude::FileExt,
    path::Path,
    sync::{Arc, Mutex, RwLock},
    thread, time,
};

/// LiteVfs implements SQLite VFS ops.
pub struct LiteVfs {
    database_manager: Mutex<DatabaseManager>,
}

impl Vfs for LiteVfs {
    type Handle = LiteHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> io::Result<Self::Handle> {
        log::trace!("[vfs] open: db = {}, opts = {:?}", db, opts);

        if !matches!(opts.kind, OpenKind::MainDb | OpenKind::MainJournal) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported open kind",
            ));
        };

        let (dbname, kind) = self.database_name_kind(db);
        if kind != opts.kind {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported database name",
            ));
        };

        let database = self
            .database_manager
            .lock()
            .unwrap()
            .get_database(dbname.as_ref(), opts.access)?;
        let conn_lock = database.read().unwrap().conn_lock();

        match opts.kind {
            OpenKind::MainDb => Ok(LiteHandle::new(LiteDatabaseHandle::new(
                database, conn_lock,
            ))),
            OpenKind::MainJournal => Ok(LiteHandle::new(LiteJournalHandle::new(database)?)),
            _ => unreachable!(),
        }
    }

    fn delete(&self, db: &str) -> io::Result<()> {
        log::trace!("[vfs] delete: db = {}", db);

        let (dbname, kind) = self.database_name_kind(db);
        match kind {
            OpenKind::MainDb => (),
            OpenKind::MainJournal => {
                let database = self
                    .database_manager
                    .lock()
                    .unwrap()
                    .get_database(dbname.as_ref(), OpenAccess::Write)?;
                database.write().unwrap().commit_journal()?;
                fs::remove_file(database.read().unwrap().journal_path())?;
            }
            _ => (),
        };

        Ok(())
    }

    fn exists(&self, db: &str) -> io::Result<bool> {
        log::trace!("[vfs] exists: db = {}", db);

        let (dbname, kind) = self.database_name_kind(db);
        match kind {
            OpenKind::MainDb => self
                .database_manager
                .lock()
                .unwrap()
                .database_exists(dbname),
            OpenKind::MainJournal => {
                let database = self
                    .database_manager
                    .lock()
                    .unwrap()
                    .get_database(dbname.as_ref(), OpenAccess::Write)?;
                let journal = database.read().unwrap().journal_path();

                Ok(journal.exists())
            }
            _ => Ok(false),
        }
    }

    fn temporary_name(&self) -> String {
        "main.db".into()
    }

    fn random(&self, buffer: &mut [i8]) {
        rand::thread_rng().fill(buffer);
    }

    fn sleep(&self, duration: time::Duration) -> time::Duration {
        log::trace!("[vfs] sleep: duration: {:?}", duration);

        // TODO: This will block JS runtime. Should be call back to JS here???
        let now = time::Instant::now();
        thread::sleep(duration);
        now.elapsed()
    }
}

impl LiteVfs {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        LiteVfs {
            database_manager: Mutex::new(DatabaseManager::new(path)),
        }
    }

    fn database_name_kind<'a>(&self, db: &'a str) -> (&'a str, OpenKind) {
        if let Some(db) = db.strip_suffix("-journal") {
            (db, OpenKind::MainJournal)
        } else if let Some(db) = db.strip_suffix("-wal") {
            (db.trim_end_matches("-wal"), OpenKind::Wal)
        } else {
            (db, OpenKind::MainDb)
        }
    }
}

pub trait DatabaseHandle: Sync {
    fn size(&self) -> io::Result<u64>;
    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn sync(&mut self, _data_only: bool) -> io::Result<()> {
        Ok(())
    }
    fn set_len(&mut self, size: u64) -> io::Result<()>;
    fn lock(&mut self, _lock: LockKind) -> bool {
        unreachable!("should not be called");
    }
    fn reserved(&mut self) -> bool {
        unreachable!("should not be called");
    }
    fn current_lock(&self) -> LockKind {
        unreachable!("should not be called");
    }

    fn pragma(
        &mut self,
        _pragma: &str,
        _val: Option<&str>,
    ) -> Option<Result<Option<String>, io::Error>> {
        None
    }

    fn handle_type(&self) -> &'static str;
    fn handle_name(&self) -> String;
}

pub struct LiteHandle {
    inner: Box<dyn DatabaseHandle>,
}

impl LiteHandle {
    pub(crate) fn new<H>(handler: H) -> LiteHandle
    where
        H: DatabaseHandle + 'static,
    {
        LiteHandle {
            inner: Box::new(handler),
        }
    }
}

impl sqlite_vfs::DatabaseHandle for LiteHandle {
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> io::Result<u64> {
        match self.inner.size() {
            Err(err) => {
                log::warn!(
                    "[handle] size: type = {}, name = {}: {:?}",
                    self.inner.handle_type(),
                    self.inner.handle_name(),
                    err,
                );

                Err(err)
            }
            Ok(val) => Ok(val),
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        match self.inner.read_exact_at(buf, offset) {
            Err(err) => {
                log::warn!(
                    "[handle] read_exact_at: type = {}, name = {}, len = {}, offset = {}: {:?}",
                    self.inner.handle_type(),
                    self.inner.handle_name(),
                    buf.len(),
                    offset,
                    err,
                );

                Err(err)
            }
            _ => Ok(()),
        }
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        match self.inner.write_all_at(buf, offset) {
            Err(err) => {
                log::warn!(
                    "[handle] write_all_at: type = {}, name = {}, len = {}, offset = {}: {:?}",
                    self.inner.handle_type(),
                    self.inner.handle_name(),
                    buf.len(),
                    offset,
                    err,
                );

                Err(err)
            }
            _ => Ok(()),
        }
    }

    fn sync(&mut self, data_only: bool) -> io::Result<()> {
        match self.inner.sync(data_only) {
            Err(err) => {
                log::warn!(
                    "[handle] sync: type = {}, name = {}, data_only = {}: {:?}",
                    self.inner.handle_type(),
                    self.inner.handle_name(),
                    data_only,
                    err,
                );

                Err(err)
            }
            _ => Ok(()),
        }
    }

    fn set_len(&mut self, size: u64) -> io::Result<()> {
        match self.inner.set_len(size) {
            Err(err) => {
                log::warn!(
                    "[handle] set_len: type = {}, name = {}, size = {}: {:?}",
                    self.inner.handle_type(),
                    self.inner.handle_name(),
                    size,
                    err,
                );

                Err(err)
            }
            _ => Ok(()),
        }
    }

    fn lock(&mut self, lock: LockKind) -> io::Result<bool> {
        Ok(self.inner.lock(lock))
    }

    fn reserved(&mut self) -> io::Result<bool> {
        Ok(self.inner.reserved())
    }

    fn current_lock(&self) -> io::Result<LockKind> {
        Ok(self.inner.current_lock())
    }

    fn pragma(
        &mut self,
        pragma: &str,
        val: Option<&str>,
    ) -> Option<Result<Option<String>, io::Error>> {
        match self.inner.pragma(pragma, val) {
            Some(Err(err)) => {
                let val = if let Some(val) = val { val } else { "<none>" };
                log::warn!(
                    "[handle] pragma: pragma = {}, value = {}: {:?}",
                    pragma,
                    val,
                    err
                );

                Some(Err(err))
            }
            x => x,
        }
    }

    fn wal_index(&self, _readonly: bool) -> io::Result<Self::WalIndex> {
        Ok(sqlite_vfs::WalDisabled)
    }
}

struct LiteDatabaseHandle {
    database: Arc<RwLock<Database>>,
    lock: ConnLock,
}

impl LiteDatabaseHandle {
    pub(crate) fn new(database: Arc<RwLock<Database>>, lock: ConnLock) -> Self {
        LiteDatabaseHandle { database, lock }
    }
}

impl DatabaseHandle for LiteDatabaseHandle {
    fn size(&self) -> io::Result<u64> {
        self.database.read().unwrap().size()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.database.read().unwrap().read_at(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.database.write().unwrap().write_at(buf, offset)
    }

    fn set_len(&mut self, size: u64) -> io::Result<()> {
        self.database.write().unwrap().truncate(size)
    }

    fn lock(&mut self, lock: LockKind) -> bool {
        self.lock.acquire(lock)
    }

    fn reserved(&mut self) -> bool {
        self.lock.reserved()
    }

    fn current_lock(&self) -> LockKind {
        self.lock.state()
    }

    fn pragma(
        &mut self,
        pragma: &str,
        val: Option<&str>,
    ) -> Option<Result<Option<String>, io::Error>> {
        match (pragma, val) {
            ("journal_mode", Some(val)) if val.to_uppercase() == "WAL" => {
                Some(Ok(Some("WAL is not supported by LiteVFS".into())))
            }
            _ => None,
        }
    }

    fn handle_type(&self) -> &'static str {
        "database"
    }
    fn handle_name(&self) -> String {
        self.database.read().unwrap().name()
    }
}

struct LiteJournalHandle {
    journal: fs::File,
    database: Arc<RwLock<Database>>,
}

impl LiteJournalHandle {
    pub(crate) fn new(database: Arc<RwLock<Database>>) -> io::Result<Self> {
        let journal = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(database.read().unwrap().journal_path())?;

        Ok(LiteJournalHandle { journal, database })
    }
}

impl DatabaseHandle for LiteJournalHandle {
    fn size(&self) -> io::Result<u64> {
        self.journal.metadata().map(|m| m.len())
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.journal.read_exact_at(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        const JOURNAL_HDR_SIZE: usize = 28;

        {
            let mut db = self.database.write().unwrap();
            if offset == 0 && buf.len() >= JOURNAL_HDR_SIZE && db.page_size().is_err() {
                db.set_page_size(Database::parse_page_size_journal(buf)?);
            };
            if offset == 0 && buf.len() == JOURNAL_HDR_SIZE && buf.iter().all(|&b| b == 0) {
                db.commit_journal()?;
            };
        }

        self.journal.write_all_at(buf, offset)
    }

    fn set_len(&mut self, size: u64) -> io::Result<()> {
        self.database.write().unwrap().commit_journal()?;
        self.journal.set_len(size)
    }

    fn handle_type(&self) -> &'static str {
        "journal"
    }
    fn handle_name(&self) -> String {
        self.database.read().unwrap().name()
    }
}
