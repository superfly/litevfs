use crate::{
    database::{Database, DatabaseManager},
    lfsc,
    locks::{ConnLock, VfsLock},
    pager::Pager,
};
use bytesize::ByteSize;
use humantime::{format_duration, parse_duration};
use rand::Rng;
use sqlite_vfs::{LockKind, OpenAccess, OpenKind, OpenOptions, Vfs};
use std::{
    fs, io,
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    thread, time,
};

/// LiteVfs implements SQLite VFS ops.
pub struct LiteVfs {
    path: PathBuf,
    pager: Arc<Pager>,
    database_manager: Mutex<DatabaseManager>,
    temp_counter: AtomicU64,
}

impl Vfs for LiteVfs {
    type Handle = LiteHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> io::Result<Self::Handle> {
        log::debug!("[vfs] open: db = {}, opts = {:?}", db, opts);

        if !matches!(
            opts.kind,
            OpenKind::MainDb | OpenKind::TempDb | OpenKind::MainJournal | OpenKind::TempJournal
        ) {
            log::warn!(
                "[vfs] open: db = {}, opts = {:?}: unsupported open kind",
                db,
                opts
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported open kind",
            ));
        };

        let (dbname, kind) = self.database_name_kind(db);
        if kind != opts.kind && (opts.kind != OpenKind::TempJournal && kind != OpenKind::TempDb) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported database name",
            ));
        };

        let res = match kind {
            OpenKind::MainDb => self
                .database_manager
                .lock()
                .unwrap()
                .get_database(dbname, opts.access)
                .map(|database| {
                    let conn_lock = database.read().unwrap().conn_lock();
                    LiteHandle::new(LiteDatabaseHandle::new(
                        Arc::clone(&self.pager),
                        database,
                        conn_lock,
                    ))
                }),
            OpenKind::TempDb => Ok(LiteHandle::new(LiteTempDbHandle::new(
                self.path.join(db),
                opts.access,
            )?)),

            OpenKind::MainJournal => self
                .database_manager
                .lock()
                .unwrap()
                .get_database(dbname, opts.access)
                .and_then(|database| Ok(LiteHandle::new(LiteJournalHandle::new(database)?))),
            _ => unreachable!(),
        };

        if let Err(ref err) = res {
            log::warn!("[vfs] open: db = {}, opts = {:?}: {:?}", db, opts, err,);
        }

        res
    }

    fn delete(&self, db: &str) -> io::Result<()> {
        log::debug!("[vfs] delete: db = {}", db);

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
                fs::remove_file(&database.read().unwrap().journal_path)?;
            }
            _ => (),
        };

        Ok(())
    }

    fn exists(&self, db: &str) -> io::Result<bool> {
        log::debug!("[vfs] exists: db = {}", db);

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
                let database = database.read().unwrap();

                Ok(database.journal_path.exists())
            }
            _ => Ok(false),
        }
    }

    fn temporary_name(&self) -> String {
        format!(
            "sfvetil-{:x}_{:x}.db",
            process::id(),
            self.temp_counter.fetch_add(1, Ordering::AcqRel)
        )
    }

    fn random(&self, buffer: &mut [i8]) {
        rand::thread_rng().fill(buffer);
    }

    fn sleep(&self, duration: time::Duration) -> time::Duration {
        log::debug!("[vfs] sleep: duration: {:?}", duration);

        // TODO: This will block JS runtime. Should be call back to JS here???
        let now = time::Instant::now();
        thread::sleep(duration);
        now.elapsed()
    }
}

impl LiteVfs {
    pub(crate) fn new<P: AsRef<Path>>(path: P, client: lfsc::Client) -> Self {
        let client = Arc::new(client);
        let pager = Arc::new(Pager::new(&path, Arc::clone(&client)));

        LiteVfs {
            path: path.as_ref().to_path_buf(),
            pager: Arc::clone(&pager),
            database_manager: Mutex::new(DatabaseManager::new(pager, client)),
            temp_counter: AtomicU64::new(0),
        }
    }

    fn database_name_kind<'a>(&self, db: &'a str) -> (&'a str, OpenKind) {
        if let Some(db) = db.strip_suffix("-journal") {
            (db, OpenKind::MainJournal)
        } else if let Some(db) = db.strip_suffix("-wal") {
            (db.trim_end_matches("-wal"), OpenKind::Wal)
        } else if db.starts_with("sfvetil-") {
            (db, OpenKind::TempDb)
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
    pager: Arc<Pager>,
    database: Arc<RwLock<Database>>,
    lock: ConnLock,
}

impl LiteDatabaseHandle {
    pub(crate) fn new(pager: Arc<Pager>, database: Arc<RwLock<Database>>, lock: ConnLock) -> Self {
        LiteDatabaseHandle {
            pager,
            database,
            lock,
        }
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
        // This connection will read data soon, check if we need to sync with LFSC.
        if self.lock.state() == LockKind::None
            && lock == LockKind::Shared
            && self.database.read().unwrap().needs_sync()
        {
            // This is a bit complicated. We need to initiate the sync even for read transactions,
            // so there may be concurrent transactions executing at the time we enter `sync()`.
            // So wait for them to finish first, otherwise they might see inconsistent state.
            let now = time::Instant::now();
            loop {
                // We can't hold the database lock for long, as another connection
                // might be in the middle of RW transaction.
                {
                    let mut db = self.database.write().unwrap();

                    if now.elapsed() > time::Duration::from_millis(500) {
                        log::warn!(
                            "[database] sync: db = {}, timeout waiting for active readers, skipping sync",
                            db.name,
                        );
                        break;
                    }

                    // There are no readers, try and sync. If we fail, let SQLite take the read lock, we may still be
                    // able to read the data. The important part here is that `sync()` doesn't fetch any data, so
                    // the cache stays consistent.
                    if self.lock.readers() == 0 {
                        if let Err(err) = db.sync() {
                            log::warn!("[database] sync: db = {}: {:?}", db.name, err);
                        }
                        break;
                    }

                    log::debug!(
                        "[database] sync: db = {}, waiting for active readers",
                        db.name
                    );
                }

                thread::sleep(time::Duration::from_millis(5));
            }
        }

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
                Some(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL is not supported by LiteVFS",
                )))
            }
            ("litevfs_min_available_space", None) => Some(Ok(Some(
                ByteSize::b(self.pager.min_available_space()).to_string_as(true),
            ))),
            ("litevfs_min_available_space", Some(val)) => match val.parse::<ByteSize>() {
                Ok(val) => {
                    self.pager.set_min_available_space(val.as_u64());
                    Some(Ok(None))
                }
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidInput, e))),
            },
            ("litevfs_max_cached_pages", None) => {
                Some(Ok(Some(self.pager.max_cached_pages().to_string())))
            }
            ("litevfs_max_cached_pages", Some(val)) => match val.parse::<usize>() {
                Ok(val) => {
                    self.pager.set_max_cached_pages(val);
                    Some(Ok(None))
                }
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidInput, e))),
            },
            ("litevfs_cache_sync_period", None) => Some(Ok(Some(
                format_duration(self.database.read().unwrap().sync_period).to_string(),
            ))),
            ("litevfs_cache_sync_period", Some(val)) => match parse_duration(val) {
                Ok(val) => {
                    self.database.write().unwrap().sync_period = val;
                    Some(Ok(None))
                }
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidInput, e))),
            },
            _ => None,
        }
    }

    fn handle_type(&self) -> &'static str {
        "database"
    }
    fn handle_name(&self) -> String {
        self.database.read().unwrap().name.clone()
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
            .open(&database.read().unwrap().journal_path)?;

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
        self.database.read().unwrap().name.clone()
    }
}

struct LiteTempDbHandle {
    name: PathBuf,
    file: fs::File,
    lock: ConnLock,
}

impl LiteTempDbHandle {
    pub(crate) fn new<P: AsRef<Path>>(path: P, access: OpenAccess) -> io::Result<Self> {
        let mut o = fs::OpenOptions::new();
        o.read(true).write(access != OpenAccess::Read);
        match access {
            OpenAccess::Create => {
                o.create(true);
            }
            OpenAccess::CreateNew => {
                o.create_new(true);
            }
            _ => (),
        };

        let name = path.as_ref().to_path_buf();
        let file = o.open(path)?;
        let vfs_lock = VfsLock::new();
        let lock = vfs_lock.conn_lock();
        Ok(LiteTempDbHandle { name, file, lock })
    }
}

impl DatabaseHandle for LiteTempDbHandle {
    fn size(&self) -> io::Result<u64> {
        self.file.metadata().map(|m| m.len())
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(buf, offset)
    }

    fn set_len(&mut self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
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

    fn handle_type(&self) -> &'static str {
        "tempdb"
    }
    fn handle_name(&self) -> String {
        self.name.to_string_lossy().to_string()
    }
}
