use crate::{
    database::{Database, DatabaseManager, MAX_MAX_PREFETCH_PAGES},
    leaser::Leaser,
    lfsc,
    locks::{ConnLock, VfsLock},
    pager::{PageSource, Pager},
    syncer::Syncer,
};
use bytesize::ByteSize;
use humantime::{format_duration, parse_duration};
use rand::Rng;
use read_write_at::{ReadAtMut, WriteAtMut};
use sqlite_vfs::{LockKind, OpenAccess, OpenKind, OpenOptions, Vfs};
use std::{
    fs, io,
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    thread, time,
};

const DEFAULT_MAX_REQS_PER_QUERY: usize = 64;
const MAX_MAX_REQS_PER_QUERY: usize = 1024;

/// LiteVfs implements SQLite VFS ops.
pub struct LiteVfs {
    path: PathBuf,
    pager: Arc<Pager>,
    syncer: Arc<Syncer>,
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
            log::error!(
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
                    let (conn_lock, pos) = {
                        let database = database.read().unwrap();

                        (database.conn_lock(), database.pos)
                    };
                    self.syncer.open_conn(dbname, pos);

                    LiteHandle::new(LiteDatabaseHandle::new(
                        Arc::clone(&self.pager),
                        Arc::clone(&self.syncer),
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
            log::error!("[vfs] open: db = {}, opts = {:?}: {}", db, opts, err);
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
                    .get_database(dbname.as_ref(), OpenAccess::Read)?;
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
        let leaser = Leaser::new(Arc::clone(&client), time::Duration::from_secs(1));
        let syncer = Syncer::new(Arc::clone(&client), time::Duration::from_secs(1));

        LiteVfs {
            path: path.as_ref().to_path_buf(),
            pager: Arc::clone(&pager),
            syncer: Arc::clone(&syncer),
            database_manager: Mutex::new(DatabaseManager::new(pager, client, leaser, syncer)),
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
    fn handle_name(&self) -> &str;
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
                log::error!(
                    "[handle] size: type = {}, name = {}: {}",
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
                // SQLite reads past journal file during normal operation.
                // Silence this error.
                if err.kind() == io::ErrorKind::UnexpectedEof
                    && self.inner.handle_type() == "journal"
                    && offset >= self.size()?
                {
                    return Err(err);
                }

                log::error!(
                    "[handle] read_exact_at: type = {}, name = {}, len = {}, offset = {}: {}",
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
                log::error!(
                    "[handle] write_all_at: type = {}, name = {}, len = {}, offset = {}: {}",
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
                log::error!(
                    "[handle] sync: type = {}, name = {}, data_only = {}: {}",
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
                log::error!(
                    "[handle] set_len: type = {}, name = {}, size = {}: {}",
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
                log::error!(
                    "[handle] pragma: pragma = {}, value = {}: {}",
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
    syncer: Arc<Syncer>,
    database: Arc<RwLock<Database>>,
    lock: ConnLock,
    name: String,

    cur_pages_per_query: usize,
    max_pages_per_query: usize,
}

impl LiteDatabaseHandle {
    pub(crate) fn new(
        pager: Arc<Pager>,
        syncer: Arc<Syncer>,
        database: Arc<RwLock<Database>>,
        lock: ConnLock,
    ) -> Self {
        let name = database.read().unwrap().name.clone();
        LiteDatabaseHandle {
            pager,
            syncer,
            database,
            lock,
            name,

            cur_pages_per_query: 0,
            max_pages_per_query: DEFAULT_MAX_REQS_PER_QUERY,
        }
    }

    fn acquire_exclusive(&mut self) -> io::Result<()> {
        if self.lock.state() != LockKind::None {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "connection is already holding a lock",
            ));
        }

        let now = time::Instant::now();
        let timeout = time::Duration::from_secs(1);
        let check_timeout = move || -> io::Result<()> {
            if now.elapsed() > timeout {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "waiting for more than {} to acquire exclusive lock",
                        format_duration(timeout)
                    ),
                ));
            };

            thread::sleep(time::Duration::from_millis(1));

            Ok(())
        };

        loop {
            if self.lock.acquire(LockKind::Shared) {
                if self.lock.acquire(LockKind::Reserved) {
                    // Now that we have a reserved lock there can only be readers.
                    // So loop here until all of them finish.
                    while !self.lock.acquire(LockKind::Exclusive) {
                        check_timeout()?;
                    }

                    return Ok(());
                } else {
                    // Return back to none if we can't progress from shared
                    self.lock.acquire(LockKind::None);
                }
            }

            check_timeout()?;
        }
    }

    fn release_exclusive(&mut self) {
        self.lock.acquire(LockKind::None);
    }

    fn acquire_lease_and_sync(&mut self) -> io::Result<()> {
        self.acquire_exclusive()?;

        {
            let mut db = self.database.write().unwrap();
            if let Err(err) = db.acquire_lease() {
                drop(db);
                self.release_exclusive();
                return Err(err);
            }
            if let Err(err) = db.sync(true) {
                _ = db.release_lease();
                drop(db);
                self.release_exclusive();
                return Err(err);
            }
        };

        self.release_exclusive();

        Ok(())
    }

    fn cache_db(&mut self) -> io::Result<()> {
        self.acquire_exclusive()?;

        let ret = self.database.write().unwrap().cache();

        self.release_exclusive();

        ret
    }
}

impl Drop for LiteDatabaseHandle {
    fn drop(&mut self) {
        self.syncer.close_conn(&self.name)
    }
}

impl DatabaseHandle for LiteDatabaseHandle {
    fn size(&self) -> io::Result<u64> {
        self.database.read().unwrap().size()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let local_only =
            self.max_pages_per_query > 0 && self.cur_pages_per_query >= self.max_pages_per_query;
        if let PageSource::Remote = self
            .database
            .read()
            .unwrap()
            .read_at(buf, offset, local_only)?
        {
            self.cur_pages_per_query += 1;
        }

        Ok(())
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
            if let Err(err) = self.acquire_exclusive() {
                log::warn!(
                    "[database] sync: db = {}, timeout waiting for active connections, skipping sync: {}",
                    self.name, err
                );

                return self.lock.acquire(lock);
            }

            // There are no readers, try and sync. If we fail, let SQLite take the read lock, we may still be
            // able to read the data. The important part here is that `sync()` doesn't fetch any data, so
            // the cache stays consistent.
            if let Err(err) = self.database.write().unwrap().sync(false) {
                log::warn!("[database] sync: db = {}: {}", self.name, err);
            }

            self.release_exclusive();
        }

        if lock == LockKind::None {
            self.cur_pages_per_query = 0
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

            ("litevfs_max_reqs_per_query", None) => {
                Some(Ok(Some(self.max_pages_per_query.to_string())))
            }
            ("litevfs_max_reqs_per_query", Some(val)) => match val.parse::<usize>() {
                Ok(val) if val <= MAX_MAX_REQS_PER_QUERY => {
                    self.max_pages_per_query = val;
                    Some(Ok(None))
                }
                Ok(_) => Some(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("can't be greater than {}", MAX_MAX_REQS_PER_QUERY),
                ))),
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidInput, e))),
            },

            ("litevfs_cache_sync_period", None) => Some(Ok(Some(
                format_duration(self.syncer.sync_period(&self.name)).to_string(),
            ))),
            ("litevfs_cache_sync_period", Some(val)) => {
                let val = if val
                    .chars()
                    .last()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or_default()
                {
                    val.parse()
                        .map(time::Duration::from_secs)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
                } else {
                    parse_duration(val).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
                };

                match val {
                    Ok(val) => {
                        self.syncer.set_sync_period(&self.name, val);
                        Some(Ok(None))
                    }
                    Err(e) => Some(Err(e)),
                }
            }

            ("litevfs_max_prefetch_pages", None) => Some(Ok(Some(
                self.database.read().unwrap().prefetch_limit.to_string(),
            ))),
            ("litevfs_max_prefetch_pages", Some(val)) => match val.parse::<usize>() {
                Ok(val) if val <= MAX_MAX_PREFETCH_PAGES => {
                    self.database.write().unwrap().prefetch_limit = val;
                    Some(Ok(None))
                }
                Ok(_) => Some(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("can't be greater than {}", MAX_MAX_PREFETCH_PAGES),
                ))),
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidInput, e))),
            },

            ("litevfs_acquire_lease", None) => match self.acquire_lease_and_sync() {
                Ok(()) => Some(Ok(None)),
                Err(e) => Some(Err(e)),
            },
            ("litevfs_release_lease", None) => {
                match self.database.read().unwrap().release_lease() {
                    Ok(()) => Some(Ok(None)),
                    Err(e) => Some(Err(e)),
                }
            }

            ("litevfs_cache_db", None) => match self.cache_db() {
                Ok(()) => Some(Ok(None)),
                Err(e) => Some(Err(e)),
            },
            _ => None,
        }
    }

    fn handle_type(&self) -> &'static str {
        "database"
    }
    fn handle_name(&self) -> &str {
        &self.name
    }
}

struct LiteJournalHandle {
    journal: fs::File,
    database: Arc<RwLock<Database>>,
    name: String,
}

impl LiteJournalHandle {
    pub(crate) fn new(database: Arc<RwLock<Database>>) -> io::Result<Self> {
        let (journal, name) = {
            let db = database.read().unwrap();
            let journal = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&db.journal_path)?;
            let name = db.name.clone();
            (journal, name)
        };

        Ok(LiteJournalHandle {
            journal,
            database,
            name,
        })
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
                db.page_size = Some(Database::parse_page_size_journal(buf)?);
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
    fn handle_name(&self) -> &str {
        &self.name
    }
}

struct LiteTempDbHandle {
    name: String,
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

        let name = path.as_ref().to_string_lossy().to_string();
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
    fn handle_name(&self) -> &str {
        &self.name
    }
}
