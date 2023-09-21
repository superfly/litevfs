use crate::{
    leaser::Leaser,
    lfsc,
    locks::{ConnLock, VfsLock},
    pager::{PageRef, PageSource, Pager},
    syncer::{Changes, Syncer},
    IterLogger, PosLogger,
};
use litetx as ltx;
use sqlite_vfs::OpenAccess;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, RwLock},
    time,
};

const SQLITE_HEADER_SIZE: u64 = 100;
const SQLITE_WRITE_VERSION_OFFSET: usize = 18;
const SQLITE_READ_VERSION_OFFSET: usize = 19;

pub(crate) struct DatabaseManager {
    pager: Arc<Pager>,
    databases: HashMap<String, Arc<RwLock<Database>>>,
    client: Arc<lfsc::Client>,
    leaser: Arc<Leaser>,
    syncer: Arc<Syncer>,
}

impl DatabaseManager {
    pub(crate) fn new(
        pager: Arc<Pager>,
        client: Arc<lfsc::Client>,
        leaser: Arc<Leaser>,
        syncer: Arc<Syncer>,
    ) -> DatabaseManager {
        DatabaseManager {
            pager,
            databases: HashMap::new(),
            client,
            leaser,
            syncer,
        }
    }

    pub(crate) fn get_database(
        &mut self,
        dbname: &str,
        access: OpenAccess,
    ) -> io::Result<Arc<RwLock<Database>>> {
        if let Some(db) = self.get_database_local(dbname, access)? {
            return Ok(db);
        }

        let db = if let Some(db) = self.get_database_remote(dbname, access)? {
            db
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "database not found",
            ));
        };

        self.databases.insert(dbname.into(), Arc::clone(&db));

        Ok(db)
    }

    fn get_database_local(
        &self,
        dbname: &str,
        access: OpenAccess,
    ) -> io::Result<Option<Arc<RwLock<Database>>>> {
        let db = self.databases.get(dbname);

        if db.is_some() && access == OpenAccess::CreateNew {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "database already exists",
            ));
        }

        Ok(db.map(Arc::clone))
    }

    fn get_database_remote(
        &self,
        dbname: &str,
        access: OpenAccess,
    ) -> io::Result<Option<Arc<RwLock<Database>>>> {
        let pos = self.client.pos_map()?.remove(dbname);

        if pos.is_some() && access == OpenAccess::CreateNew {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "database already exists",
            ));
        };

        if pos.is_none() && matches!(access, OpenAccess::Read | OpenAccess::Write) {
            return Ok(None);
        }

        let pos = pos.flatten();
        log::info!(
            "[manager] get_database_remote: name = {}, access = {:?}, pos = {}",
            dbname,
            access,
            PosLogger(&pos)
        );

        Ok(Some(Arc::new(RwLock::new(Database::new(
            dbname,
            pos,
            Arc::clone(&self.pager),
            Arc::clone(&self.client),
            Arc::clone(&self.leaser),
            Arc::clone(&self.syncer),
        )?))))
    }

    pub(crate) fn database_exists<S: AsRef<str>>(&self, dbname: S) -> io::Result<bool> {
        if self.databases.contains_key(dbname.as_ref())
            || self.client.pos_map()?.contains_key(dbname.as_ref())
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub(crate) struct Database {
    lock: VfsLock,
    pub(crate) name: String,
    client: Arc<lfsc::Client>,
    pager: Arc<Pager>,
    leaser: Arc<Leaser>,
    syncer: Arc<Syncer>,
    ltx_path: PathBuf,
    pub(crate) journal_path: PathBuf,
    pub(crate) page_size: Option<ltx::PageSize>,
    committed_db_size: Option<ltx::PageNum>,
    current_db_size: Option<ltx::PageNum>,
    pub(crate) pos: Option<ltx::Pos>,
    dirty_pages: BTreeMap<ltx::PageNum, Option<ltx::Checksum>>,
    pub(crate) sync_period: time::Duration,
    wal: bool,
}

impl Database {
    fn new(
        name: &str,
        pos: Option<ltx::Pos>,
        pager: Arc<Pager>,
        client: Arc<lfsc::Client>,
        leaser: Arc<Leaser>,
        syncer: Arc<Syncer>,
    ) -> io::Result<Database> {
        let ltx_path = pager.db_path(name).join("ltx");
        let journal_path = pager.db_path(name).join("journal");

        pager.prepare_db(name)?;
        fs::create_dir_all(&ltx_path)?;

        let (wal, page_size, commit) = match pager.get_page(name, pos, ltx::PageNum::ONE) {
            Ok(page) => (
                Database::ensure_supported(name, page.as_ref())?,
                Some(Database::parse_page_size_database(page.as_ref())?),
                Some(Database::parse_commit_database(page.as_ref())?),
            ),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => (false, None, None),
            Err(err) => return Err(err),
        };

        Ok(Database {
            lock: VfsLock::new(),
            name: name.into(),
            client,
            pager,
            leaser,
            syncer,
            ltx_path,
            journal_path,
            page_size,
            committed_db_size: commit,
            current_db_size: commit,
            pos,
            dirty_pages: BTreeMap::new(),
            sync_period: time::Duration::from_secs(1),
            wal,
        })
    }

    fn ensure_supported(name: &str, page1: &[u8]) -> io::Result<bool> {
        let write_version = u8::from_be(page1[SQLITE_WRITE_VERSION_OFFSET]);
        let read_version = u8::from_be(page1[SQLITE_READ_VERSION_OFFSET]);
        let auto_vacuum = u32::from_be_bytes(
            page1[52..56]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );

        let wal = if write_version == 2 || read_version == 2 {
            log::warn!(
                "[database] ensure_supported: db = {}, database in WAL mode",
                name
            );
            true
            // return Err(io::Error::new(
            //     io::ErrorKind::InvalidData,
            //     "WAL is not supported by LiteVFS",
            // ));
        } else {
            false
        };

        if auto_vacuum > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "autovacuum is not supported by LiteVFS",
            ));
        }

        Ok(wal)
    }

    fn parse_page_size_database(page1: &[u8]) -> io::Result<ltx::PageSize> {
        let page_size = match u16::from_be_bytes(
            page1[16..18]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        ) {
            1 => 65536,
            n => n as u32,
        };

        ltx::PageSize::new(page_size).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn parse_commit_database(page1: &[u8]) -> io::Result<ltx::PageNum> {
        let commit = u32::from_be_bytes(
            page1[28..32]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );

        ltx::PageNum::new(commit).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub(crate) fn parse_page_size_journal(hdr: &[u8]) -> io::Result<ltx::PageSize> {
        let page_size = u32::from_be_bytes(
            hdr[24..28]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );

        ltx::PageSize::new(page_size).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub(crate) fn conn_lock(&self) -> ConnLock {
        self.lock.conn_lock()
    }

    pub(crate) fn page_size(&self) -> io::Result<ltx::PageSize> {
        self.page_size
            .ok_or(io::Error::new(io::ErrorKind::Other, "page size unknown"))
    }

    fn ensure_aligned(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let page_size = self.page_size()?.into_inner() as usize;

        // SQLite always writes exactly one page
        if offset as usize % page_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset not page aligned",
            ));
        }
        if buf.len() != page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected buffer size",
            ));
        };

        Ok(())
    }

    fn page_num_for(&self, offset: u64) -> io::Result<ltx::PageNum> {
        let page_size = self.page_size()?;
        Ok(ltx::PageNum::new(
            (offset / page_size.into_inner() as u64 + 1) as u32,
        )?)
    }

    pub(crate) fn size(&self) -> io::Result<u64> {
        let commit = if let Some(commit) = self.current_db_size {
            commit
        } else {
            return Ok(0);
        };

        Ok(self.page_size()?.into_inner() as u64 * commit.into_inner() as u64)
    }

    pub(crate) fn read_at(
        &self,
        buf: &mut [u8],
        offset: u64,
        local_only: bool,
    ) -> io::Result<PageSource> {
        let (number, offset) = if offset <= SQLITE_HEADER_SIZE {
            (ltx::PageNum::ONE, offset)
        } else {
            self.ensure_aligned(buf, offset)?;
            (self.page_num_for(offset)?, 0)
        };

        let source = self
            .pager
            .get_page_slice(&self.name, self.pos, number, buf, offset, local_only)?;

        if offset as usize <= SQLITE_WRITE_VERSION_OFFSET
            && offset as usize + buf.len() >= SQLITE_READ_VERSION_OFFSET
            && self.wal
        {
            buf[SQLITE_WRITE_VERSION_OFFSET - offset as usize] = u8::to_be(1);
            buf[SQLITE_READ_VERSION_OFFSET - offset as usize] = u8::to_be(1);
        }

        Ok(source)
    }

    pub(crate) fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if offset == 0 && buf.len() >= (SQLITE_HEADER_SIZE as usize) {
            if self.page_size().is_err() {
                self.page_size = Some(Database::parse_page_size_database(buf)?);
            }
            self.current_db_size = Some(Database::parse_commit_database(buf)?);
        }

        _ = self.leaser.get_lease(&self.name)?;
        if self.wal {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "writing to DB in WAL mode is unsupported",
            ));
        }

        self.ensure_aligned(buf, offset)?;
        let page_num = self.page_num_for(offset)?;

        let orig_checksum = match self.pager.get_page(&self.name, self.pos, page_num) {
            Ok(page) => Some(page.checksum()),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(err) => return Err(err),
        };

        let page = PageRef::new(page_num, buf);
        self.pager.put_page(&self.name, page)?;

        if page_num == ltx::PageNum::lock_page(self.page_size()?) {
            return Ok(());
        }

        self.dirty_pages
            .entry(page.number())
            .or_insert(orig_checksum);

        Ok(())
    }

    pub(crate) fn truncate(&mut self, size: u64) -> io::Result<()> {
        let page_size = self.page_size()?.into_inner() as usize;
        let size = size as usize;
        if size % page_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "size not page aligned",
            ));
        }

        self.pager
            .truncate(&self.name, ltx::PageNum::new((size / page_size) as u32)?)
    }

    fn is_journal_header_valid(&self) -> io::Result<bool> {
        const VALID_JOURNAL_HDR: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
        let mut hdr: [u8; 8] = [0; 8];

        fs::File::open(&self.journal_path)?.read_exact(&mut hdr)?;

        Ok(hdr == VALID_JOURNAL_HDR)
    }

    pub(crate) fn commit_journal(&mut self) -> io::Result<()> {
        if !self.is_journal_header_valid()? {
            log::info!("[database] rollback: db = {}", self.name);
            self.dirty_pages.clear();
            return Ok(());
        };

        let txid = if let Some(pos) = self.pos {
            pos.txid + 1
        } else {
            ltx::TXID::ONE
        };

        let checksum = match self.commit_journal_inner(txid) {
            Ok(checksum) => checksum,
            Err(err) => {
                // Commit failed, remove the dirty pages so they can
                // be refetched from LFSC
                for &page_num in self.dirty_pages.keys() {
                    self.pager.del_page(&self.name, page_num)?;
                }
                self.dirty_pages.clear();

                return Err(err);
            }
        };

        self.committed_db_size = self.current_db_size;
        self.dirty_pages.clear();
        let pos = ltx::Pos {
            txid,
            post_apply_checksum: checksum,
        };

        self.pos = Some(pos);
        self.syncer.set_pos(&self.name, self.pos);

        Ok(())
    }

    fn commit_journal_inner(&mut self, txid: ltx::TXID) -> io::Result<ltx::Checksum> {
        if self.current_db_size < self.committed_db_size {
            log::warn!(
                "[database] commit_journal: db = {}: VACUUM is not supported by LiteVFS",
                self.name
            );
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "vacuum is not supported by LiteVFS",
            ));
        }

        let commit = self.current_db_size.ok_or(io::Error::new(
            io::ErrorKind::Other,
            "database size unknown",
        ))?;
        let lease = self.leaser.get_lease(&self.name)?;

        let ltx_path = self.ltx_path.join(format!("{0}-{0}.ltx", txid));
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&ltx_path)?;
        let mut enc = ltx::Encoder::new(
            &file,
            &ltx::Header {
                flags: ltx::HeaderFlags::empty(),
                page_size: self.page_size()?,
                commit,
                min_txid: txid,
                max_txid: txid,
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: self.pos.map(|p| p.post_apply_checksum),
            },
        )?;

        let mut checksum = self
            .pos
            .map(|p| p.post_apply_checksum.into_inner())
            .unwrap_or(0);
        let mut pages = Vec::with_capacity(self.dirty_pages.len());
        for (&page_num, &prev_checksum) in self.dirty_pages.iter().filter(|&(&n, _)| n <= commit) {
            let page = self.pager.get_page(&self.name, self.pos, page_num)?;
            if let Some(prev_checksum) = prev_checksum {
                checksum ^= prev_checksum.into_inner();
            };
            checksum ^= page.checksum().into_inner();
            enc.encode_page(page_num, page.as_ref())?;
            pages.push(page_num);
        }

        let checksum = ltx::Checksum::new(checksum);
        enc.finish(checksum)?;

        // rewind the file and send it to LFSC
        file.seek(SeekFrom::Start(0))?;
        self.client
            .write_tx(&self.name, &file, file.metadata()?.len(), &lease)?;
        fs::remove_file(&ltx_path)?;

        Ok(checksum)
    }

    pub(crate) fn needs_sync(&self) -> bool {
        self.syncer.needs_sync(&self.name, self.pos)
    }

    pub(crate) fn sync(&mut self, force: bool) -> io::Result<()> {
        if force {
            self.syncer.sync()?;
        }

        let pos = match self.syncer.get_changes(&self.name, self.pos)? {
            // No changes
            (pos, None) => {
                log::debug!(
                    "[database] sync: db = {}, prev_pos = {}, pos = {}, no changes",
                    self.name,
                    PosLogger(&self.pos),
                    PosLogger(&pos),
                );
                pos
            }

            // All pages have changed, clear the cache completely
            (pos, Some(Changes::All)) => {
                log::debug!(
                    "[database] sync: db = {}, prev_pos = {}, pos = {}, all pages have changed",
                    self.name,
                    PosLogger(&self.pos),
                    PosLogger(&pos)
                );
                if let Err(err) = self.pager.clear(&self.name) {
                    self.syncer.put_changes(&self.name, Changes::All);
                    return Err(err);
                };

                pos
            }

            // Some pages have changed, drop them from the cache
            (pos, Some(Changes::Pages(pgnos))) => {
                log::debug!(
                    "[database] sync: db = {}, prev_pos = {}, pos = {}, pages = {}",
                    self.name,
                    PosLogger(&self.pos),
                    PosLogger(&pos),
                    IterLogger(&pgnos)
                );
                for pgno in &pgnos {
                    if let Err(err) = self.pager.del_page(&self.name, *pgno) {
                        self.syncer.put_changes(&self.name, Changes::Pages(pgnos));
                        return Err(err);
                    }
                }
                pos
            }
        };

        self.pos = pos;

        Ok(())
    }

    pub(crate) fn acquire_lease(&self) -> io::Result<()> {
        self.leaser.acquire_lease(&self.name)
    }

    pub(crate) fn release_lease(&self) -> io::Result<()> {
        self.leaser.release_lease(&self.name)
    }
}
