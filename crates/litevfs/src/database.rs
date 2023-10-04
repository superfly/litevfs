use crate::{
    leaser::Leaser,
    lfsc,
    locks::{ConnLock, VfsLock},
    pager::{PageRef, PageSource, Pager},
    sqlite,
    syncer::{Changes, Syncer},
    IterLogger, OptionLogger,
};
use litetx as ltx;
use sqlite_vfs::OpenAccess;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::{self, Read, Seek, SeekFrom},
    ops,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time,
};

const DEFAULT_MAX_PREFETCH_PAGES: usize = 32;
pub(crate) const MAX_MAX_PREFETCH_PAGES: usize = 128;

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
        let db = if let Some(db) = self.get_database_local_in_mem(dbname, access)? {
            db
        } else if let Some(db) = self.get_database_local_on_disk(dbname, access)? {
            db
        } else if let Some(db) = self.get_database_remote(dbname, access)? {
            self.databases.insert(dbname.into(), Arc::clone(&db));
            db
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "database not found",
            ));
        };

        if access != OpenAccess::Read {
            if db.read().unwrap().wal {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "DB in WAL mode can't be opened for RW",
                ));
            }
            if db.read().unwrap().auto_vacuum {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "DB with auto_vacuum can't be opened for RW",
                ));
            }
        }

        Ok(db)
    }

    fn get_database_local_in_mem(
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

    fn get_database_local_on_disk(
        &self,
        dbname: &str,
        access: OpenAccess,
    ) -> io::Result<Option<Arc<RwLock<Database>>>> {
        let pos = self.pager.db_path(dbname).join("pos");
        if !pos.try_exists()? {
            return Ok(None);
        }

        if access == OpenAccess::CreateNew {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "database already exists",
            ));
        }

        let pos = fs::read(pos)?;
        let pos = serde_json::from_slice(&pos)?;
        log::info!(
            "[manager] get_database_local_on_disk: name = {}, access = {:?}, pos = {}",
            dbname,
            access,
            pos
        );

        Ok(Some(Arc::new(RwLock::new(Database::new(
            dbname,
            Some(pos),
            Arc::clone(&self.pager),
            Arc::clone(&self.client),
            Arc::clone(&self.leaser),
            Arc::clone(&self.syncer),
        )?))))
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
            OptionLogger(&pos)
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
    pos_path: PathBuf,
    pub(crate) journal_path: PathBuf,
    pub(crate) page_size: Option<ltx::PageSize>,
    committed_db_size: Mutex<Option<ltx::PageNum>>,
    current_db_size: Option<ltx::PageNum>,
    pub(crate) pos: Option<ltx::Pos>,
    dirty_pages: BTreeMap<ltx::PageNum, Option<ltx::Checksum>>,
    prefetch_pages: Mutex<BTreeSet<ltx::PageNum>>,
    pub(crate) prefetch_limit: usize,
    wal: bool,
    auto_vacuum: bool,
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
        let pos_path = pager.db_path(name).join("pos");
        let journal_path = pager.db_path(name).join("journal");

        pager.prepare_db(name)?;
        fs::create_dir_all(&ltx_path)?;

        let (wal, auto_vacuum, page_size, commit) =
            match pager.get_page(name, pos, ltx::PageNum::ONE, None) {
                Ok(page) => (
                    Database::parse_wal(page.as_ref()),
                    Database::parse_autovacuum(page.as_ref())?,
                    Some(Database::parse_page_size_database(page.as_ref())?),
                    Some(Database::parse_commit_database(
                        page.as_ref(),
                        sqlite::COMMIT_RANGE,
                    )?),
                ),
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    (false, false, None, None)
                }
                Err(err) => return Err(err),
            };

        if wal {
            log::warn!("[database] db = {}, database in WAL mode", name);
        }
        if auto_vacuum {
            log::warn!("[database] db = {}, database with auto vacuum", name);
        }

        Ok(Database {
            lock: VfsLock::new(),
            name: name.into(),
            client,
            pager,
            leaser,
            syncer,
            ltx_path,
            pos_path,
            journal_path,
            page_size,
            committed_db_size: Mutex::new(commit),
            current_db_size: commit,
            pos,
            dirty_pages: BTreeMap::new(),
            prefetch_pages: Mutex::new(BTreeSet::new()),
            prefetch_limit: DEFAULT_MAX_PREFETCH_PAGES,
            wal,
            auto_vacuum,
        })
    }

    fn parse_wal(page1: &[u8]) -> bool {
        let write_version = u8::from_be(page1[sqlite::WRITE_VERSION_OFFSET]);
        let read_version = u8::from_be(page1[sqlite::READ_VERSION_OFFSET]);

        write_version == 2 || read_version == 2
    }

    fn parse_autovacuum(page1: &[u8]) -> io::Result<bool> {
        let auto_vacuum = u32::from_be_bytes(
            page1[52..56]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );

        Ok(auto_vacuum > 0)
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

    fn parse_commit_database(page1: &[u8], loc: ops::Range<usize>) -> io::Result<ltx::PageNum> {
        let commit = u32::from_be_bytes(
            page1[loc]
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
        let (number, page_offset) = if offset <= sqlite::HEADER_SIZE as u64 {
            (ltx::PageNum::ONE, offset)
        } else {
            self.ensure_aligned(buf, offset)?;
            (self.page_num_for(offset)?, 0)
        };

        let source = self.pager.get_page_slice(
            &self.name,
            self.pos,
            number,
            buf,
            page_offset,
            local_only,
            self.prefetch_pages(number),
        )?;

        if self.can_prefetch(buf) {
            let mut prefetch = self.prefetch_pages.lock().unwrap();
            if let Some(candidates) = sqlite::prefetch_candidates(buf, number).map(|t| {
                t.into_iter()
                    .filter(|&pgno| !self.pager.has_page(&self.name, pgno).unwrap_or(false))
                    .collect()
            }) {
                *prefetch = candidates;
            }
        }

        let offset = offset as usize;
        if offset <= sqlite::WRITE_VERSION_OFFSET
            && offset + buf.len() >= sqlite::READ_VERSION_OFFSET
            && self.wal
        {
            buf[sqlite::WRITE_VERSION_OFFSET - offset] = u8::to_be(1);
            buf[sqlite::READ_VERSION_OFFSET - offset] = u8::to_be(1);
        };

        if offset <= sqlite::COMMIT_RANGE.start
            && offset + buf.len() >= sqlite::COMMIT_RANGE.end
            && !self.dirty_pages.contains_key(&ltx::PageNum::ONE)
        {
            *self.committed_db_size.lock().unwrap() = Some(Database::parse_commit_database(
                buf,
                sqlite::COMMIT_RANGE.start - offset..sqlite::COMMIT_RANGE.end - offset,
            )?);
        }

        Ok(source)
    }

    fn can_prefetch(&self, buf: &[u8]) -> bool {
        let page_size = if let Ok(ps) = self.page_size() {
            ps.into_inner() as usize
        } else {
            return false;
        };

        buf.len() == page_size
    }

    pub(crate) fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if offset == 0 && buf.len() >= sqlite::HEADER_SIZE {
            if self.page_size().is_err() {
                self.page_size = Some(Database::parse_page_size_database(buf)?);
            }
            self.current_db_size =
                Some(Database::parse_commit_database(buf, sqlite::COMMIT_RANGE)?);
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

        let orig_checksum = match *self.committed_db_size.lock().unwrap() {
            Some(dbsize) if page_num > dbsize => None,
            _ => match self.pager.get_page(&self.name, self.pos, page_num, None) {
                Ok(page) => Some(page.checksum()),
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
                Err(err) => return Err(err),
            },
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

        log::debug!(
            "[database] commit_journal: db = {}, pos = {}, committed_size = {}, current_size = {}",
            self.name,
            OptionLogger(&self.pos),
            OptionLogger(&self.committed_db_size.lock().unwrap()),
            OptionLogger(&self.current_db_size),
        );

        let pos = match self.commit_journal_inner(txid) {
            Ok(pos) => pos,
            Err(err) => {
                log::error!(
                    "[database] commit_journal: db = {}, pos = {}, committed_size = {}, current_size = {}: {}",
                    self.name,
                    OptionLogger(&self.pos),
                    OptionLogger(&self.committed_db_size.lock().unwrap()),
                    OptionLogger(&self.current_db_size),
                    err,
                );

                // Commit failed, remove the dirty pages so they can
                // be refetched from LFSC
                for &page_num in self.dirty_pages.keys() {
                    self.pager.del_page(&self.name, page_num)?;
                }
                self.dirty_pages.clear();

                return Err(err);
            }
        };

        *self.committed_db_size.lock().unwrap() = self.current_db_size;
        self.dirty_pages.clear();

        self.pos = Some(pos);
        self.syncer.set_pos(&self.name, self.pos);

        Ok(())
    }

    fn commit_journal_inner(&mut self, txid: ltx::TXID) -> io::Result<ltx::Pos> {
        if self.current_db_size < *self.committed_db_size.lock().unwrap() {
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
            let page = self.pager.get_page(&self.name, self.pos, page_num, None)?;
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

        let pos = ltx::Pos {
            txid,
            post_apply_checksum: checksum,
        };

        self.commit_pos(pos)?;

        Ok(pos)
    }

    fn commit_pos(&mut self, pos: ltx::Pos) -> io::Result<()> {
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.pos_path)?;
        serde_json::to_writer(&file, &pos)?;
        file.sync_all()?;

        Ok(())
    }

    pub(crate) fn needs_sync(&self) -> bool {
        self.syncer.needs_sync(&self.name, self.pos)
    }

    pub(crate) fn sync(&mut self, force: bool) -> io::Result<()> {
        if force {
            self.syncer.sync_one(&self.name)?;
        }

        let pos = match self.syncer.get_changes(&self.name, self.pos)? {
            // No changes
            (pos, None) => {
                log::debug!(
                    "[database] sync: db = {}, prev_pos = {}, pos = {}, no changes",
                    self.name,
                    OptionLogger(&self.pos),
                    OptionLogger(&pos),
                );
                pos
            }

            // All pages have changed, clear the cache completely
            (pos, Some(Changes::All)) => {
                log::debug!(
                    "[database] sync: db = {}, prev_pos = {}, pos = {}, all pages have changed",
                    self.name,
                    OptionLogger(&self.pos),
                    OptionLogger(&pos)
                );
                match self.pager.clear(&self.name) {
                    Err(err) => {
                        self.syncer.put_changes(&self.name, Changes::All);
                        return Err(err);
                    }
                    Ok(pgnos) => {
                        *self.prefetch_pages.lock().unwrap() =
                            pgnos.into_iter().take(self.prefetch_limit).collect();
                    }
                };
                self.committed_db_size.lock().unwrap().take();

                pos
            }

            // Some pages have changed, drop them from the cache
            (pos, Some(Changes::Pages(pgnos))) => {
                log::debug!(
                    "[database] sync: db = {}, prev_pos = {}, pos = {}, pages = {}",
                    self.name,
                    OptionLogger(&self.pos),
                    OptionLogger(&pos),
                    IterLogger(&pgnos)
                );

                let mut prefetch = self.prefetch_pages.lock().unwrap();
                prefetch.clear();
                for pgno in &pgnos {
                    match self.pager.del_page(&self.name, *pgno) {
                        Err(err) => {
                            self.syncer.put_changes(&self.name, Changes::Pages(pgnos));
                            return Err(err);
                        }
                        Ok(true) if prefetch.len() < self.prefetch_limit => {
                            prefetch.insert(*pgno);
                        }
                        _ => (),
                    }
                    if *pgno == ltx::PageNum::ONE {
                        self.committed_db_size.lock().unwrap().take();
                    };
                }

                pos
            }
        };

        if let Some(pos) = pos {
            self.commit_pos(pos)?;
        }

        self.pos = pos;

        Ok(())
    }

    pub(crate) fn cache(&mut self) -> io::Result<()> {
        self.sync(true)?;

        // Make sure we have up-to-date view of the DB header
        let mut header = [0; sqlite::HEADER_SIZE];
        self.read_at(&mut header, 0, false)?;

        let dbsize = self
            .committed_db_size
            .lock()
            .unwrap()
            .ok_or(io::Error::new(
                io::ErrorKind::Other,
                "database size unknown",
            ))?;

        log::info!(
            "[database] caching, db = {}, pos = {}, size = {}",
            self.name,
            OptionLogger(&self.pos),
            dbsize
        );
        let mut pgnos = Vec::with_capacity(MAX_MAX_PREFETCH_PAGES);
        for pgno in 1..=dbsize.into_inner() {
            let pgno = ltx::PageNum::new(pgno).unwrap();

            if self.pager.has_page(&self.name, pgno)? {
                continue;
            }

            if pgno == dbsize || pgnos.len() == MAX_MAX_PREFETCH_PAGES {
                self.pager
                    .get_page(&self.name, self.pos, pgno, Some(&pgnos))?;
                pgnos.clear();
            } else {
                pgnos.push(pgno);
            }
        }

        Ok(())
    }

    fn prefetch_pages(&self, pgno: ltx::PageNum) -> Option<Vec<ltx::PageNum>> {
        let prefetch = self.prefetch_pages.lock().unwrap();
        let pgnos = if prefetch.contains(&pgno) {
            Some(prefetch.iter().filter(|&&no| no != pgno).copied().collect())
        } else {
            None
        };

        pgnos
    }

    pub(crate) fn acquire_lease(&self) -> io::Result<()> {
        self.leaser.acquire_lease(&self.name)
    }

    pub(crate) fn release_lease(&self) -> io::Result<()> {
        self.leaser.release_lease(&self.name)
    }
}
