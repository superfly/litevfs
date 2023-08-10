use crate::{
    lfsc,
    locks::{ConnLock, VfsLock},
    pager::{PageRef, Pager},
    PosLogger,
};
use sqlite_vfs::OpenAccess;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time,
};

const SQLITE_HEADER_SIZE: u64 = 100;

pub(crate) struct DatabaseManager {
    pager: Arc<Pager>,
    databases: HashMap<String, Arc<RwLock<Database>>>,
    client: Arc<lfsc::Client>,
}

impl DatabaseManager {
    pub(crate) fn new(pager: Arc<Pager>, client: Arc<lfsc::Client>) -> DatabaseManager {
        DatabaseManager {
            pager,
            databases: HashMap::new(),
            client,
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
    name: String,
    client: Arc<lfsc::Client>,
    pager: Arc<Pager>,
    ltx_path: PathBuf,
    journal_path: PathBuf,
    page_size: Option<ltx::PageSize>,
    pos: Option<ltx::Pos>,
    dirty_pages: BTreeMap<ltx::PageNum, Option<ltx::Checksum>>,
}

impl Database {
    fn new(
        name: &str,
        pos: Option<ltx::Pos>,
        pager: Arc<Pager>,
        client: Arc<lfsc::Client>,
    ) -> io::Result<Database> {
        let ltx_path = pager.db_path(name).join("ltx");
        let journal_path = pager.db_path(name).join("journal");

        pager.prepare_db(name)?;
        fs::create_dir_all(&ltx_path)?;

        let page_size = match pager.get_page(name, pos, ltx::PageNum::ONE) {
            Ok(page) => Some(Database::parse_page_size_database(page.as_ref())?),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(err) => return Err(err),
        };

        Ok(Database {
            lock: VfsLock::new(),
            name: name.into(),
            client,
            pager,
            ltx_path,
            journal_path,
            page_size,
            pos,
            dirty_pages: BTreeMap::new(),
        })
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

    pub(crate) fn parse_page_size_journal(hdr: &[u8]) -> io::Result<ltx::PageSize> {
        let page_size = u32::from_be_bytes(
            hdr[24..28]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );

        ltx::PageSize::new(page_size).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub(crate) fn name(&self) -> String {
        self.name.clone()
    }

    pub(crate) fn conn_lock(&self) -> ConnLock {
        self.lock.conn_lock()
    }

    pub(crate) fn journal_path(&self) -> &Path {
        self.journal_path.as_path()
    }

    pub(crate) fn page_size(&self) -> io::Result<ltx::PageSize> {
        self.page_size
            .ok_or(io::Error::new(io::ErrorKind::Other, "page size unknown"))
    }

    pub(crate) fn set_page_size(&mut self, ps: ltx::PageSize) {
        self.page_size = Some(ps)
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
        // TODO: cache commit value?
        let page1 = match self.pager.get_page(&self.name, self.pos, ltx::PageNum::ONE) {
            Ok(page1) => page1,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
            Err(err) => return Err(err),
        };
        let num_pages = u32::from_be_bytes(page1.as_ref()[28..32].try_into().unwrap());

        Ok(self.page_size()?.into_inner() as u64 * num_pages as u64)
    }

    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let (number, offset) = if offset <= SQLITE_HEADER_SIZE {
            (ltx::PageNum::ONE, offset)
        } else {
            self.ensure_aligned(buf, offset)?;
            (self.page_num_for(offset)?, 0)
        };

        self.pager
            .get_page_slice(&self.name, self.pos, number, buf, offset)?;

        Ok(())
    }

    pub(crate) fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.page_size().is_err() && offset == 0 && buf.len() >= (SQLITE_HEADER_SIZE as usize) {
            self.set_page_size(Database::parse_page_size_database(buf)?);
        }

        self.ensure_aligned(buf, offset)?;
        let page_num = self.page_num_for(offset)?;
        let page = PageRef::new(page_num, buf);
        self.pager.put_page(&self.name, page)?;

        if page_num == ltx::PageNum::lock_page(self.page_size()?) {
            return Ok(());
        }

        let current_checksum = match self.pager.get_page(&self.name, self.pos, page_num) {
            Ok(page) => Some(page.checksum()),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(err) => return Err(err),
        };
        self.dirty_pages
            .entry(page.number())
            .or_insert(current_checksum);

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

        fs::File::open(self.journal_path())?.read_exact(&mut hdr)?;

        Ok(hdr == VALID_JOURNAL_HDR)
    }

    pub(crate) fn commit_journal(&mut self) -> io::Result<()> {
        if !self.is_journal_header_valid()? {
            log::info!("[database] rollback: db = {}", self.name());
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

        self.dirty_pages.clear();
        let pos = ltx::Pos {
            txid,
            post_apply_checksum: checksum,
        };

        self.pos = Some(pos);

        Ok(())
    }

    fn commit_journal_inner(&mut self, txid: ltx::TXID) -> io::Result<ltx::Checksum> {
        let page1 = self
            .pager
            .get_page(&self.name, self.pos, ltx::PageNum::ONE)?;
        let commit = ltx::PageNum::new(u32::from_be_bytes(
            page1.as_ref()[28..32].try_into().unwrap(),
        ))?;

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
            .write_tx(&self.name, &file, file.metadata()?.len())?;
        fs::remove_file(&ltx_path)?;

        Ok(checksum)
    }
}
