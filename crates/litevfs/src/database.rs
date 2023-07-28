use crate::{
    locks::{ConnLock, VfsLock},
    pager::{FilesystemPager, PageRef, Pager, ShortReadPager},
};
use sqlite_vfs::OpenAccess;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time,
};

const SQLITE_HEADER_SIZE: usize = 100;

pub(crate) struct DatabaseManager {
    base_path: PathBuf,
    databases: HashMap<String, Arc<RwLock<Database>>>,
}

impl DatabaseManager {
    pub(crate) fn new<P: AsRef<Path>>(base_path: P) -> DatabaseManager {
        DatabaseManager {
            base_path: base_path.as_ref().to_path_buf(),
            // TODO: Populate from LFSC
            databases: HashMap::new(),
        }
    }

    pub(crate) fn get_database(
        &mut self,
        dbname: &str,
        access: OpenAccess,
    ) -> io::Result<Arc<RwLock<Database>>> {
        match (access, self.database_exists(dbname)?) {
            (OpenAccess::CreateNew, true) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "database exists",
            )),
            (OpenAccess::Read | OpenAccess::Write, false) => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "database not found",
            )),
            _ => Ok(()),
        }?;

        let db = Arc::clone(
            self.databases
                .entry(dbname.into())
                .or_insert(Arc::new(RwLock::new(Database::new(
                    dbname,
                    self.base_path.join(dbname),
                )?))),
        );

        Ok(db)
    }

    pub(crate) fn database_exists<S: AsRef<str>>(&self, dbname: S) -> io::Result<bool> {
        // TODO: check LFSC
        Ok(self.databases.contains_key(dbname.as_ref()))
    }
}

pub(crate) struct Database {
    lock: VfsLock,

    name: String,
    path: PathBuf,
    ltx_path: PathBuf,
    page_size: Option<ltx::PageSize>,
    pos: Option<ltx::Pos>,

    pager: ShortReadPager<FilesystemPager>,
    dirty_pages: BTreeMap<ltx::PageNum, Option<ltx::Checksum>>,
}

impl Database {
    fn new(name: &str, path: PathBuf) -> io::Result<Database> {
        let dbpath = path.join("db");
        let ltx_path = path.join("ltx"); // TODO: temporary
        fs::create_dir_all(&ltx_path)?;

        let pos = match fs::read(path.join(".pos")) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
            Ok(data) => Some(
                serde_json::from_slice(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
            ),
        };
        let pager = ShortReadPager::new(FilesystemPager::new(dbpath)?);
        let page_size = match pager.get_page(pos, ltx::PageNum::ONE) {
            Ok(page) => Some(Database::parse_page_size_database(page.as_ref())?),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(err) => return Err(err),
        };

        Ok(Database {
            lock: VfsLock::new(),
            name: name.into(),
            path,
            ltx_path,
            page_size,
            pos,
            pager,
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

    pub(crate) fn journal_path(&self) -> PathBuf {
        self.path.join("journal")
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
        let page1 = match self.pager.get_page(self.pos, ltx::PageNum::ONE) {
            Ok(page1) => page1,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
            Err(err) => return Err(err),
        };
        let num_pages = u32::from_be_bytes(page1.as_ref()[28..32].try_into().unwrap());

        Ok(self.page_size()?.into_inner() as u64 * num_pages as u64)
    }

    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let (number, offset) = if offset as usize <= SQLITE_HEADER_SIZE {
            (ltx::PageNum::ONE, offset as usize)
        } else {
            self.ensure_aligned(buf, offset)?;
            (self.page_num_for(offset)?, 0)
        };

        let page = self.pager.get_page(self.pos, number)?;
        buf.copy_from_slice(&page.as_ref()[offset..offset + buf.len()]);

        Ok(())
    }

    pub(crate) fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.page_size().is_err() && offset == 0 && buf.len() >= SQLITE_HEADER_SIZE {
            self.set_page_size(Database::parse_page_size_database(buf)?);
        }

        self.ensure_aligned(buf, offset)?;
        let page_num = self.page_num_for(offset)?;
        let current_checksum = match self.pager.get_page(self.pos, page_num) {
            Ok(page) => Some(page.checksum()),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(err) => return Err(err),
        };

        let page = PageRef::new(page_num, buf);
        self.pager.put_page(page)?;

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
            .truncate(ltx::PageNum::new((size / page_size) as u32)?)
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

        let page1 = self.pager.get_page(self.pos, ltx::PageNum::ONE)?;
        let commit = ltx::PageNum::new(u32::from_be_bytes(
            page1.as_ref()[28..32].try_into().unwrap(),
        ))?;

        let txid = if let Some(pos) = self.pos {
            pos.txid + 1
        } else {
            ltx::TXID::ONE
        };

        let file = fs::File::create(self.ltx_path.join(format!("{0}-{0}.ltx", txid)))?;
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
        for (&page_num, &prev_checksum) in self.dirty_pages.iter().filter(|&(&n, _)| n <= commit) {
            let page = self.pager.get_page(self.pos, page_num)?;
            if let Some(prev_checksum) = prev_checksum {
                checksum ^= prev_checksum.into_inner();
            };
            checksum ^= page.checksum().into_inner();
            enc.encode_page(page_num, page.as_ref())?;
        }

        let checksum = ltx::Checksum::new(checksum);
        enc.finish(checksum)?;
        file.sync_all()?;

        self.dirty_pages.clear();
        let pos = ltx::Pos {
            txid,
            post_apply_checksum: checksum,
        };

        // TODO: temporary
        fs::write(
            self.path.join(".pos"),
            serde_json::to_vec_pretty(&pos).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        )?;

        self.pos = Some(pos);

        Ok(())
    }
}
