use ltx::PageChecksum;
use sqlite_vfs::OpenAccess;
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

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
                    self.base_path.join(dbname),
                )))),
        );

        Ok(db)
    }

    pub(crate) fn database_exists<S: AsRef<str>>(&self, dbname: S) -> io::Result<bool> {
        // TODO: check LFSC
        Ok(self.databases.contains_key(dbname.as_ref()))
    }
}

pub(crate) struct Database {
    path: PathBuf,
    page_size: Option<ltx::PageSize>,

    pages: BTreeMap<ltx::PageNum, Page>,
    dirty_pages: BTreeMap<ltx::PageNum, DirtyPage>,
}

impl Database {
    fn new(path: PathBuf) -> Database {
        Database {
            path,
            page_size: None,
            pages: BTreeMap::new(),
            dirty_pages: BTreeMap::new(),
        }
    }

    pub(crate) fn name(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    fn page_size(&self) -> io::Result<ltx::PageSize> {
        self.page_size
            .ok_or(io::Error::new(io::ErrorKind::Other, "page size unknown"))
    }

    fn set_page_size(&mut self, page0: &[u8]) -> io::Result<()> {
        let page_size = match u16::from_be_bytes(page0[16..18].try_into().unwrap()) {
            1 => 65536,
            n => n as u32,
        };

        self.page_size = Some(
            ltx::PageSize::new(page_size)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );

        Ok(())
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
        if buf.len() > page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected buffer size",
            ));
        };

        Ok(())
    }

    fn ensure_single_page(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let page_size = self.page_size()?.into_inner() as usize;
        let offset = offset as usize % page_size;

        if offset + buf.len() > page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read requests to multiple pages",
            ));
        }

        Ok(())
    }

    fn page_num_for(&self, offset: u64) -> io::Result<(ltx::PageNum, usize)> {
        let page_size = self.page_size()?;
        Ok((
            ltx::PageNum::new((offset / page_size.into_inner() as u64 + 1) as u32)?,
            offset as usize % page_size.into_inner() as usize,
        ))
    }

    fn get_page(&self, page_num: ltx::PageNum) -> io::Result<&Page> {
        // TODO: fetch from local cache or LFSC
        self.pages
            .get(&page_num)
            .ok_or(io::ErrorKind::UnexpectedEof.into())
    }

    fn get_page_mut(&mut self, page_num: ltx::PageNum) -> io::Result<Page> {
        // TODO: fetch from local cache or LFSC
        let page_size = self.page_size()?.into_inner() as usize;

        Ok(self.pages.remove(&page_num).unwrap_or_else(|| {
            let buf = vec![0; page_size];
            let checksum = buf.page_checksum(page_num);

            Page { buf, checksum }
        }))
    }

    fn put_page(&mut self, page_num: ltx::PageNum, buf: &[u8]) -> io::Result<()> {
        let mut page = self.get_page_mut(page_num)?;

        let original_checksum = if let Some(dirty_page) = self.dirty_pages.get(&page_num) {
            dirty_page.checksum
        } else {
            Some(page.checksum)
        };

        page.buf.copy_from_slice(buf);
        page.checksum = page.buf.page_checksum(page_num);

        self.pages.insert(page_num, page);
        self.dirty_pages.insert(
            page_num,
            DirtyPage {
                checksum: original_checksum,
            },
        );

        Ok(())
    }

    pub(crate) fn size(&self) -> io::Result<u64> {
        match self.page_size {
            None => Ok(0),
            Some(ps) => Ok(ps.into_inner() as u64 * self.pages.len() as u64),
        }
    }

    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        if self.page_size.is_none() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        self.ensure_single_page(buf, offset)?;
        let (page_num, offset) = self.page_num_for(offset)?;

        let page = self.get_page(page_num)?;

        buf.copy_from_slice(&page.buf[offset..offset + buf.len()]);

        Ok(())
    }

    pub(crate) fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.page_size.is_none() && offset == 0 && buf.len() > 100 {
            self.set_page_size(buf)?;
        }

        self.ensure_aligned(buf, offset)?;
        let (page_num, _) = self.page_num_for(offset)?;

        self.put_page(page_num, buf)?;

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
        if let Some(split_off_point) = ltx::PageNum::new((size / page_size) as u32)? + 1 {
            self.pages.split_off(&split_off_point);
            self.dirty_pages.split_off(&split_off_point);
        }

        Ok(())
    }
}

struct Page {
    buf: Vec<u8>,
    checksum: ltx::Checksum,
}

struct DirtyPage {
    checksum: Option<ltx::Checksum>,
}
