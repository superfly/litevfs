use std::{
    collections::{BTreeSet, HashMap},
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use sqlite_vfs::OpenAccess;

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
    _path: PathBuf,
    page_size: Option<ltx::PageSize>,

    // TODO: temporary, for now DB is in-memory only
    pages: Vec<Vec<u8>>,
    dirty_pages: BTreeSet<usize>,
}

impl Database {
    fn new(path: PathBuf) -> Database {
        Database {
            _path: path,
            page_size: None,
            pages: Vec::new(),
            dirty_pages: BTreeSet::new(),
        }
    }

    fn page_size(&self) -> io::Result<usize> {
        if let Some(ps) = self.page_size {
            Ok(ps.into_inner() as usize)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "page size unknown",
            ));
        }
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
        let page_size = self.page_size()?;

        // SQLite always reads exactly one page
        if offset as usize % page_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset not page aligned",
            ));
        }
        if buf.len() % page_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected buffer size",
            ));
        };

        Ok(())
    }

    pub(crate) fn size(&self) -> io::Result<u64> {
        match self.page_size {
            None => Ok(0),
            Some(ps) => Ok(ps.into_inner() as u64 * self.pages.len() as u64),
        }
    }

    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.ensure_aligned(buf, offset)?;
        let page_size = self.page_size()?;

        let page_index = offset as usize / page_size;
        if page_index >= self.pages.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if self.pages[page_index].len() != page_size {
            buf.fill(0);
        } else {
            buf.copy_from_slice(&self.pages[page_index]);
        }

        Ok(())
    }

    pub(crate) fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.page_size.is_none() && offset == 0 && buf.len() > 100 {
            self.set_page_size(buf)?;
        }

        self.ensure_aligned(buf, offset)?;
        let page_size = self.page_size()?;

        let page_index = offset as usize / page_size;
        if page_index >= self.pages.len() {
            self.pages.resize_with(page_index + 1, Default::default);
        }
        if self.pages[page_index].len() != page_size {
            self.pages[page_index].resize(page_size, 0);
        }

        self.pages[page_index].copy_from_slice(buf);

        self.dirty_pages.insert(page_index);

        Ok(())
    }

    pub(crate) fn truncate(&mut self, size: u64) -> io::Result<()> {
        let page_size = self.page_size()?;
        let size = size as usize;
        if size % page_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "size not page aligned",
            ));
        }

        let want_len = size / page_size;
        if self.pages.len() > want_len {
            self.pages.truncate(want_len);
        } else if self.pages.len() < want_len {
            self.pages.resize_with(want_len, Default::default);
        }

        Ok(())
    }
}
