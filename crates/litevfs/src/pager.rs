use crate::lfsc;
use ltx::PageChecksum;
use std::{
    ffi, fmt, fs,
    io::{self, Read, Write},
    path::{self, Path},
    sync::Arc,
};

/// [Pager] manages SQLite page data. It uses local filesystem to cache
/// the pages and when the pages are absent in the cache, requests them from LFSC.
pub(crate) struct Pager {
    root: path::PathBuf,
    client: Arc<lfsc::Client>,
}

impl Pager {
    pub(crate) fn new<P: AsRef<Path>>(path: P, client: Arc<lfsc::Client>) -> Pager {
        Pager {
            root: path.as_ref().to_path_buf(),
            client,
        }
    }

    /// Returns a base path for the given `db`.
    pub(crate) fn db_path(&self, db: &str) -> path::PathBuf {
        self.root.join(db)
    }

    /// Prepares all the paths for the given `db`.
    pub(crate) fn prepare_db(&self, db: &str) -> io::Result<()> {
        fs::create_dir_all(self.pages_path(db))?;
        fs::create_dir_all(self.tmp_path(db))?;

        Ok(())
    }

    /// Returns a `db` `page` at the given database `pos`.
    pub(crate) fn get_page(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
    ) -> io::Result<Page> {
        log::debug!(
            "[pager] get_page: db = {}, pos = {}, pgno = {}",
            db,
            PosLogger(&pos),
            pgno,
        );

        // Request the page either from local cache or from LFSC and convert
        // io::ErrorKind::NotFound errors to io::ErrorKind::UnexpectedEof, as
        // this is what local IO will return in case we read past the file.
        let r = match self.get_page_inner(db, pos, pgno) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
            x => x,
        };

        // Log the error, if any
        match r {
            Err(err) => {
                log::warn!(
                    "[pager] get_page: db = {}, pos = {}, pgno = {}: {:?}",
                    db,
                    PosLogger(&pos),
                    pgno,
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    /// Writes page into the local cache. The page is not shipped to LFSC until the
    /// database is committed.
    pub(crate) fn put_page(&self, db: &str, page: PageRef) -> io::Result<()> {
        log::debug!("[pager] put_page: db = {}, pgno = {}", db, page.number());

        match self.put_page_inner(db, page) {
            Err(err) => {
                log::warn!(
                    "[pager] put_page: db = {}, pgno = {}: {:?}",
                    db,
                    page.number(),
                    err,
                );
                Err(err)
            }
            x => x,
        }
    }

    /// Deletes the page from the local cache. It's fine to attempt to delete an non-existing
    /// page.
    pub(crate) fn del_page(&self, db: &str, pgno: ltx::PageNum) -> io::Result<()> {
        log::debug!("[pager] del_page: db = {} , pgno = {}", db, pgno);

        match self.del_page_inner(db, pgno) {
            Err(err) => {
                log::warn!("[pager] del_page: db = {}, pgno = {}: {:?}", db, pgno, err);
                Err(err)
            }
            x => x,
        }
    }

    /// Removes all pages past the provided `pgno`.
    pub(crate) fn truncate(&self, db: &str, pgno: ltx::PageNum) -> io::Result<()> {
        log::debug!("[pager] truncate: db = {}, pgno = {}", db, pgno);

        match self.truncate_inner(db, pgno) {
            Err(err) => {
                log::warn!("[pager] truncate: db = {}, pgno = {}: {:?}", db, pgno, err);
                Err(err)
            }
            x => x,
        }
    }

    fn get_page_inner(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
    ) -> io::Result<Page> {
        match self.get_page_local(db, pos, pgno) {
            Ok(page) => return Ok(page),
            Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
            _ => (),
        };

        self.get_page_remote(db, pos, pgno)
    }

    fn get_page_local(
        &self,
        db: &str,
        _pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
    ) -> io::Result<Page> {
        let mut file = fs::File::open(self.pages_path(db).join(pgno.file_name()))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        Ok(Page::new(pgno, buf))
    }

    fn get_page_remote(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
    ) -> io::Result<Page> {
        let pos = if let Some(pos) = pos {
            pos
        } else {
            return Err(io::ErrorKind::NotFound.into());
        };

        let pages = self.client.get_page(db, pos, pgno)?;

        let mut requested_page: Option<Page> = None;
        for page in pages {
            log::trace!(
                "[pager] get_page_remote: pos = {}, pgno = {}, got = {}",
                pos,
                pgno,
                page.number(),
            );
            let page_ref = PageRef {
                data: page.as_ref(),
                number: page.number(),
            };
            self.del_page(db, page_ref.number())?;
            self.put_page(db, page_ref)?;

            if page.number() == pgno {
                requested_page = Some(Page::new(page.number(), page.into_inner()))
            }
        }

        requested_page.ok_or(io::ErrorKind::NotFound.into())
    }

    fn put_page_inner(&self, db: &str, page: PageRef) -> io::Result<()> {
        let tmp_name = self.tmp_path(db).join(page.number().file_name());
        let final_name = self.pages_path(db).join(page.number().file_name());

        let mut file = fs::File::create(&tmp_name)?;
        file.write_all(page.as_ref())?;
        fs::rename(tmp_name, final_name)
    }

    fn del_page_inner(&self, db: &str, pgno: ltx::PageNum) -> io::Result<()> {
        let name = self.pages_path(db).join(pgno.file_name());
        match fs::remove_file(name) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            x => x,
        }
    }

    fn truncate_inner(&self, db: &str, pgno: ltx::PageNum) -> io::Result<()> {
        let fname: ffi::OsString = pgno.file_name().into();

        for entry in fs::read_dir(&self.pages_path(db))? {
            let entry = entry?;
            if entry.file_name() <= fname {
                continue;
            }

            fs::remove_file(entry.path())?;
        }

        Ok(())
    }

    fn pages_path(&self, db: &str) -> path::PathBuf {
        self.db_path(db).join("pages")
    }

    fn tmp_path(&self, db: &str) -> path::PathBuf {
        self.db_path(db).join("tmp")
    }
}

/// A struct that owns a single database page.
pub(crate) struct Page {
    data: Vec<u8>,
    _number: ltx::PageNum,
    checksum: ltx::Checksum,
}

impl Page {
    /// Return a new [Page] with `number` and the given `data`.
    pub(crate) fn new(number: ltx::PageNum, data: Vec<u8>) -> Page {
        let checksum = data.page_checksum(number);
        Page {
            data,
            _number: number,
            checksum,
        }
    }

    /// Returns `page` number.
    // pub(crate) fn number(&self) -> ltx::PageNum {
    //     self._number
    // }

    /// Returns `page` checksum.
    pub(crate) fn checksum(&self) -> ltx::Checksum {
        self.checksum
    }
}

impl AsRef<[u8]> for Page {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// A struct that borrows a single database page. Cheap to construct and copy.
#[derive(Clone, Copy)]
pub(crate) struct PageRef<'a> {
    data: &'a [u8],
    number: ltx::PageNum,
}

impl<'a> PageRef<'a> {
    /// Return a new [PageRef] with `number` and the given `data`.
    pub(crate) fn new(number: ltx::PageNum, data: &'a [u8]) -> PageRef<'a> {
        PageRef { data, number }
    }

    /// Returns `page` number.
    pub(crate) fn number(&self) -> ltx::PageNum {
        self.number
    }
}

impl<'a> AsRef<[u8]> for PageRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

struct PosLogger<'a>(&'a Option<ltx::Pos>);

impl<'a> fmt::Display for PosLogger<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(pos) = self.0 {
            pos.fmt(f)
        } else {
            write!(f, "<unknown>")
        }
    }
}
