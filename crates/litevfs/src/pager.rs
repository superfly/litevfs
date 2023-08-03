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
    db: String,
    root: path::PathBuf,
    client: Arc<lfsc::Client>,
}

impl Pager {
    pub(crate) fn new<P: AsRef<Path>>(
        db: &str,
        path: P,
        client: Arc<lfsc::Client>,
    ) -> io::Result<Pager> {
        fs::create_dir_all(path.as_ref())?;

        Ok(Pager {
            db: db.into(),
            root: path.as_ref().to_path_buf(),
            client,
        })
    }

    /// Returns a database `page` at the given database `pos`.
    pub(crate) fn get_page(&self, pos: Option<ltx::Pos>, pgno: ltx::PageNum) -> io::Result<Page> {
        log::debug!(
            "[pager] get_page: pos = {}, pgno = {}",
            PosLogger(&pos),
            pgno,
        );

        // Request the page either from local cache or from LFSC and convert
        // io::ErrorKind::NotFound errors to io::ErrorKind::UnexpectedEof, as
        // this is what local IO will return in case we read past the file.
        let r = match self.get_page_inner(pos, pgno) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
            x => x,
        };

        // Log the error, if any
        match r {
            Err(err) => {
                log::warn!(
                    "[pager] get_page: pos = {}, pgno = {}: {:?}",
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
    pub(crate) fn put_page(&self, page: PageRef) -> io::Result<()> {
        log::debug!("[pager] put_page: pgno = {}", page.number());

        match self.put_page_inner(page) {
            Err(err) => {
                log::warn!("[pager] put_page: pgno = {}: {:?}", page.number(), err,);
                Err(err)
            }
            x => x,
        }
    }

    /// Deletes the page from the local cache. It's fine to attempt to delete an non-existing
    /// page.
    pub(crate) fn del_page(&self, pgno: ltx::PageNum) -> io::Result<()> {
        log::debug!("[pager] del_page: pgno = {}", pgno);

        match self.del_page_inner(pgno) {
            Err(err) => {
                log::warn!("[pager] del_page: pgno = {}: {:?}", pgno, err);
                Err(err)
            }
            x => x,
        }
    }

    /// Removes all pages past the provided `pgno`.
    pub(crate) fn truncate(&self, pgno: ltx::PageNum) -> io::Result<()> {
        log::debug!("[pager] truncate: pgno = {}", pgno);

        match self.truncate_inner(pgno) {
            Err(err) => {
                log::warn!("[pager] truncate: pgno = {}: {:?}", pgno, err);
                Err(err)
            }
            x => x,
        }
    }

    fn get_page_inner(&self, pos: Option<ltx::Pos>, pgno: ltx::PageNum) -> io::Result<Page> {
        match self.get_page_local(pos, pgno) {
            Ok(page) => return Ok(page),
            Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
            _ => (),
        };

        self.get_page_remote(pos, pgno)
    }

    fn get_page_local(&self, _pos: Option<ltx::Pos>, pgno: ltx::PageNum) -> io::Result<Page> {
        let mut file = fs::File::open(self.root.join(pgno.file_name()))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        Ok(Page::new(pgno, buf))
    }

    fn get_page_remote(&self, pos: Option<ltx::Pos>, pgno: ltx::PageNum) -> io::Result<Page> {
        let pos = if let Some(pos) = pos {
            pos
        } else {
            return Err(io::ErrorKind::NotFound.into());
        };

        let pages = self.client.get_page(&self.db, pos, pgno)?;

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
            self.del_page(page_ref.number())?;
            self.put_page(page_ref)?;

            if page.number() == pgno {
                requested_page = Some(Page::new(page.number(), page.into_inner()))
            }
        }

        requested_page.ok_or(io::ErrorKind::NotFound.into())
    }

    fn put_page_inner(&self, page: PageRef) -> io::Result<()> {
        let tmp_name = self.root.join(format!("{}.tmp", page.number()));
        let final_name = self.root.join(page.number().file_name());

        let mut file = fs::File::create(&tmp_name)?;
        file.write_all(page.as_ref())?;
        fs::rename(tmp_name, final_name)
    }

    fn del_page_inner(&self, pgno: ltx::PageNum) -> io::Result<()> {
        let name = self.root.join(pgno.file_name());
        match fs::remove_file(name) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            x => x,
        }
    }

    fn truncate_inner(&self, pgno: ltx::PageNum) -> io::Result<()> {
        let fname: ffi::OsString = pgno.file_name().into();

        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_name() <= fname {
                continue;
            }

            fs::remove_file(entry.path())?;
        }

        Ok(())
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
