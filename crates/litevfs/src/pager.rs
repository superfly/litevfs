use crate::lfsc;
use ltx::PageChecksum;
use std::{
    ffi, fmt, fs,
    io::{self, Read, Write},
    path::{self, Path},
    sync::Arc,
};

/// Trait for reading and writing SQLite pages from/to external storage.
pub(crate) trait Pager: Sync {
    /// Returns a database `page` at the given database `state`.
    fn get_page(&self, state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page>;

    /// Stores the database `page`.
    fn put_page(&self, page: PageRef) -> io::Result<()>;

    /// Deletes the database page indentified by `number`.
    fn del_page(&self, number: ltx::PageNum) -> io::Result<()>;

    /// Removes all database pages after `number`.
    fn truncate(&self, number: ltx::PageNum) -> io::Result<()>;
}

/// A struct that owns a single database page.
/// It is cheap to clone a `page` as the underlying storage is shared
/// between the copies.
#[derive(Clone)]
pub(crate) struct Page {
    data: Arc<Vec<u8>>,
    _number: ltx::PageNum,
    checksum: ltx::Checksum,
}

impl Page {
    /// Return a new [Page] with `number` and the given `data`.
    pub(crate) fn new(number: ltx::PageNum, data: Vec<u8>) -> Page {
        let checksum = data.page_checksum(number);
        Page {
            data: Arc::new(data),
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

/// A struct that borrows a single database page.
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

/// A [Pager] that logs errors returned from the underlying pagers.
pub(crate) struct LoggingPager<P> {
    inner: P,
}

impl<P: Pager> LoggingPager<P> {
    pub(crate) fn new(pager: P) -> LoggingPager<P> {
        LoggingPager { inner: pager }
    }
}

impl<P: Pager> Pager for LoggingPager<P> {
    fn get_page(&self, state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page> {
        match self.inner.get_page(state, number) {
            Ok(page) => Ok(page),
            Err(err) => {
                log::warn!(
                    "[pager] get_page: state = {}, number = {}: {:?}",
                    PosLogger(&state),
                    number,
                    err
                );
                Err(err)
            }
        }
    }

    fn put_page(&self, page: PageRef) -> io::Result<()> {
        match self.inner.put_page(page) {
            Ok(()) => Ok(()),
            Err(err) => {
                log::warn!("[pager] put_page: number = {}: {:?}", page.number(), err,);
                Err(err)
            }
        }
    }

    fn del_page(&self, number: ltx::PageNum) -> io::Result<()> {
        match self.inner.del_page(number) {
            Ok(()) => Ok(()),
            Err(err) => {
                log::warn!("[pager] del_page: number = {}: {:?}", number, err,);
                Err(err)
            }
        }
    }

    fn truncate(&self, number: ltx::PageNum) -> io::Result<()> {
        match self.inner.truncate(number) {
            Ok(()) => Ok(()),
            Err(err) => {
                log::warn!("[pager] truncate: number = {}: {:?}", number, err,);
                Err(err)
            }
        }
    }
}

/// A [Pager] that translates [std::io::ErrorKind::NotFound] errors into
/// [std::io::ErrorKind::UnexpectedEof] errors.
pub(crate) struct ShortReadPager<P> {
    inner: P,
}

impl<P: Pager> ShortReadPager<P> {
    pub(crate) fn new(pager: P) -> ShortReadPager<P> {
        ShortReadPager { inner: pager }
    }
}

impl<P: Pager> Pager for ShortReadPager<P> {
    fn get_page(&self, state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page> {
        match self.inner.get_page(state, number) {
            Ok(page) => Ok(page),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
            Err(err) => Err(err),
        }
    }

    fn put_page(&self, page: PageRef) -> io::Result<()> {
        self.inner.put_page(page)
    }

    fn del_page(&self, number: ltx::PageNum) -> io::Result<()> {
        self.inner.del_page(number)
    }

    fn truncate(&self, number: ltx::PageNum) -> io::Result<()> {
        self.inner.truncate(number)
    }
}

/// A [Pager] that uses local filesystem to store pages.
/// It assumes that it always works with the up-to-date database states
/// and cannot fetch pages at particular state.
pub(crate) struct FilesystemPager {
    db: String,
    root: path::PathBuf,
    client: Arc<lfsc::Client>,
}

impl FilesystemPager {
    pub(crate) fn new<P: AsRef<Path>>(
        db: &str,
        path: P,
        client: Arc<lfsc::Client>,
    ) -> io::Result<FilesystemPager> {
        fs::create_dir_all(path.as_ref())?;

        Ok(FilesystemPager {
            db: db.into(),
            root: path.as_ref().to_path_buf(),
            client,
        })
    }

    fn get_page_local(&self, _state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page> {
        let mut file = fs::File::open(self.root.join(number.file_name()))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        Ok(Page::new(number, buf))
    }

    fn get_page_remote(&self, state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page> {
        let pos = if let Some(pos) = state {
            pos
        } else {
            return Err(io::ErrorKind::NotFound.into());
        };

        let pages = self.client.get_page(&self.db, pos, number)?;

        let mut requested_page: Option<Page> = None;
        for page in pages {
            let page_ref = PageRef {
                data: page.as_ref(),
                number: page.number(),
            };
            self.del_page(number)?;
            self.put_page(page_ref)?;

            if page.number() == number {
                requested_page = Some(Page::new(page.number(), page.into_inner()))
            }
        }

        requested_page.ok_or(io::ErrorKind::NotFound.into())
    }
}

impl Pager for FilesystemPager {
    fn get_page(&self, state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page> {
        log::debug!(
            "[fs-pager] get_page: state = {}, number = {}",
            PosLogger(&state),
            number,
        );

        match self.get_page_local(state, number) {
            Ok(page) => return Ok(page),
            Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
            _ => (),
        };

        self.get_page_remote(state, number)
    }

    fn put_page(&self, page: PageRef) -> io::Result<()> {
        log::debug!("[fs-pager] put_page: number = {}", page.number());

        let tmp_name = self.root.join(format!("{}.tmp", page.number()));
        let final_name = self.root.join(page.number().file_name());

        let mut file = fs::File::create(&tmp_name)?;
        file.write_all(page.as_ref())?;
        fs::rename(tmp_name, final_name)
    }

    fn del_page(&self, number: ltx::PageNum) -> io::Result<()> {
        log::debug!("[fs-pager] del_page: number = {}", number);

        let name = self.root.join(number.file_name());
        match fs::remove_file(name) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            x => x,
        }
    }

    fn truncate(&self, number: ltx::PageNum) -> io::Result<()> {
        log::debug!("[fs-pager] truncate: number = {}", number);

        let fname: ffi::OsString = number.file_name().into();

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
