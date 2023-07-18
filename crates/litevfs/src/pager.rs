use ltx::PageChecksum;
use std::{
    ffi, fs,
    io::{self, Read, Write},
    path::{self, Path},
    sync::Arc,
};

/// Trait for reading and writing SQLite pages from/to external storage.
pub(crate) trait Pager {
    /// Returns a database `page` at the given database `state`.
    fn get_page(&self, state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page>;

    /// Stores the database `page`.
    fn put_page(&self, page: &Page) -> io::Result<()>;

    /// Removes all database pages after `number`.
    fn truncate(&self, number: ltx::PageNum) -> io::Result<()>;
}

/// A struct that owns a single database page.
/// It is cheap to clone a `page` as the underlying storage is shared
/// between the copies.
#[derive(Clone)]
pub(crate) struct Page {
    data: Arc<Vec<u8>>,
    number: ltx::PageNum,
    checksum: ltx::Checksum,
}

impl Page {
    /// Return a new [Page] with `number` and the given `data`.
    pub(crate) fn new(number: ltx::PageNum, data: Vec<u8>) -> Page {
        let checksum = data.page_checksum(number);
        Page {
            data: Arc::new(data),
            number,
            checksum,
        }
    }

    /// Returns `page` number.
    pub(crate) fn number(&self) -> ltx::PageNum {
        self.number
    }

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

    fn put_page(&self, page: &Page) -> io::Result<()> {
        self.inner.put_page(page)
    }

    fn truncate(&self, number: ltx::PageNum) -> io::Result<()> {
        self.inner.truncate(number)
    }
}

/// A [Pager] that uses local filesystem to store pages.
/// It assumes that it always works with the up-to-date database states
/// and cannot fetch pages at particular state.
pub(crate) struct FilesystemPager {
    root: path::PathBuf,
}

impl FilesystemPager {
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> io::Result<FilesystemPager> {
        fs::create_dir_all(&path)?;

        Ok(FilesystemPager {
            root: path.as_ref().to_path_buf(),
        })
    }
}

impl Pager for FilesystemPager {
    fn get_page(&self, _state: Option<ltx::Pos>, number: ltx::PageNum) -> io::Result<Page> {
        let mut file = fs::File::open(self.root.join(number.to_string()))?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        Ok(Page::new(number, buf))
    }

    fn put_page(&self, page: &Page) -> io::Result<()> {
        let tmp_name = self.root.join(format!("{}.tmp", page.number()));
        let final_name = self.root.join(page.number().to_string());

        let mut file = fs::File::create(&tmp_name)?;
        file.write_all(page.as_ref())?;
        fs::rename(tmp_name, final_name)
    }

    fn truncate(&self, number: ltx::PageNum) -> io::Result<()> {
        let fname: ffi::OsString = number.to_string().into();

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
