use crate::{lfsc, IterLogger, OptionLogger, LITEVFS_IOERR_POS_MISMATCH};
use bytesize::ByteSize;
use caches::{Cache, SegmentedCache};
use litetx::{self as ltx, PageChecksum};
use read_write_at::ReadAtMut;
use sqlite_vfs::CodeError;
use std::{
    ffi, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use string_interner::{DefaultSymbol, StringInterner};

#[derive(PartialEq, Eq)]
pub(crate) enum PageSource {
    Local,
    Remote,
}

/// [Pager] manages SQLite page data. It uses local filesystem to cache
/// the pages and when the pages are absent in the cache, requests them from LFSC.
pub(crate) struct Pager {
    root: PathBuf,
    client: Arc<lfsc::Client>,

    interner: Mutex<StringInterner>,
    lru: Mutex<SegmentedCache<PageCacheKey, ()>>,

    min_available_space: AtomicU64,
    max_cached_pages: AtomicUsize,
}

impl Pager {
    pub(crate) fn new<P: AsRef<Path>>(path: P, client: Arc<lfsc::Client>) -> Pager {
        Pager {
            root: path.as_ref().to_path_buf(),
            client,

            interner: Mutex::new(StringInterner::new()),
            // The size is chosen from:
            //  - 128Mb of space
            //  - 4k page size
            // In reality is doesn't matter as we are gonna check available
            // FS space anyway. But we need some predetermined size as
            // the cache is not resizable.
            lru: Mutex::new(SegmentedCache::new(6500, 26000).unwrap()),

            min_available_space: AtomicU64::new(10 * 1024 * 1024),
            max_cached_pages: AtomicUsize::new(0),
        }
    }

    /// Returns a base path for the given `db`.
    pub(crate) fn db_path(&self, db: &str) -> PathBuf {
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
        prefetch: Option<&[ltx::PageNum]>,
    ) -> io::Result<Page> {
        log::debug!(
            "[pager] get_page: db = {}, pos = {}, pgno = {}, prefetch = {}",
            db,
            OptionLogger(&pos),
            pgno,
            IterLogger(if let Some(pgnos) = prefetch {
                pgnos
            } else {
                &[]
            }),
        );

        // Request the page either from local cache or from LFSC and convert
        // io::ErrorKind::NotFound errors to io::ErrorKind::UnexpectedEof, as
        // this is what local IO will return in case we read past the file.
        // TODO: we may need to suppress duplicated calls to the same page here.
        let r = match self.get_page_inner(db, pos, pgno, prefetch) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
            x => x,
        };

        // Log the error, if any
        match r {
            Err(err) => {
                log::error!(
                    "[pager] get_page: db = {}, pos = {}, pgno = {}, prefetch = {}: {}",
                    db,
                    OptionLogger(&pos),
                    pgno,
                    IterLogger(if let Some(pgnos) = prefetch {
                        pgnos
                    } else {
                        &[]
                    }),
                    err
                );
                Err(err)
            }
            x => x,
        }
    }

    /// Copies the page starting at `offset` to the provided buffer.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_page_slice(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
        buf: &mut [u8],
        offset: u64,
        local_only: bool,
        prefetch: Option<Vec<ltx::PageNum>>,
    ) -> io::Result<PageSource> {
        log::debug!(
            "[pager] get_page_slice: db = {}, pos = {}, pgno = {}, len = {}, offset = {}, local_only = {}, prefetch = {}",
            db,
            OptionLogger(&pos),
            pgno,
            buf.len(),
            offset,
            local_only,
            IterLogger(if let Some(pgnos) = prefetch.as_deref() { pgnos } else { &[] }),
        );

        // Request the page either from local cache or from LFSC and convert
        // io::ErrorKind::NotFound errors to io::ErrorKind::UnexpectedEof, as
        // this is what local IO will return in case we read past the file.
        // TODO: we may need to suppress duplicated calls to the same page here.
        let r = match self.get_page_slice_inner(
            db,
            pos,
            pgno,
            buf,
            offset,
            local_only,
            prefetch.as_deref(),
        ) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(io::ErrorKind::UnexpectedEof.into())
            }
            x => x,
        };

        // Log the error, if any
        match r {
            Err(err) => {
                log::error!(
                    "[pager] get_page_slice: db = {}, pos = {}, pgno = {}, len = {}, offset = {}, local_only = {}, prefetch = {}: {}",
                    db,
                    OptionLogger(&pos),
                    pgno,
                    buf.len(),
                    offset,
                    local_only,
                    IterLogger(if let Some(pgnos) = prefetch.as_deref() { pgnos } else { &[] }),
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
                log::error!(
                    "[pager] put_page: db = {}, pgno = {}: {}",
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
    pub(crate) fn del_page(&self, db: &str, pgno: ltx::PageNum) -> io::Result<bool> {
        log::debug!("[pager] del_page: db = {} , pgno = {}", db, pgno);

        match self.del_page_inner(db, pgno) {
            Err(err) => {
                log::error!("[pager] del_page: db = {}, pgno = {}: {}", db, pgno, err);
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
                log::error!("[pager] truncate: db = {}, pgno = {}: {}", db, pgno, err);
                Err(err)
            }
            x => x,
        }
    }

    /// Removes all pages of a database.
    pub(crate) fn clear(&self, db: &str) -> io::Result<Vec<ltx::PageNum>> {
        log::debug!("[pager] clear: db = {}", db);

        match self.clear_inner(db) {
            Err(err) => {
                log::error!("[pager] clear: db = {}: {}", db, err);
                Err(err)
            }
            x => x,
        }
    }

    /// Checks if the page is cached locally
    pub(crate) fn has_page(&self, db: &str, pgno: ltx::PageNum) -> io::Result<bool> {
        match self.has_page_inner(db, pgno) {
            Err(err) => {
                log::error!("[pager] has_page: db = {} pgno = {}: {}", db, pgno, err);
                Err(err)
            }
            x => x,
        }
    }

    /// Returns the minimum available space that pager is trying to keep on the FS.
    pub(crate) fn min_available_space(&self) -> u64 {
        self.min_available_space.load(Ordering::Acquire)
    }

    /// Sets the minimum available space that pager needs to maintain on the FS.
    pub(crate) fn set_min_available_space(&self, maa: u64) {
        self.min_available_space.store(maa, Ordering::Release)
    }

    /// Returns the maximum number of pages that pager will cache on local FS.
    pub(crate) fn max_cached_pages(&self) -> usize {
        self.max_cached_pages.load(Ordering::Acquire)
    }

    /// Sets the maximum number of pages that pager will cache on local FS.
    pub(crate) fn set_max_cached_pages(&self, mcp: usize) {
        self.max_cached_pages.store(mcp, Ordering::Release)
    }

    fn get_page_inner(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
        prefetch: Option<&[ltx::PageNum]>,
    ) -> io::Result<Page> {
        match self.get_page_local(db, pos, pgno) {
            Ok(page) => return Ok(page),
            Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
            _ => (),
        };

        self.get_page_remote(db, pos, pgno, prefetch)
    }

    #[allow(clippy::too_many_arguments)]
    fn get_page_slice_inner(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
        buf: &mut [u8],
        offset: u64,
        local_only: bool,
        prefetch: Option<&[ltx::PageNum]>,
    ) -> io::Result<PageSource> {
        match self.get_page_slice_local(db, pos, pgno, buf, offset) {
            Ok(_) => return Ok(PageSource::Local),
            Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
            _ => (),
        };

        if local_only {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "local_only page not found in cache",
            ));
        }

        let page = self.get_page_remote(db, pos, pgno, prefetch)?;
        let offset = offset as usize;
        buf.copy_from_slice(&page.as_ref()[offset..offset + buf.len()]);

        Ok(PageSource::Remote)
    }

    fn get_page_local(
        &self,
        db: &str,
        _pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
    ) -> io::Result<Page> {
        let mut file = fs::File::open(self.pages_path(db).join(PathBuf::from(pgno)))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // Mark the page as recently accessed
        self.lru.lock().unwrap().get(&self.cache_key(db, pgno));

        Ok(Page::new(pgno, buf))
    }

    fn get_page_slice_local(
        &self,
        db: &str,
        _pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
        buf: &mut [u8],
        offset: u64,
    ) -> io::Result<()> {
        let mut file = fs::File::open(self.pages_path(db).join(PathBuf::from(pgno)))?;
        file.read_exact_at(buf, offset)?;

        // Mark the page as recently accessed
        self.lru.lock().unwrap().get(&self.cache_key(db, pgno));

        Ok(())
    }

    fn get_page_remote(
        &self,
        db: &str,
        pos: Option<ltx::Pos>,
        pgno: ltx::PageNum,
        prefetch: Option<&[ltx::PageNum]>,
    ) -> io::Result<Page> {
        let pos = if let Some(pos) = pos {
            pos
        } else {
            return Err(io::ErrorKind::NotFound.into());
        };

        let mut pages = vec![pgno];
        if let Some(pgnos) = prefetch {
            pages.extend(pgnos);
        }
        let pages = match self.client.get_pages(db, pos, &pages) {
            Ok(pages) => pages,
            Err(lfsc::Error::PosMismatch(x)) => {
                log::warn!("get_page_remote: db = {}, pgno = {}, pos mismatch error, requested = {}, got = {}",
                    db, pgno, pos, x);
                // LFSC no longer have the requested pos. At this point we may try to recover
                // from this ourselves, or tell the user to retry the transaction. The only
                // safe situation when we can recover is when this is the very first read
                // of a TX. But, in 99.9% the very first read will hit the cache (page 1),
                // so just return a custom error code to the user. The client code can retry
                // the transaction automatically after that.
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    CodeError::new(LITEVFS_IOERR_POS_MISMATCH),
                ));
            }
            Err(x) => return Err(x.into()),
        };

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
            self.put_page_inner(db, page_ref)?;

            if page.number() == pgno {
                requested_page = Some(Page::new(page.number(), page.into_inner()))
            }
        }

        requested_page.ok_or(io::ErrorKind::NotFound.into())
    }

    fn put_page_inner(&self, db: &str, page: PageRef) -> io::Result<()> {
        let tmp_name = self.tmp_path(db).join(PathBuf::from(page.number()));
        let final_name = self.pages_path(db).join(PathBuf::from(page.number()));

        self.reclaim_space()?;

        let mut file = fs::File::create(&tmp_name)?;
        file.write_all(page.as_ref())?;
        fs::rename(tmp_name, final_name)?;

        self.lru
            .lock()
            .unwrap()
            .put(self.cache_key(db, page.number()), ());

        Ok(())
    }

    fn del_page_inner(&self, db: &str, pgno: ltx::PageNum) -> io::Result<bool> {
        let name = self.pages_path(db).join(PathBuf::from(pgno));
        let removed = remove_file(name)?;

        self.lru.lock().unwrap().remove(&self.cache_key(db, pgno));

        Ok(removed)
    }

    fn truncate_inner(&self, db: &str, pgno: ltx::PageNum) -> io::Result<()> {
        let fname: ffi::OsString = PathBuf::from(pgno).into();

        for entry in fs::read_dir(self.pages_path(db))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() || entry.file_name() <= fname {
                continue;
            }

            remove_file(entry.path())?;

            let rpgno = ltx::PageNum::try_from(Path::new(&entry.file_name()))?;
            self.lru.lock().unwrap().remove(&self.cache_key(db, rpgno));
        }

        Ok(())
    }

    fn clear_inner(&self, db: &str) -> io::Result<Vec<ltx::PageNum>> {
        let mut pgnos = Vec::new();

        for entry in fs::read_dir(self.pages_path(db))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }

            remove_file(entry.path())?;

            let rpgno = ltx::PageNum::try_from(Path::new(&entry.file_name()))?;
            self.lru.lock().unwrap().remove(&self.cache_key(db, rpgno));

            pgnos.push(rpgno);
        }

        Ok(pgnos)
    }

    fn has_page_inner(&self, db: &str, pgno: ltx::PageNum) -> io::Result<bool> {
        let page_name = self.pages_path(db).join(PathBuf::from(pgno));

        page_name.try_exists()
    }

    fn pages_path(&self, db: &str) -> PathBuf {
        self.db_path(db).join("pages")
    }

    fn tmp_path(&self, db: &str) -> PathBuf {
        self.db_path(db).join("tmp")
    }

    fn cache_key(&self, db: &str, pgno: ltx::PageNum) -> PageCacheKey {
        PageCacheKey {
            dbsym: self.interner.lock().unwrap().get_or_intern(db),
            pgno,
        }
    }

    fn reclaim_space(&self) -> io::Result<()> {
        let max_pages = self.max_cached_pages();
        let min_space = self.min_available_space();

        loop {
            let pages = self.lru.lock().unwrap().len();
            let space = statvfs(&self.root)?.available_space;

            log::trace!(
                "[pager] reclaim_space: pages = {}, max_pages = {}, space = {}, min_space = {}",
                pages,
                max_pages,
                ByteSize::b(space).to_string_as(true),
                ByteSize::b(min_space).to_string_as(true),
            );

            if pages == 0 || space >= min_space && (pages <= max_pages || max_pages == 0) {
                return Ok(());
            }

            self.remove_lru_page()?;
        }
    }

    fn remove_lru_page(&self) -> io::Result<()> {
        let cache_key = {
            let mut lru = self.lru.lock().unwrap();

            if let Some((cache_key, _)) = lru.remove_lru_from_probationary() {
                cache_key
            } else if let Some((cache_key, _)) = lru.remove_lru_from_protected() {
                cache_key
            } else {
                return Ok(());
            }
        };

        if let Some(db) = self.interner.lock().unwrap().resolve(cache_key.dbsym) {
            log::trace!(
                "[pager] remove_lru_page: db = {}, pgno = {}",
                db,
                cache_key.pgno
            );
            remove_file(self.pages_path(db).join(PathBuf::from(cache_key.pgno)))?;
        }

        Ok(())
    }
}

/// A struct that owns a single database page.
pub(crate) struct Page {
    data: Vec<u8>,
    number: ltx::PageNum,
}

impl Page {
    /// Return a new [Page] with `number` and the given `data`.
    pub(crate) fn new(number: ltx::PageNum, data: Vec<u8>) -> Page {
        Page { data, number }
    }

    /// Returns `page` number.
    pub(crate) fn number(&self) -> ltx::PageNum {
        self.number
    }

    /// Returns `page` checksum.
    pub(crate) fn checksum(&self) -> ltx::Checksum {
        self.data.page_checksum(self.number())
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

#[derive(PartialEq, Eq, Hash)]
struct PageCacheKey {
    dbsym: DefaultSymbol,
    pgno: ltx::PageNum,
}

fn remove_file<P: AsRef<Path>>(file: P) -> io::Result<bool> {
    match fs::remove_file(file) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(x) => Err(x),
        Ok(()) => Ok(true),
    }
}

struct FsStats {
    available_space: u64,
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn statvfs<P>(path: P) -> io::Result<FsStats>
where
    P: AsRef<Path>,
{
    use std::{mem, os::unix::prelude::OsStrExt};

    let cstr = match ffi::CString::new(path.as_ref().as_os_str().as_bytes()) {
        Ok(cstr) => cstr,
        Err(..) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contained a null",
            ))
        }
    };

    unsafe {
        let mut stat: libc::statvfs = mem::zeroed();
        if libc::statvfs(cstr.as_ptr() as *const _, &mut stat) != 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(FsStats {
                available_space: stat.f_frsize as u64 * stat.f_bavail as u64,
            })
        }
    }
}

#[cfg(windows)]
fn statvfs<P>(path: P) -> io::Result<FsStats>
where
    P: AsRef<Path>,
{
    use std::os::windows::ffi::OsStrExt;
    use winapi::{
        shared::minwindef::DWORD,
        um::fileapi::{GetDiskFreeSpaceW, GetVolumePathNameW},
    };

    let root_path: &mut [u16] = &mut [0; 261];
    let path_utf8: Vec<u16> = path
        .as_ref()
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    let available_space = unsafe {
        if GetVolumePathNameW(
            path_utf8.as_ptr(),
            root_path.as_mut_ptr(),
            root_path.len() as DWORD,
        ) == 0
        {
            return Err(io::Error::last_os_error());
        }

        let mut sectors_per_cluster = 0;
        let mut bytes_per_sector = 0;
        let mut number_of_free_clusters = 0;
        let mut total_number_of_clusters = 0;
        if GetDiskFreeSpaceW(
            root_path.as_ptr(),
            &mut sectors_per_cluster,
            &mut bytes_per_sector,
            &mut number_of_free_clusters,
            &mut total_number_of_clusters,
        ) == 0
        {
            return Err(io::Error::last_os_error());
        }

        let bytes_per_cluster = sectors_per_cluster as u64 * bytes_per_sector as u64;
        bytes_per_cluster * number_of_free_clusters as u64
    };

    Ok(FsStats { available_space })
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    #[test]
    fn statvfs() {
        let stats = super::statvfs(temp_dir()).expect("statvfs");

        assert!(stats.available_space > 0);
    }
}
