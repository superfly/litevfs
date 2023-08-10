mod database;
mod ext;
mod lfsc;
mod locks;
mod pager;
mod vfs;

use sqlite_vfs::ffi;
use std::fmt;
pub use vfs::LiteVfs;

/// A custom SQLite error code to indicate that LFSC no longer have the
/// required state and LiteVFS can't recover from this in the middle of
/// a transaction. 'POS' in hex, which is hopefully large enough to never
/// collide with an upstream's error code.
const LITEVFS_IOERR_POS_MISMATCH: i32 = ffi::SQLITE_IOERR | (0x504F53 << 8);

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
