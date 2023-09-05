mod database;
mod ext;
mod http;
mod leaser;
mod lfsc;
mod locks;
mod pager;
mod syncer;
mod vfs;

use litetx as ltx;
use sqlite_vfs::ffi;
use std::{collections::BTreeSet, fmt};

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

struct PageNumLogger<'a>(&'a BTreeSet<ltx::PageNum>);

impl<'a> fmt::Display for PageNumLogger<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[")?;
        for (i, pgno) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", pgno)?;
        }

        write!(f, "]")
    }
}
