mod database;
mod ext;
mod http;
mod leaser;
mod lfsc;
mod locks;
mod pager;
mod sqlite;
mod syncer;
mod vfs;

use litetx as ltx;
use sqlite_vfs::ffi;
use std::{collections::HashMap, fmt};

/// A custom SQLite error code to indicate that LFSC no longer have the
/// required state and LiteVFS can't recover from this in the middle of
/// a transaction. 'POS' in hex, which is hopefully large enough to never
/// collide with an upstream's error code.
const LITEVFS_IOERR_POS_MISMATCH: i32 = ffi::SQLITE_IOERR | (0x504F53 << 8);

struct OptionLogger<'a, T>(&'a Option<T>);

impl<'a, T> fmt::Display for OptionLogger<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(inner) = self.0 {
            inner.fmt(f)
        } else {
            write!(f, "<unknown>")
        }
    }
}

struct IterLogger<T>(T);

impl<T, I> fmt::Display for IterLogger<T>
where
    T: IntoIterator<Item = I> + Copy,
    I: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[")?;
        for (i, pgno) in self.0.into_iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", pgno)?;
        }

        write!(f, "]")
    }
}

struct PositionsLogger<'a>(&'a HashMap<String, Option<ltx::Pos>>);

impl<'a> fmt::Display for PositionsLogger<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;

        for (i, (db, pos)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}={}", db, OptionLogger(pos))?;
        }

        write!(f, "]")
    }
}
