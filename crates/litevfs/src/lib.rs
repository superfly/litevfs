mod database;
mod ext;
mod lfsc;
mod locks;
mod pager;
mod vfs;

use std::fmt;

pub use vfs::LiteVfs;

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
