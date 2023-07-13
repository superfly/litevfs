use crate::locks::ConnLock;
use sqlite_vfs::{DatabaseHandle, LockKind};
use std::io;

pub struct LiteConnection {
    lock: ConnLock,
}

impl LiteConnection {
    pub(crate) fn new(lock: ConnLock) -> Self {
        LiteConnection { lock }
    }
}

impl DatabaseHandle for LiteConnection {
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> io::Result<u64> {
        Ok(0)
    }

    fn read_exact_at(&mut self, _buf: &mut [u8], _offset: u64) -> io::Result<()> {
        Ok(())
    }

    fn write_all_at(&mut self, _buf: &[u8], _offset: u64) -> io::Result<()> {
        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&mut self, _size: u64) -> io::Result<()> {
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> io::Result<bool> {
        Ok(self.lock.acquire(lock))
    }

    fn reserved(&mut self) -> io::Result<bool> {
        Ok(self.lock.reserved())
    }

    fn current_lock(&self) -> io::Result<LockKind> {
        Ok(self.lock.state())
    }

    fn wal_index(&self, _readonly: bool) -> io::Result<Self::WalIndex> {
        Ok(sqlite_vfs::WalDisabled::default())
    }
}
