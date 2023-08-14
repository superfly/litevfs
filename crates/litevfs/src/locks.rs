use std::sync::{Arc, Mutex};

use sqlite_vfs::LockKind;

pub(crate) struct VfsLock {
    inner: Arc<Mutex<InnerVfsLock>>,
}

// Lock implements in-memory SQLite lock shared between multiple connections.
// https://www.sqlite.org/lockingv3.html
impl VfsLock {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InnerVfsLock::new())),
        }
    }

    pub(crate) fn conn_lock(&self) -> ConnLock {
        ConnLock::new(Arc::clone(&self.inner))
    }

    #[cfg(test)]
    fn readers(&self) -> usize {
        self.inner.lock().unwrap().readers()
    }

    #[cfg(test)]
    fn has_writer(&self) -> bool {
        self.inner.lock().unwrap().has_writer()
    }
}

struct InnerVfsLock {
    readers: usize,
    writer: Option<bool>,
}

impl InnerVfsLock {
    fn new() -> Self {
        Self {
            readers: 0,
            writer: None,
        }
    }

    fn transition(&mut self, from: LockKind, to: LockKind) -> LockKind {
        if from == to {
            return from;
        }

        match to {
            LockKind::None => {
                if from == LockKind::Shared {
                    // Connection is a reader
                    assert!(self.readers >= 1);
                    self.readers -= 1;
                } else if from > LockKind::Shared {
                    // Connection has at least RESERVED lock, only one is possible at a time
                    self.writer = None;
                }
                LockKind::None
            }

            LockKind::Shared => {
                // PENDING lock is active, can't promote from NONE to SHARED
                if self.writer == Some(true) && from < LockKind::Shared {
                    return from;
                }

                self.readers += 1;
                // Downgrade from a write lock
                if from > LockKind::Shared {
                    self.writer = None
                }
                LockKind::Shared
            }

            LockKind::Reserved => {
                // If there is already a writer, or the connection is not in read mode, deny
                if self.writer.is_some() || from != LockKind::Shared {
                    return from;
                }

                assert!(self.readers >= 1);
                self.readers -= 1;
                self.writer = Some(false);
                LockKind::Reserved
            }

            // Never requested explicitly
            LockKind::Pending => from,

            LockKind::Exclusive => {
                // Another connection is already a writer
                if self.writer.is_some() && from < LockKind::Reserved {
                    return from;
                }

                if from == LockKind::Shared {
                    self.readers -= 1;
                }

                self.writer = Some(true);
                if self.readers > 0 {
                    LockKind::Pending
                } else {
                    LockKind::Exclusive
                }
            }
        }
    }

    fn readers(&self) -> usize {
        self.readers
    }

    fn has_writer(&self) -> bool {
        self.writer.is_some()
    }
}

/// ConnLock tracks individial connection lock state.
pub(crate) struct ConnLock {
    vfs_lock: Arc<Mutex<InnerVfsLock>>,
    state: LockKind,
}

impl ConnLock {
    fn new(vfs_lock: Arc<Mutex<InnerVfsLock>>) -> ConnLock {
        ConnLock {
            vfs_lock,
            state: LockKind::None,
        }
    }

    pub(crate) fn acquire(&mut self, to: LockKind) -> bool {
        self.state = self.vfs_lock.lock().unwrap().transition(self.state, to);
        self.state == to
    }

    pub(crate) fn state(&self) -> LockKind {
        self.state
    }

    pub(crate) fn reserved(&self) -> bool {
        self.state >= LockKind::Shared || self.vfs_lock.lock().unwrap().has_writer()
    }

    pub(crate) fn readers(&self) -> usize {
        self.vfs_lock.lock().unwrap().readers()
    }
}

impl Drop for ConnLock {
    fn drop(&mut self) {
        self.acquire(LockKind::None);
    }
}

#[cfg(test)]
mod tests {
    use super::VfsLock;
    use sqlite_vfs::LockKind;

    #[test]
    fn muiltiple_readers() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = vfs_lock.conn_lock();
        let mut conn2_lock = vfs_lock.conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn2_lock.acquire(LockKind::Shared));
        assert_eq!(2, vfs_lock.readers());
        assert_eq!(LockKind::Shared, conn1_lock.state());
        assert_eq!(LockKind::Shared, conn2_lock.state());
    }

    #[test]
    fn reader_while_reserved() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = vfs_lock.conn_lock();
        let mut conn2_lock = vfs_lock.conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(conn2_lock.acquire(LockKind::Shared));
        assert_eq!(1, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Reserved, conn1_lock.state());
        assert_eq!(LockKind::Shared, conn2_lock.state());
    }

    #[test]
    fn only_one_reserved() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();
        let mut conn2_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(conn2_lock.acquire(LockKind::Shared));
        assert!(!conn2_lock.acquire(LockKind::Reserved));
        assert_eq!(1, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Reserved, conn1_lock.state());
        assert_eq!(LockKind::Shared, conn2_lock.state());
    }

    #[test]
    fn pending_if_readers() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();
        let mut conn2_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn2_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(!conn1_lock.acquire(LockKind::Exclusive));
        assert_eq!(1, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Pending, conn1_lock.state());
        assert_eq!(LockKind::Shared, conn2_lock.state());
    }

    #[test]
    fn exclusive_if_no_readers() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(conn1_lock.acquire(LockKind::Exclusive));
        assert_eq!(0, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Exclusive, conn1_lock.state());
    }

    #[test]
    fn pending_to_exclusive() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();
        let mut conn2_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn2_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(!conn1_lock.acquire(LockKind::Exclusive));
        assert_eq!(1, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Pending, conn1_lock.state());
        assert_eq!(LockKind::Shared, conn2_lock.state());

        assert!(conn2_lock.acquire(LockKind::None));
        assert!(conn1_lock.acquire(LockKind::Exclusive));
        assert_eq!(0, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Exclusive, conn1_lock.state());
        assert_eq!(LockKind::None, conn2_lock.state());
    }

    #[test]
    fn no_new_readers_while_pending() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();
        let mut conn2_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn2_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(!conn1_lock.acquire(LockKind::Exclusive));

        let mut conn3_lock = (&vfs_lock).conn_lock();

        assert!(!conn3_lock.acquire(LockKind::Shared));
        assert_eq!(LockKind::None, conn3_lock.state());
    }

    #[test]
    fn no_new_readers_while_exclusive() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Reserved));
        assert!(conn1_lock.acquire(LockKind::Exclusive));

        let mut conn2_lock = (&vfs_lock).conn_lock();

        assert!(!conn2_lock.acquire(LockKind::Shared));
        assert_eq!(LockKind::None, conn2_lock.state());
    }

    #[test]
    fn exclusive_from_shared() {
        let vfs_lock = VfsLock::new();

        let mut conn1_lock = (&vfs_lock).conn_lock();

        assert!(conn1_lock.acquire(LockKind::Shared));
        assert!(conn1_lock.acquire(LockKind::Exclusive));
        assert_eq!(0, vfs_lock.readers());
        assert!(vfs_lock.has_writer());
        assert_eq!(LockKind::Exclusive, conn1_lock.state());
    }

    #[test]
    fn drop_unlocks() {
        let vfs_lock = VfsLock::new();

        {
            let mut conn1_lock = (&vfs_lock).conn_lock();
            assert!(conn1_lock.acquire(LockKind::Shared));
            assert_eq!(1, vfs_lock.readers());
        }
        assert_eq!(0, vfs_lock.readers());

        {
            let mut conn1_lock = (&vfs_lock).conn_lock();
            assert!(conn1_lock.acquire(LockKind::Shared));
            assert!(conn1_lock.acquire(LockKind::Exclusive));
            assert!(vfs_lock.has_writer());
        }
        assert!(!vfs_lock.has_writer());
    }
}
