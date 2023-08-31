#[cfg(not(target_os = "emscripten"))]
pub(crate) use native::Leaser;

#[cfg(target_os = "emscripten")]
pub(crate) use emscripten::Leaser;

#[cfg(not(target_os = "emscripten"))]
mod native {
    use crate::lfsc;
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
        thread,
    };

    pub(crate) struct Leaser {
        client: Arc<lfsc::Client>,
        leases: Mutex<HashMap<String, lfsc::Lease>>,

        duration: std::time::Duration,
        notifier: crossbeam_channel::Sender<()>,
    }

    impl Leaser {
        pub(crate) fn new(client: Arc<lfsc::Client>, duration: std::time::Duration) -> Arc<Leaser> {
            let (tx, rx) = crossbeam_channel::unbounded();
            let leaser = Arc::new(Leaser {
                client,
                leases: Mutex::new(HashMap::new()),
                duration,
                notifier: tx,
            });

            thread::spawn({
                let leaser = Arc::clone(&leaser);

                move || leaser.run(rx)
            });

            leaser
        }

        pub(crate) fn acquire_lease(&self, db: &str) -> io::Result<()> {
            let lease = match self
                .client
                .acquire_lease(db, lfsc::LeaseOp::Acquire(self.duration))
            {
                Ok(lease) => {
                    log::debug!("[leaser] acquire_lease: db = {}: {}", db, lease);
                    lease
                }
                Err(err) => {
                    log::warn!("[leaser] acquire_lease: db = {}: {:?}", db, err);
                    return Err(err.into());
                }
            };

            self.leases.lock().unwrap().insert(db.into(), lease);
            self.notify();

            Ok(())
        }

        pub(crate) fn release_lease(&self, db: &str) -> io::Result<()> {
            if let Some(lease) = self.leases.lock().unwrap().remove(db) {
                match self.client.release_lease(db, lease) {
                    Ok(()) => {
                        log::debug!("[leaser] release_lease: db = {}", db);
                    }
                    Err(err) => {
                        log::warn!("[leaser] release_lease: db = {}: {:?}", db, err);
                        return Err(err.into());
                    }
                };
                self.notify();
            }

            Ok(())
        }

        pub(crate) fn get_lease(&self, db: &str) -> io::Result<String> {
            self.leases
                .lock()
                .unwrap()
                .get(db)
                .map(|lease| lease.id.clone())
                .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "lease not found"))
        }

        fn notify(&self) {
            self.notifier.send(()).unwrap();
        }

        fn run(&self, rx: crossbeam_channel::Receiver<()>) {
            use crossbeam_channel::{after, select};
            use time::OffsetDateTime;

            let min_period = self.duration / 3;

            loop {
                // TODO: we probably not gonna have a lot of leases, but might need to optimize later
                let first = {
                    let leases = self.leases.lock().unwrap();
                    leases
                        .iter()
                        .min_by_key(|(_, lease)| lease.expires_at)
                        .map(|(db, lease)| (db.clone(), lease.clone()))
                };
                let (db, lease) = if let Some((db, lease)) = first {
                    (db, lease)
                } else {
                    // No active leases, wait to get notified
                    rx.recv().unwrap();
                    continue;
                };

                let until_expires = lease.expires_at - OffsetDateTime::now_utc();
                let wait_for =
                    if until_expires.is_negative() || until_expires.unsigned_abs() < min_period {
                        std::time::Duration::from_micros(1)
                    } else {
                        min_period
                    };

                select! {
                recv(rx) -> _ => continue,
                recv(after(wait_for)) -> _ => {
                    // Check if we are still holding the lease
                    if !self.leases.lock().unwrap().contains_key(&db) {
                        continue
                    }

                    // This can potentially reacquire a released lease, but since it won't be
                    // in map anymore, it will expire by itself.
                    log::debug!("[leaser] refreshing lease: db = {}, lease = {}", db, lease);
                    match self.client.acquire_lease(&db, lfsc::LeaseOp::Refresh(&lease.id, self.duration)) {
                        Ok(lease) => {
                            self.leases.lock().unwrap().entry(db).and_modify(|old_lease| *old_lease = lease);
                        },
                        Err(err) => {
                            log::warn!("[leaser] failed to refresh lease: db = {}, lease = {}: {:?}", db, lease, err);
                            // It's possible a new one has been acquired
                            let mut leases = self.leases.lock().unwrap();
                            match leases.get(&db) {
                                Some(l) if l.id == lease.id => {
                                    leases.remove(&db);
                                },
                                _ => (),
                            };
                        },
                    };
                },
                };
            }
        }
    }
}

#[cfg(target_os = "emscripten")]
mod emscripten {
    use crate::lfsc;
    use std::{io, sync::Arc};

    pub(crate) struct Leaser;

    impl Leaser {
        pub(crate) fn new(client: Arc<lfsc::Client>, duration: std::time::Duration) -> Arc<Leaser> {
            Arc::new(Leaser)
        }

        pub(crate) fn acquire_lease(&self, db: &str) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "write leases are not supported in WASM",
            ))
        }

        pub(crate) fn release_lease(&self, db: &str) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "write leases are not supported in WASM",
            ))
        }

        pub(crate) fn get_lease(&self, db: &str) -> io::Result<String> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "write leases are not supported in WASM",
            ))
        }
    }
}
