use crate::lfsc;
use litetx as ltx;
use std::collections::BTreeSet;

#[derive(Debug)]
pub(crate) enum Changes {
    All,
    Pages(BTreeSet<ltx::PageNum>),
}

impl From<lfsc::Changes> for Option<Changes> {
    fn from(c: lfsc::Changes) -> Self {
        match c {
            lfsc::Changes::All(_) => Some(Changes::All),
            lfsc::Changes::Pages(_, None) => None,
            lfsc::Changes::Pages(_, Some(pages)) => {
                Some(Changes::Pages(BTreeSet::from_iter(pages)))
            }
        }
    }
}

#[cfg(not(target_os = "emscripten"))]
pub(crate) use native::Syncer;

#[cfg(target_os = "emscripten")]
pub(crate) use emscripten::Syncer;

#[cfg(not(target_os = "emscripten"))]
mod native {
    use crate::lfsc;
    use litetx as ltx;
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Condvar, Mutex},
        thread, time,
    };
    use string_interner::{DefaultSymbol, StringInterner};

    pub(crate) struct Syncer {
        client: Arc<lfsc::Client>,
        notifier: crossbeam_channel::Sender<()>,
        period: time::Duration,

        interner: Mutex<StringInterner>,
        dbs: Mutex<HashMap<DefaultSymbol, Db>>,
        cvar: Condvar,
    }

    struct Db {
        position: Option<ltx::Pos>,
        changes: Option<super::Changes>,
        conns: u32,

        last_sync: time::SystemTime,
    }

    impl Db {
        fn time_since_last_sync(&self) -> time::Duration {
            time::SystemTime::now()
                .duration_since(self.last_sync)
                .unwrap_or_default()
        }
    }

    impl Syncer {
        pub(crate) fn new(client: Arc<lfsc::Client>, period: time::Duration) -> Arc<Syncer> {
            let (tx, rx) = crossbeam_channel::unbounded();
            let syncer = Arc::new(Syncer {
                client,
                notifier: tx,
                period,
                interner: Mutex::new(StringInterner::new()),
                dbs: Mutex::new(HashMap::new()),
                cvar: Condvar::new(),
            });

            thread::spawn({
                let syncer = Arc::clone(&syncer);

                move || syncer.run(rx)
            });

            syncer
        }

        fn sym(&self, db: &str) -> DefaultSymbol {
            self.interner.lock().unwrap().get_or_intern(db)
        }

        pub(crate) fn open_conn(&self, db: &str, pos: Option<ltx::Pos>) {
            let sym = self.sym(db);

            self.dbs
                .lock()
                .unwrap()
                .entry(sym)
                .and_modify(|db| db.conns += 1)
                .or_insert(Db {
                    position: pos,
                    changes: None,
                    conns: 1,
                    last_sync: time::SystemTime::now(),
                });

            self.notify();
        }

        pub(crate) fn close_conn(&self, db: &str) {
            let sym = self.sym(db);

            let mut dbs = self.dbs.lock().unwrap();
            let remove = {
                let db = dbs.get_mut(&sym).unwrap();

                assert!(db.conns > 0);
                db.conns -= 1;
                db.conns == 0
            };

            if remove {
                dbs.remove(&sym);
            }

            self.notify();
        }

        pub(crate) fn needs_sync(&self, db: &str, pos: Option<ltx::Pos>) -> bool {
            let sym = self.sym(db);

            let dbs = self.dbs.lock().unwrap();
            let db = dbs.get(&sym).unwrap();
            if db.position.is_some() && db.position != pos {
                return true;
            }
            if db.time_since_last_sync() > self.period {
                return true;
            }

            false
        }

        pub(crate) fn get_changes(
            &self,
            db: &str,
            _pos: Option<ltx::Pos>,
        ) -> io::Result<(Option<ltx::Pos>, Option<super::Changes>)> {
            let sym = self.sym(db);

            let mut dbs = self.dbs.lock().unwrap();
            while dbs.get(&sym).unwrap().time_since_last_sync() > self.period {
                self.notify();
                dbs = self.cvar.wait(dbs).unwrap();
            }

            let db = dbs.get_mut(&sym).unwrap();

            Ok((db.position, db.changes.take()))
        }

        pub(crate) fn put_changes(&self, db: &str, prev_changes: super::Changes) {
            let sym = self.sym(db);

            let mut dbs = self.dbs.lock().unwrap();
            let db = dbs.get_mut(&sym).unwrap();

            db.changes = merge_changes(Some(prev_changes), db.changes.take())
        }

        pub(crate) fn set_pos(&self, db: &str, pos: Option<ltx::Pos>) {
            let pos = if let Some(pos) = pos { pos } else { return };

            let sym = self.sym(db);

            let mut dbs = self.dbs.lock().unwrap();
            let db = dbs.get_mut(&sym).unwrap();

            if matches!(db.position, Some(rp) if rp.txid > pos.txid) {
                return;
            }

            db.position = Some(pos);
            db.last_sync = time::SystemTime::now();
            db.changes.take();
        }

        pub(crate) fn sync(&self) -> io::Result<()> {
            let old_positions = {
                let interner = self.interner.lock().unwrap();

                self.dbs
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(&k, db)| (interner.resolve(k).unwrap().to_owned(), db.position))
                    .collect()
            };

            log::debug!("[syncer] sync: positions = {:?}", old_positions);
            let mut changes = self.client.sync(&old_positions)?;

            let interner = self.interner.lock().unwrap();
            let mut dbs = self.dbs.lock().unwrap();
            for (&k, db) in dbs.iter_mut() {
                let name = interner.resolve(k).unwrap();
                let (new_pos, changes) = if let Some(changes) = changes.remove(name) {
                    (changes.pos(), changes.into())
                } else {
                    (None, None)
                };

                db.changes = merge_changes(
                    if old_positions.get(name) == Some(&db.position) {
                        changes
                    } else {
                        None
                    },
                    db.changes.take(),
                );
                db.position = new_pos;
                db.last_sync = time::SystemTime::now();
            }

            self.cvar.notify_all();

            Ok(())
        }

        fn notify(&self) {
            self.notifier.send(()).unwrap();
        }

        fn run(&self, rx: crossbeam_channel::Receiver<()>) {
            use crossbeam_channel::{after, never, select};

            loop {
                let has_dbs = !self.dbs.lock().unwrap().is_empty();
                let waiter = if has_dbs {
                    if let Err(err) = self.sync() {
                        log::warn!("[syncer] run: sync failed: {:?}", err);
                    }

                    after(self.period)
                } else {
                    never()
                };

                select! {
                recv(rx) -> _ => (),
                recv(waiter) -> _ => (),
                };
            }
        }
    }

    fn merge_changes(
        c1: Option<super::Changes>,
        c2: Option<super::Changes>,
    ) -> Option<super::Changes> {
        match (c1, c2) {
            (c1, None) => c1,
            (None, c2) => c2,
            (Some(super::Changes::All), _) | (_, Some(super::Changes::All)) => {
                Some(super::Changes::All)
            }
            (Some(super::Changes::Pages(p1)), Some(super::Changes::Pages(p2))) => {
                Some(super::Changes::Pages(&p1 | &p2))
            }
        }
    }
}

#[cfg(target_os = "emscripten")]
mod emscripten {
    use crate::lfsc;
    use litetx as ltx;
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
        time,
    };

    pub(crate) struct Syncer {
        client: Arc<lfsc::Client>,
        sync_times: Mutex<HashMap<String, time::SystemTime>>,
        period: time::Duration,
    }

    impl Syncer {
        pub(crate) fn new(client: Arc<lfsc::Client>, period: time::Duration) -> Arc<Syncer> {
            Arc::new(Syncer {
                client,
                sync_times: Mutex::new(HashMap::new()),
                period,
            })
        }

        pub(crate) fn open_conn(&self, db: &str, _pos: Option<ltx::Pos>) {
            let mut sync_times = self.sync_times.lock().unwrap();

            if !sync_times.contains_key(db) {
                sync_times.insert(db.to_string(), time::SystemTime::now());
            }
        }

        pub(crate) fn close_conn(&self, _db: &str) {}

        pub(crate) fn needs_sync(&self, db: &str, _pos: Option<ltx::Pos>) -> bool {
            let last_sync = if let Some(last_sync) = self.sync_times.lock().unwrap().get(db) {
                *last_sync
            } else {
                return false;
            };

            let dur = if let Ok(dur) = time::SystemTime::now().duration_since(last_sync) {
                dur
            } else {
                return true;
            };

            dur > self.period
        }

        pub(crate) fn get_changes(
            &self,
            db: &str,
            pos: Option<ltx::Pos>,
        ) -> io::Result<(Option<ltx::Pos>, Option<super::Changes>)> {
            let changes = self.client.sync_db(db, pos)?;

            if let Some(last) = self.sync_times.lock().unwrap().get_mut(db) {
                *last = time::SystemTime::now();
            };

            Ok((changes.pos(), changes.into()))
        }

        pub(crate) fn put_changes(&self, _db: &str, _prev_changes: super::Changes) {}

        pub(crate) fn sync(&self) -> io::Result<()> {
            Ok(())
        }

        pub(crate) fn set_pos(&self, _db: &str, _pos: Option<ltx::Pos>) {}
    }
}
