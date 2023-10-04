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
    use crate::{lfsc, PositionsLogger};
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
        period: time::Duration,
    }

    impl Db {
        fn sync_period(&self) -> Option<time::Duration> {
            if self.period.is_zero() {
                return None;
            }

            Some(self.period)
        }

        fn next_sync(&self) -> Option<time::SystemTime> {
            self.last_sync.checked_add(self.sync_period()?)
        }

        fn needs_sync(&self, now: &time::SystemTime) -> bool {
            if let Some(ref ns) = self.next_sync() {
                return ns <= now;
            }

            false
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
                    period: self.period,
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

            db.needs_sync(&time::SystemTime::now())
        }

        pub(crate) fn get_changes(
            &self,
            db: &str,
            _pos: Option<ltx::Pos>,
        ) -> io::Result<(Option<ltx::Pos>, Option<super::Changes>)> {
            let sym = self.sym(db);

            let mut dbs = self.dbs.lock().unwrap();
            while dbs.get(&sym).unwrap().needs_sync(&time::SystemTime::now()) {
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

        pub(crate) fn sync_one(&self, db: &str) -> io::Result<()> {
            let sym = self.sym(db);
            let pos = self.dbs.lock().unwrap().get(&sym).unwrap().position;

            let changes = self.client.sync_db(db, pos)?;

            self.dbs.lock().unwrap().entry(sym).and_modify(|db| {
                let local_txid = db.position.map(|p| p.txid.into_inner()).unwrap_or(0);
                let remote_txid = changes.pos().map(|p| p.txid.into_inner()).unwrap_or(0);

                if remote_txid >= local_txid {
                    db.position = changes.pos();
                    db.changes = merge_changes(changes.into(), db.changes.take());
                    db.last_sync = time::SystemTime::now();
                }
            });

            Ok(())
        }

        pub(crate) fn sync_period(&self, db: &str) -> time::Duration {
            let sym = self.sym(db);

            self.dbs.lock().unwrap().get(&sym).unwrap().period
        }

        pub(crate) fn set_sync_period(&self, db: &str, period: time::Duration) {
            let sym = self.sym(db);

            self.dbs.lock().unwrap().get_mut(&sym).unwrap().period = period;

            self.notify();
        }

        fn sync(&self, db_syms: &[DefaultSymbol]) -> io::Result<()> {
            let old_positions = {
                let interner = self.interner.lock().unwrap();
                let dbs = self.dbs.lock().unwrap();

                db_syms
                    .iter()
                    .filter_map(|&k| {
                        Some((
                            interner.resolve(k).unwrap().to_owned(),
                            dbs.get(&k)?.position,
                        ))
                    })
                    .collect()
            };

            log::debug!(
                "[syncer] sync: positions = {}",
                PositionsLogger(&old_positions)
            );
            let mut changes = self.client.sync(&old_positions)?;

            let interner = self.interner.lock().unwrap();
            let mut dbs = self.dbs.lock().unwrap();
            let now = time::SystemTime::now();
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
                db.last_sync = now;
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
                let (min_sync_period, last_sync) = {
                    let dbs = self.dbs.lock().unwrap();

                    (
                        dbs.values().filter_map(|db| db.sync_period()).min(),
                        dbs.values().map(|db| db.last_sync).max(),
                    )
                };

                let since_last_sync = if let Some(last_sync) = last_sync {
                    time::SystemTime::now()
                        .duration_since(last_sync)
                        .unwrap_or_default()
                } else {
                    time::Duration::ZERO
                };
                let next_sync =
                    min_sync_period.map(|p| p.checked_sub(since_last_sync).unwrap_or_default());

                let waiter = if let Some(next_sync) = next_sync {
                    log::debug!("[syncer]: next sync in {}ms", next_sync.as_millis());
                    after(next_sync)
                } else {
                    never()
                };

                select! {
                recv(rx) -> _ => (),
                recv(waiter) -> _ => (),
                };

                let now = time::SystemTime::now();
                let dbs = self
                    .dbs
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|(&k, db)| if db.needs_sync(&now) { Some(k) } else { None })
                    .collect::<Vec<_>>();
                if !dbs.is_empty() {
                    if let Err(err) = self.sync(&dbs) {
                        log::warn!("[syncer] run: sync failed: {}", err);
                    }
                }
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
        period: time::Duration,

        dbs: Mutex<HashMap<String, Db>>,
    }

    struct Db {
        last_sync: time::SystemTime,
        period: time::Duration,
    }

    impl Db {
        fn sync_period(&self) -> Option<time::Duration> {
            if self.period.is_zero() {
                return None;
            }

            Some(self.period)
        }

        fn next_sync(&self) -> Option<time::SystemTime> {
            self.last_sync.checked_add(self.sync_period()?)
        }

        fn needs_sync(&self, now: &time::SystemTime) -> bool {
            if let Some(ref ns) = self.next_sync() {
                return ns <= now;
            }

            false
        }
    }

    impl Syncer {
        pub(crate) fn new(client: Arc<lfsc::Client>, period: time::Duration) -> Arc<Syncer> {
            Arc::new(Syncer {
                client,
                period,

                dbs: Mutex::new(HashMap::new()),
            })
        }

        pub(crate) fn open_conn(&self, db: &str, _pos: Option<ltx::Pos>) {
            let mut dbs = self.dbs.lock().unwrap();

            if !dbs.contains_key(db) {
                dbs.insert(
                    db.to_string(),
                    Db {
                        last_sync: time::SystemTime::now(),
                        period: self.period,
                    },
                );
            }
        }

        pub(crate) fn close_conn(&self, _db: &str) {}

        pub(crate) fn needs_sync(&self, db: &str, _pos: Option<ltx::Pos>) -> bool {
            let dbs = self.dbs.lock().unwrap();

            let db = dbs.get(db).unwrap();

            db.needs_sync(&time::SystemTime::now())
        }

        pub(crate) fn get_changes(
            &self,
            db: &str,
            pos: Option<ltx::Pos>,
        ) -> io::Result<(Option<ltx::Pos>, Option<super::Changes>)> {
            let changes = self.client.sync_db(db, pos)?;

            let mut dbs = self.dbs.lock().unwrap();
            dbs.get_mut(db).unwrap().last_sync = time::SystemTime::now();

            Ok((changes.pos(), changes.into()))
        }

        pub(crate) fn put_changes(&self, _db: &str, _prev_changes: super::Changes) {}

        pub(crate) fn set_pos(&self, _db: &str, _pos: Option<ltx::Pos>) {}

        pub(crate) fn sync_one(&self, _db: &str) -> io::Result<()> {
            Ok(())
        }

        pub(crate) fn sync_period(&self, db: &str) -> time::Duration {
            self.dbs.lock().unwrap().get(db).unwrap().period
        }

        pub(crate) fn set_sync_period(&self, db: &str, period: time::Duration) {
            self.dbs.lock().unwrap().get_mut(db).unwrap().period = period;
        }
    }
}
