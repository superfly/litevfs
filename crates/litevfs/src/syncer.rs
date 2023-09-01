use crate::lfsc;
use std::{
    collections::{BTreeSet, HashMap},
    io,
    sync::{Arc, Mutex},
    thread, time,
};

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

pub(crate) struct Syncer {
    client: Arc<lfsc::Client>,
    notifier: crossbeam_channel::Sender<()>,
    period: time::Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    // latest known position
    positions: HashMap<String, Option<ltx::Pos>>,
    // changes accumulated since the last DB sync
    changes: HashMap<String, Changes>,
    // numer of open connections per DB
    conns: HashMap<String, u32>,
}

impl Syncer {
    pub(crate) fn new(client: Arc<lfsc::Client>, period: time::Duration) -> Arc<Syncer> {
        let (tx, rx) = crossbeam_channel::unbounded();
        let syncer = Arc::new(Syncer {
            client,
            notifier: tx,
            period,
            inner: Mutex::new(Inner {
                positions: HashMap::new(),
                changes: HashMap::new(),
                conns: HashMap::new(),
            }),
        });

        thread::spawn({
            let syncer = Arc::clone(&syncer);

            move || syncer.run(rx)
        });

        syncer
    }

    pub(crate) fn open_conn(&self, db: &str, pos: Option<ltx::Pos>) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.positions.contains_key(db) {
            inner.positions.insert(db.into(), pos);
        }
        inner
            .conns
            .entry(db.into())
            .and_modify(|c| *c += 1)
            .or_insert(1);

        self.notify();
    }

    pub(crate) fn close_conn(&self, db: &str) {
        let mut inner = self.inner.lock().unwrap();

        let remove = {
            let c = inner.conns.get_mut(db).unwrap();
            *c -= 1;
            *c == 0
        };

        if remove {
            inner.positions.remove(db);
            inner.changes.remove(db);
            inner.conns.remove(db);
        }

        self.notify();
    }

    pub(crate) fn get_pos(&self, db: &str) -> Option<ltx::Pos> {
        self.inner
            .lock()
            .unwrap()
            .positions
            .get(db)
            .cloned()
            .flatten()
    }

    pub(crate) fn get_changes(&self, db: &str) -> (Option<ltx::Pos>, Option<Changes>) {
        let mut inner = self.inner.lock().unwrap();

        (
            inner.positions.get(db).cloned().flatten(),
            inner.changes.remove(db),
        )
    }

    pub(crate) fn put_changes(&self, db: &str, prev_changes: Changes) {
        let mut inner = self.inner.lock().unwrap();
        let new_changes = inner.changes.remove(db);
        if let Some(changes) = merge_changes(Some(prev_changes), new_changes) {
            inner.changes.insert(db.into(), changes);
        }
    }

    fn notify(&self) {
        self.notifier.send(()).unwrap();
    }

    fn run(&self, rx: crossbeam_channel::Receiver<()>) {
        use crossbeam_channel::{after, never, select};

        loop {
            let has_dbs = self.inner.lock().unwrap().positions.len() > 0;
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

    fn sync(&self) -> io::Result<()> {
        let positions = self.inner.lock().unwrap().positions.clone();

        log::debug!("[syncer] sync: positions = {:?}", positions);

        let changes = self.client.sync(positions)?;

        let mut inner = self.inner.lock().unwrap();
        let positions = changes
            .iter()
            .map(|(k, v)| (k.to_string(), v.pos()))
            .collect();
        let changes = changes
            .into_iter()
            .filter_map(|(k, v)| {
                let changes = merge_changes(v.into(), inner.changes.remove(&k))?;
                Some((k, changes))
            })
            .collect();

        inner.positions = positions;
        inner.changes = changes;

        Ok(())
    }
}

fn merge_changes(c1: Option<Changes>, c2: Option<Changes>) -> Option<Changes> {
    match (c1, c2) {
        (c1, None) => c1,
        (None, c2) => c2,
        (Some(Changes::All), _) | (_, Some(Changes::All)) => Some(Changes::All),
        (Some(Changes::Pages(p1)), Some(Changes::Pages(p2))) => Some(Changes::Pages(&p1 | &p2)),
    }
}
