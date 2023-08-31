use std::{sync::Arc, thread};

use crate::lfsc;

pub(crate) struct Syncer {
    client: Arc<lfsc::Client>,
    notifier: crossbeam_channel::Sender<()>,
}

impl Syncer {
    pub(crate) fn new(client: Arc<lfsc::Client>) -> Arc<Syncer> {
        let (tx, rx) = crossbeam_channel::unbounded();
        let syncer = Arc::new(Syncer {
            client,
            notifier: tx,
        });

        thread::spawn({
            let syncer = Arc::clone(&syncer);

            move || syncer.run(rx)
        });

        syncer
    }

    fn run(&self, rx: crossbeam_channel::Receiver<()>) {}
}
