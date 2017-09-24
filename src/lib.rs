use std::thread;
use std::sync::{mpsc, Mutex, Arc};

pub struct WorkersPool<T> {
    tx: mpsc::Sender<T>,
}

impl<T> WorkersPool<T> where T: FnOnce() + Send + 'static {
    pub fn new(count: usize) -> WorkersPool<T> {
        let (tx, rx) = mpsc::channel::<T>();
        let chan = Arc::new(Mutex::new(rx));

        for _ in 1..count {
            let rx = chan.clone();

            thread::spawn(move ||
            {
                let f = rx.lock().unwrap().recv().unwrap();
                (f)();
            });

        }

        WorkersPool {tx: tx }
    }

    pub fn exec(&self, f: T) {
        self.tx.send(f).unwrap();
    }
}
