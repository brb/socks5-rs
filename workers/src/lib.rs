use std::thread;
use std::sync::{mpsc, Arc, Mutex};

// FnBox idea is taken from the rust book
trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Job = Box<FnBox + Send + 'static>;

pub struct WorkersPool {
    tx: mpsc::Sender<Job>,
}

impl WorkersPool {
    pub fn new(count: usize) -> WorkersPool {
        let (tx, rx) = mpsc::channel::<Job>();
        let chan = Arc::new(Mutex::new(rx));

        for _ in 1..count {
            let rx = chan.clone();

            thread::spawn(move || loop {
                let f = rx.lock().unwrap().recv().unwrap();
                f.call_box();
            });
        }

        WorkersPool { tx: tx }
    }

    pub fn exec<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.tx.send(job).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;

    #[test]
    fn exec() {
        let p = WorkersPool::new(2);
        let counter = Arc::new(Mutex::new(0));

        for i in 1..5 {
            let c = counter.clone();
            p.exec(move || {
                let mut n = c.lock().unwrap();
                *n += i;
            });
        }

        thread::sleep(time::Duration::from_secs(2));

        let c = counter.clone();
        let n = c.lock().unwrap();
        assert_eq!(10, *n);
    }
}
