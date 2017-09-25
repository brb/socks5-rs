use std::thread;
use std::sync::{mpsc, Mutex, Arc};

pub mod thc;

// why would you use Box(...) ?
pub struct WorkersPool<T> {
    tx: mpsc::Sender<T>,
}

impl<T> WorkersPool<T> where T: FnOnce() + Send + 'static {
    pub fn new(count: usize) -> WorkersPool<T> {
        let (tx, rx) = mpsc::channel::<T>();
        let chan = Arc::new(Mutex::new(rx));

        for _ in 1..count {
            let rx = chan.clone();

            thread::spawn(move || {
                let f = rx.lock().unwrap().recv().unwrap();
                f();
            });

        }

        WorkersPool {tx: tx }
    }

    pub fn exec(&self, f: T) {
        self.tx.send(f).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;

    #[test]
    fn exec() {
        let p = WorkersPool::new(5);
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
