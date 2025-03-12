use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
pub struct FastLock<T> {
    lock: AtomicBool,
    value: UnsafeCell<T>,
}

pub struct FastLockGuard<'a, T> {
    lock: &'a FastLock<T>,
}

impl<'a, T> FastLockGuard<'a, T> {
    pub fn get_mut(&self) -> &mut T {
        self.lock.get_mut()
    }
}

impl<'a, T> Drop for FastLockGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

unsafe impl<T: Send> Send for FastLock<T> {}
unsafe impl<T> Sync for FastLock<T> {}

impl<T> FastLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            lock: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> FastLockGuard<T> {
        while self.lock.compare_and_swap(false, true, Ordering::Acquire) {
            std::thread::yield_now();
        }
        FastLockGuard { lock: self }
    }

    pub fn unlock(&self) {
        assert!(self.lock.compare_and_swap(true, false, Ordering::Acquire));
    }

    pub fn get_mut(&self) -> &mut T {
        unsafe { self.value.get().as_mut().unwrap() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::FastLock;

    #[test]
    fn test_fast_lock_multiple_thread_sum() {
        let lock = Arc::new(FastLock::new(0));
        let mut threads = vec![];
        const NTHREADS: usize = 1000;
        for _ in 0..NTHREADS {
            let lock = lock.clone();
            threads.push(std::thread::spawn(move || {
                lock.lock();
                let value = lock.get_mut();
                *value += 1;
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(*lock.get_mut(), NTHREADS);
    }
}
