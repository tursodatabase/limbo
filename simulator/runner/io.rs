use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use limbo_core::{Clock, Instant, OpenFlags, PlatformIO, Result, IO};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::runner::file::SimulatorFile;

pub(crate) struct SimulatorIO {
    pub(crate) inner: Box<dyn IO>,
    pub(crate) fault: Cell<bool>,
    pub(crate) files: RefCell<Vec<Arc<SimulatorFile>>>,
    pub(crate) rng: RefCell<ChaCha8Rng>,
    pub(crate) nr_run_once_faults: Cell<usize>,
    pub(crate) page_size: usize,
    seed: u64,
    latency_probability: usize,
}

unsafe impl Send for SimulatorIO {}
unsafe impl Sync for SimulatorIO {}

impl SimulatorIO {
    pub(crate) fn new(seed: u64, page_size: usize, latency_probability: usize) -> Result<Self> {
        let inner = Box::new(PlatformIO::new()?);
        let fault = Cell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = Cell::new(0);
        Ok(Self {
            inner,
            fault,
            files,
            rng,
            nr_run_once_faults,
            page_size,
            seed,
            latency_probability,
        })
    }

    pub(crate) fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        for file in self.files.borrow().iter() {
            file.inject_fault(fault);
        }
    }

    pub(crate) fn print_stats(&self) {
        tracing::info!("run_once faults: {}", self.nr_run_once_faults.get());
        for file in self.files.borrow().iter() {
            tracing::info!("\n===========================\n{}", file.stats_table());
        }
    }
}

impl Clock for SimulatorIO {
    fn now(&self) -> Instant {
        Instant {
            secs: 1704067200, // 2024-01-01 00:00:00 UTC
            micros: 0,
        }
    }
}

impl IO for SimulatorIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> Result<Arc<dyn limbo_core::File>> {
        let inner = self.inner.open_file(path, flags, false)?;
        let file = Arc::new(SimulatorFile {
            inner,
            fault: Cell::new(false),
            nr_pread_faults: Cell::new(0),
            nr_pwrite_faults: Cell::new(0),
            nr_pread_calls: Cell::new(0),
            nr_pwrite_calls: Cell::new(0),
            nr_sync_calls: Cell::new(0),
            page_size: self.page_size,
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(self.seed)),
            latency_probability: self.latency_probability,
        });
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn wait_for_completion(&self, c: Arc<limbo_core::Completion>) -> Result<()> {
        while !c.is_completed() {
            self.run_once()?;
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        if self.fault.get() {
            self.nr_run_once_faults
                .replace(self.nr_run_once_faults.get() + 1);
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.run_once()?;
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }

    fn get_memory_io(&self) -> Arc<limbo_core::MemoryIO> {
        Arc::new(limbo_core::MemoryIO::new())
    }
}
