use std::{
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;

use rayon::prelude::ParallelSliceMut;
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    amino_acid::TRYPTOPHAN, database_build::IsProteinAccess, mass::to_float as mass_to_float,
    molecules::WATER_MONO_MASS, protease::Protease, protein::Protein,
};

pub static PARTIAL_PROGRESS_METRIC: &str = "mass_index::partial_progress::processed_proteins";

/// Per digest-worker pair buffer before it is flushed to the collector (~100 MB).
static LOCAL_MASS_LIMIT: usize = 8_333_333; // 100 MB for i64 + i32

/// Associations per compressed `pids` block. Independent of (but chosen to mirror) the columnar
/// stripe size so a block is a natural unit; a worker decompresses only the blocks its claim spans.
const PID_BLOCK_ASSOCIATIONS: usize = 150_000;
/// zstd level for the on-disk `protein_ids` blocks — read back during the digestion-bound stage 3,
/// so a fast level is plenty; the win is fewer disk bytes.
const ZSTD_PID_LEVEL: i32 = 3;

/// Generic margin biasing the bucket count toward more/smaller buckets so estimate error pushes
/// buckets *under* the sort budget rather than over. Not protease-specific.
const BUCKET_MARGIN: f64 = 1.3;
/// Cap on bucket count so the scatter write-buffers (`K × flush`) stay bounded. Does NOT bound
/// total data: a dataset larger than `MAX_BUCKETS × budget` is handled by the sub-spill guard.
/// It is not an FD limit either — the collector opens at most one bucket file at a time.
const MAX_BUCKETS: usize = 4096;
/// Bounds for a single bucket's scatter write-buffer (in pairs).
const MIN_FLUSH_PAIRS: usize = 4096;
const MAX_FLUSH_PAIRS: usize = 1 << 20; // 1M pairs ≈ 12 MB

/// Sub-spill guard: if a bucket exceeds the budget at finalize, re-scatter it into this many
/// finer sub-buckets, up to this recursion depth.
const MAX_SUBSPILL_DEPTH: usize = 4;
const SUBSPILL_FANOUT: usize = 16;
const SUBSPILL_FLUSH_PAIRS: usize = 1 << 18;

/// Sample sizing for the pairs-per-protein estimate (grows ×4 until the estimate stabilises).
const SAMPLE_START: usize = 2048;
const ESTIMATE_TOLERANCE: f64 = 0.05;

fn max_peptide_mass(max_length: NonZeroUsize) -> i64 {
    WATER_MONO_MASS + TRYPTOPHAN.mono_mass() * max_length.get() as i64
}

/// A `(mass, protein_id)` pair packed into 12 bytes (no alignment padding).
///
/// A plain `(i64, i32)` is 16 bytes — the `i64` forces 8-byte alignment, wasting 4 bytes
/// per pair. With billions of pairs that 33% is real memory and real disk. The mass is split
/// into two `u32` halves so every field is `i32`-sized and the struct keeps a 4-byte alignment.
/// The zerocopy derives let the scatter/spill code read and write the packed bytes directly.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct MassPidPair {
    mass_lo: u32,
    mass_hi: u32,
    pid: i32,
}

const _: () = {
    assert!(std::mem::size_of::<MassPidPair>() == 3 * std::mem::size_of::<i32>());
    assert!(std::mem::align_of::<MassPidPair>() == std::mem::align_of::<i32>());
};

impl MassPidPair {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    #[inline]
    fn new(mass: i64, pid: i32) -> Self {
        let mass = mass as u64;
        Self {
            mass_lo: mass as u32,
            mass_hi: (mass >> 32) as u32,
            pid,
        }
    }

    #[inline]
    fn mass(&self) -> i64 {
        (((self.mass_hi as u64) << 32) | self.mass_lo as u64) as i64
    }

    #[inline]
    fn pid(&self) -> i32 {
        self.pid
    }
}

impl Eq for MassPidPair {}

impl Hash for MassPidPair {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mass().hash(state);
        self.pid().hash(state);
    }
}

impl PartialEq for MassPidPair {
    fn eq(&self, other: &Self) -> bool {
        self.mass() == other.mass() && self.pid() == other.pid()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error in mass index: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to join task: {0}")]
    Join(String),
    #[error("Protein ID missing")]
    MissingProteinId,
    #[error("Unable to build rayon thread pool for sorting: {0}")]
    SortThreadPool(rayon::ThreadPoolBuildError),
    #[error("No errored thread found in mass index, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in mass index: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in mass index: {0}")]
    ProteinAccess(#[from] crate::database_build::Error),
}

type PairQueue = Arc<ArrayQueue<Option<Vec<MassPidPair>>>>;

fn bucket_path(dir: &Path, bucket: usize) -> PathBuf {
    dir.join(format!("bucket_{bucket}.bin"))
}

fn pids_path(dir: &Path) -> PathBuf {
    dir.join("pids.zst")
}

/// Append packed pairs to a bucket file, opening and closing it per call. This keeps the scatter's
/// open file-descriptor count at ~1 regardless of how many buckets exist — bucket counts can reach
/// thousands on low-memory builds and must never exhaust the process FD limit. Flushes are chunky
/// (>= `scatter_flush_pairs`), so the open/close overhead is amortised.
fn append_pairs(path: &Path, pairs: &[MassPidPair]) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(pairs.as_bytes())?;
    Ok(())
}

/// Decode native-endian `i32`s out of a freshly-decompressed byte buffer (1-byte aligned, so a
/// zero-copy reinterpret is not sound — copy through `from_ne_bytes`).
fn bytes_to_i32_vec(bytes: &[u8]) -> Vec<i32> {
    bytes
        .chunks_exact(4)
        .map(|c| i32::from_ne_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Read every `MassPidPair` out of a bucket file into memory. Only called on buckets that fit the
/// budget (oversized buckets are handled by `restream_into_subbuckets` without a full load).
fn read_pairs(path: &Path) -> Result<Vec<MassPidPair>, Error> {
    let bytes = std::fs::read(path)?;
    let n = bytes.len() / MassPidPair::SIZE;
    let mut pairs = Vec::with_capacity(n);
    for chunk in bytes.chunks_exact(MassPidPair::SIZE) {
        pairs.push(MassPidPair::read_from_bytes(chunk).expect("12-byte chunk is a MassPidPair"));
    }
    Ok(pairs)
}

// ---------------------------------------------------------------------------------------------
// On-disk protein-ids store: a single block-framed zstd file. Stage 3 streams it forward; the
// in-RAM block directory maps a block index to its byte offset so a worker can seek to the block
// containing the first association of its claim.
// ---------------------------------------------------------------------------------------------

struct PidStore {
    path: PathBuf,
    /// Byte offset of each block, length `num_blocks + 1` (the trailing entry is EOF).
    block_dir: Vec<u64>,
    /// Associations per block (uniform except the last). Injectable so tests can force many blocks.
    block_assoc: usize,
}

impl PidStore {
    /// Open a streaming reader positioned at association index `assoc`.
    fn reader_at(&self, assoc: u64) -> Result<PidBlockReader<'_>, Error> {
        let block = (assoc / self.block_assoc as u64) as usize;
        let mut reader = PidBlockReader {
            file: File::open(&self.path)?,
            block_dir: &self.block_dir,
            cur_block: block,
            buf: Vec::new(),
            pos: 0,
        };
        reader.load_block(block)?;
        reader.pos = (assoc - block as u64 * self.block_assoc as u64) as usize;
        Ok(reader)
    }

    fn metadata_bytes(&self) -> usize {
        self.block_dir.capacity() * std::mem::size_of::<u64>()
    }
}

struct PidStoreWriter {
    file: BufWriter<File>,
    path: PathBuf,
    block_dir: Vec<u64>,
    block_buf: Vec<i32>,
    bytes_written: u64,
    block_assoc: usize,
}

impl PidStoreWriter {
    fn create(path: PathBuf, block_assoc: usize) -> Result<Self, Error> {
        let file = BufWriter::new(File::create(&path)?);
        Ok(Self {
            file,
            path,
            block_dir: vec![0],
            block_buf: Vec::with_capacity(block_assoc),
            bytes_written: 0,
            block_assoc,
        })
    }

    fn push(&mut self, pid: i32) -> Result<(), Error> {
        self.block_buf.push(pid);
        if self.block_buf.len() >= self.block_assoc {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), Error> {
        if self.block_buf.is_empty() {
            return Ok(());
        }
        let compressed = zstd::encode_all(self.block_buf.as_bytes(), ZSTD_PID_LEVEL)?;
        self.file.write_all(&compressed)?;
        self.bytes_written += compressed.len() as u64;
        self.block_dir.push(self.bytes_written);
        self.block_buf.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<PidStore, Error> {
        self.flush_block()?;
        self.file.flush()?;
        Ok(PidStore {
            path: self.path,
            block_dir: self.block_dir,
            block_assoc: self.block_assoc,
        })
    }
}

struct PidBlockReader<'a> {
    file: File,
    block_dir: &'a [u64],
    cur_block: usize,
    buf: Vec<i32>,
    pos: usize,
}

impl PidBlockReader<'_> {
    fn load_block(&mut self, block: usize) -> Result<(), Error> {
        let start = self.block_dir[block];
        let end = self.block_dir[block + 1];
        self.file.seek(SeekFrom::Start(start))?;
        let mut compressed = vec![0u8; (end - start) as usize];
        self.file.read_exact(&mut compressed)?;
        let raw = zstd::decode_all(&compressed[..])?;
        self.buf = bytes_to_i32_vec(&raw);
        self.cur_block = block;
        self.pos = 0;
        Ok(())
    }

    /// Read the next `n` protein ids, crossing block boundaries as needed.
    fn read_n(&mut self, n: usize) -> Result<Vec<i32>, Error> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            if self.pos >= self.buf.len() {
                self.load_block(self.cur_block + 1)?;
            }
            let take = (n - out.len()).min(self.buf.len() - self.pos);
            out.extend_from_slice(&self.buf[self.pos..self.pos + take]);
            self.pos += take;
        }
        Ok(out)
    }
}

/// A forward, streaming view over a claimed global range `[start, end)`. Yields `(mass, ids)` in
/// ascending mass order, reading the `protein_ids` blocks from disk on demand.
pub struct MassIndexReader<'a> {
    masses: &'a [i64],
    indptr: &'a [u64],
    g: usize,
    end: usize,
    pids: PidBlockReader<'a>,
}

impl MassIndexReader<'_> {
    /// Next `(mass, protein_ids)` in the claim, or `None` when the range is exhausted.
    pub fn next_entry(&mut self) -> Result<Option<(i64, Vec<i32>)>, Error> {
        if self.g >= self.end {
            return Ok(None);
        }
        let mass = self.masses[self.g];
        let n = (self.indptr[self.g + 1] - self.indptr[self.g]) as usize;
        let ids = self.pids.read_n(n)?;
        self.g += 1;
        Ok(Some((mass, ids)))
    }
}

/// Disk-backed mass index. The metadata (`masses`, cumulative `indptr`) lives in RAM as a single
/// global, ascending run; the `protein_ids` array lives in a block-framed zstd file on disk and is
/// streamed in stage 3. The whole scratch tree is removed when this drops (success or error).
pub struct MassIndex {
    masses: Vec<i64>,
    indptr: Vec<u64>,
    pids: PidStore,
    // Owns the scratch directory holding `pids.zst`; dropping it deletes the tree.
    _scratch: tempfile::TempDir,
}

impl MassIndex {
    pub fn is_empty(&self) -> bool {
        self.masses.is_empty()
    }

    /// Number of distinct masses.
    pub fn len(&self) -> usize {
        self.masses.len()
    }

    pub fn num_protein_associations(&self) -> usize {
        self.indptr.last().copied().unwrap_or(0) as usize
    }

    /// In-RAM footprint of the index metadata (the `protein_ids` array is on disk).
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.masses.capacity() * std::mem::size_of::<i64>()
            + self.indptr.capacity() * std::mem::size_of::<u64>()
            + self.pids.metadata_bytes()
    }

    /// Smallest global mass index strictly greater than `start` whose masses (from `start`) carry at
    /// least `target_associations` associations, clamped to the end of the index.
    ///
    /// Build workers claim contiguous chunks `[start, claim_end(start))` so each chunk does roughly
    /// equal *work* — digestion cost is ~proportional to associations, not mass count. The index is
    /// a single global run, so claims span freely (no per-part clamping). Always advances by at
    /// least one mass so the claim cursor makes progress.
    pub fn claim_end(&self, start: usize, target_associations: u64) -> usize {
        let goal = self.indptr[start] + target_associations;
        self.indptr
            .partition_point(|&cumulative| cumulative < goal)
            .clamp(start + 1, self.masses.len())
    }

    /// Open a streaming reader over the claimed global range `[start, end)`.
    pub fn claim_reader(&self, start: usize, end: usize) -> Result<MassIndexReader<'_>, Error> {
        let pids = self.pids.reader_at(self.indptr[start])?;
        Ok(MassIndexReader {
            masses: &self.masses,
            indptr: &self.indptr,
            g: start,
            end,
            pids,
        })
    }

    /// Build the disk-backed mass index in a single digestion pass.
    ///
    /// 1. Estimate pairs-per-protein from a protease-agnostic sample to size the buckets.
    /// 2. Scatter every `(mass, pid)` pair (one cleave per protein) into per-mass-range disk
    ///    buckets sized to fit `memory_budget_bytes` when sorted.
    /// 3. Finalize buckets in ascending mass order: sort, dedup, append ids to the on-disk store,
    ///    and accumulate the global `masses`/`indptr` metadata. Oversized buckets sub-spill.
    pub async fn build(
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        protease: Arc<Protease>,
        num_threads: NonZeroUsize,
        scratch_dir: PathBuf,
        memory_budget_bytes: usize,
    ) -> Result<Self, Error> {
        std::fs::create_dir_all(&scratch_dir)?;
        let scratch = tempfile::Builder::new()
            .prefix("macpepdb-massindex-")
            .tempdir_in(&scratch_dir)?;
        let dir = scratch.path().to_path_buf();

        let max_mass = max_peptide_mass(protease.max_length());
        let protein_count = protein_access.count().await?;

        // --- size the buckets ---
        let pairs_per_protein = estimate_pairs_per_protein(&protein_access, &protease).await?;
        let est_total_pairs = (pairs_per_protein * protein_count as f64).ceil().max(1.0) as u64;
        let budget_pairs = (memory_budget_bytes / MassPidPair::SIZE).max(1) as u64;
        let target_buckets =
            ((est_total_pairs as f64 * BUCKET_MARGIN) / budget_pairs as f64).ceil() as usize;
        let target_buckets = target_buckets.clamp(1, MAX_BUCKETS);
        let bucket_width = (max_mass / target_buckets as i64).max(1);
        let num_buckets = (max_mass / bucket_width + 1) as usize;
        let scatter_flush_pairs =
            (budget_pairs as usize / num_buckets).clamp(MIN_FLUSH_PAIRS, MAX_FLUSH_PAIRS);

        tracing::info!(
            "Mass index: ~{:.1} pairs/protein, est {} pairs, budget {} pairs/bucket, {} buckets (width {:.4} Da)",
            pairs_per_protein,
            est_total_pairs,
            budget_pairs,
            num_buckets,
            mass_to_float(bucket_width)
        );

        // --- scatter (single digestion pass) ---
        let now = std::time::Instant::now();
        scatter(
            protein_access,
            protease,
            num_threads,
            dir.clone(),
            bucket_width,
            num_buckets,
            scatter_flush_pairs,
        )
        .await?;
        tracing::info!(
            "Mass index scatter done in {:.2?}s",
            now.elapsed().as_secs_f64()
        );

        // --- finalize buckets into the global metadata + on-disk pids ---
        let now = std::time::Instant::now();
        let sort_pool = rayon::ThreadPoolBuilder::new()
            .stack_size(512 << 20)
            .build()
            .map_err(Error::SortThreadPool)?;
        let mut masses: Vec<i64> = Vec::new();
        let mut indptr: Vec<u64> = vec![0u64];
        let mut pid_writer = PidStoreWriter::create(pids_path(&dir), PID_BLOCK_ASSOCIATIONS)?;
        let mut subspill_count = 0usize;

        for bucket in 0..num_buckets {
            let lo = bucket as i64 * bucket_width;
            let hi = (bucket as i64 + 1) * bucket_width;
            finalize_bucket(
                bucket_path(&dir, bucket),
                (lo, hi),
                budget_pairs,
                0,
                &sort_pool,
                &mut masses,
                &mut indptr,
                &mut pid_writer,
                &mut subspill_count,
            )?;
        }

        let pids = pid_writer.finish()?;
        masses.shrink_to_fit();
        indptr.shrink_to_fit();

        if subspill_count > 0 {
            tracing::warn!(
                "Mass index sub-spill fired on {} bucket(s): the pairs estimate under-provisioned bucket sizes. Correctness is unaffected; a larger memory budget would avoid the extra disk passes.",
                subspill_count
            );
        }
        tracing::info!(
            "Mass index finalize done in {:.2?}s; {} masses, {} associations",
            now.elapsed().as_secs_f64(),
            masses.len(),
            indptr.last().copied().unwrap_or(0)
        );

        Ok(MassIndex {
            masses,
            indptr,
            pids,
            _scratch: scratch,
        })
    }
}

/// Single digestion pass: cleave each protein once and scatter its `(mass, pid)` pairs into
/// per-mass-range bucket files on disk. A single collector task owns all bucket writers (no
/// per-bucket lock contention); the parallel digest workers feed it through `pair_queue`.
async fn scatter(
    protein_access: Arc<Box<dyn IsProteinAccess>>,
    protease: Arc<Protease>,
    num_threads: NonZeroUsize,
    dir: PathBuf,
    bucket_width: i64,
    num_buckets: usize,
    scatter_flush_pairs: usize,
) -> Result<(), Error> {
    let protein_queue: Arc<ArrayQueue<Option<Arc<Protein>>>> =
        Arc::new(ArrayQueue::new(num_threads.get() * 3));
    let pair_queue: PairQueue = Arc::new(ArrayQueue::new(num_threads.get()));
    let partial_progress_metric = Arc::new(metrics::counter!(PARTIAL_PROGRESS_METRIC));
    partial_progress_metric.absolute(0);

    let collector_task = {
        let pair_queue = pair_queue.clone();
        tokio::spawn(async move {
            // Buffer per bucket in RAM (total bounded by the budget); flush via open/append/close
            // so open FDs stay ~1 regardless of bucket count. A bucket that receives no pairs is
            // simply never created — finalize treats a missing file as empty.
            let mut buffers: Vec<Vec<MassPidPair>> = (0..num_buckets).map(|_| Vec::new()).collect();

            loop {
                let batch = match pair_queue.pop() {
                    Some(Some(batch)) => batch,
                    Some(None) => break,
                    None => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };

                for pair in batch {
                    let idx = ((pair.mass() / bucket_width) as usize).min(num_buckets - 1);
                    let buf = &mut buffers[idx];
                    buf.push(pair);
                    if buf.len() >= scatter_flush_pairs {
                        append_pairs(&bucket_path(&dir, idx), buf)?;
                        buf.clear();
                    }
                }
            }

            for (idx, buf) in buffers.iter().enumerate() {
                if !buf.is_empty() {
                    append_pairs(&bucket_path(&dir, idx), buf)?;
                }
            }
            Ok::<_, Error>(())
        })
    };

    let mut proteins = protein_access.all().await?;

    let digest_threads = (0..num_threads.get())
        .map(|_| {
            let protease = protease.clone();
            let protein_queue = protein_queue.clone();
            let partial_progress_metric = partial_progress_metric.clone();
            let pair_queue = pair_queue.clone();

            tokio::spawn(async move {
                let mut local_pairs: Vec<MassPidPair> = Vec::with_capacity(LOCAL_MASS_LIMIT);

                loop {
                    let protein = match protein_queue.pop() {
                        Some(Some(protein)) => protein,
                        Some(None) => break,
                        None => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };

                    let protein_id = protein.id().ok_or(Error::MissingProteinId)?;

                    protease
                        .cleave_masses_only(protein.sequence().as_ref())
                        .for_each(|mass| {
                            local_pairs.push(MassPidPair::new(mass, protein_id));
                            Ok(())
                        })?;

                    if local_pairs.len() >= LOCAL_MASS_LIMIT {
                        let mut batch = Some(std::mem::replace(
                            &mut local_pairs,
                            Vec::with_capacity(LOCAL_MASS_LIMIT),
                        ));
                        loop {
                            batch = match pair_queue.push(batch) {
                                Ok(()) => break,
                                Err(errored_batch) => {
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                    errored_batch
                                }
                            };
                        }
                    }

                    partial_progress_metric.increment(1);
                }

                let mut batch = Some(local_pairs);
                loop {
                    batch = match pair_queue.push(batch) {
                        Ok(()) => break,
                        Err(errored_batch) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            errored_batch
                        }
                    };
                }

                Ok::<_, Error>(())
            })
        })
        .collect::<Vec<_>>();

    while let Some(protein) = proteins.next().await.transpose()? {
        let mut protein = Some(protein);
        loop {
            protein = match protein_queue.push(protein) {
                Ok(()) => break,
                Err(entry) => {
                    if digest_threads.iter().any(|thread| thread.is_finished()) {
                        return Err(find_errored_thread(digest_threads).await);
                    }
                    entry
                }
            };
        }
    }

    // Signal stop to digest workers.
    for _ in 0..num_threads.get() {
        loop {
            if protein_queue.push(None).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    for thread in digest_threads {
        thread.await.map_err(|err| Error::Join(err.to_string()))??;
    }

    // Signal stop to the collector.
    loop {
        match pair_queue.push(None) {
            Ok(()) => break,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
    }

    collector_task
        .await
        .map_err(|err| Error::Join(err.to_string()))??;

    Ok(())
}

/// Finalize one bucket file into the global metadata + on-disk pid store. If the bucket is larger
/// than the sort budget, re-scatter it into finer sub-buckets over its mass range and recurse
/// (the sub-spill guard) instead of loading it whole.
#[allow(clippy::too_many_arguments)]
fn finalize_bucket(
    path: PathBuf,
    range: (i64, i64),
    budget_pairs: u64,
    depth: usize,
    sort_pool: &rayon::ThreadPool,
    masses: &mut Vec<i64>,
    indptr: &mut Vec<u64>,
    pid_writer: &mut PidStoreWriter,
    subspill_count: &mut usize,
) -> Result<(), Error> {
    let (lo, hi) = range;
    // A bucket that received no pairs is never created by the scatter collector.
    let n_pairs = match std::fs::metadata(&path) {
        Ok(meta) => meta.len() / MassPidPair::SIZE as u64,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    if n_pairs > budget_pairs && hi - lo > 1 && depth < MAX_SUBSPILL_DEPTH {
        *subspill_count += 1;
        let fanout = SUBSPILL_FANOUT.min((hi - lo) as usize).max(2);
        let sub_width = ((hi - lo) / fanout as i64).max(1);
        let actual = (((hi - lo) - 1) / sub_width + 1) as usize;
        let sub_paths = restream_into_subbuckets(&path, lo, sub_width, actual)?;
        std::fs::remove_file(&path)?;

        for (s, sub_path) in sub_paths.into_iter().enumerate() {
            let slo = lo + s as i64 * sub_width;
            let shi = (lo + (s as i64 + 1) * sub_width).min(hi);
            finalize_bucket(
                sub_path,
                (slo, shi),
                budget_pairs,
                depth + 1,
                sort_pool,
                masses,
                indptr,
                pid_writer,
                subspill_count,
            )?;
        }
        return Ok(());
    }

    let mut pairs = read_pairs(&path)?;
    std::fs::remove_file(&path)?;
    if pairs.is_empty() {
        return Ok(());
    }

    sort_pool.install(|| {
        pairs.par_sort_unstable_by(|a, b| {
            a.mass().cmp(&b.mass()).then_with(|| a.pid().cmp(&b.pid()))
        })
    });
    pairs.dedup();

    for pair in &pairs {
        let mass = pair.mass();
        if masses.last() != Some(&mass) {
            masses.push(mass);
            indptr.push(*indptr.last().unwrap());
        }
        *indptr.last_mut().unwrap() += 1;
        pid_writer.push(pair.pid())?;
    }

    Ok(())
}

/// Stream an oversized bucket file and re-scatter its pairs into `actual` finer sub-bucket files
/// over `[lo, lo + actual*sub_width)`, without loading the whole file into RAM.
fn restream_into_subbuckets(
    path: &Path,
    lo: i64,
    sub_width: i64,
    actual: usize,
) -> Result<Vec<PathBuf>, Error> {
    let dir = path.parent().expect("bucket file has a parent dir");
    let stem = path
        .file_stem()
        .expect("bucket file has a stem")
        .to_string_lossy();
    let sub_paths: Vec<PathBuf> = (0..actual)
        .map(|s| dir.join(format!("{stem}_s{s}.bin")))
        .collect();

    let mut writers: Vec<BufWriter<File>> = Vec::with_capacity(actual);
    for sub_path in &sub_paths {
        writers.push(BufWriter::new(File::create(sub_path)?));
    }
    let mut buffers: Vec<Vec<MassPidPair>> = (0..actual).map(|_| Vec::new()).collect();

    let mut reader = BufReader::new(File::open(path)?);
    let mut bytes = [0u8; MassPidPair::SIZE];
    loop {
        match reader.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
        let pair = MassPidPair::read_from_bytes(&bytes).expect("12-byte chunk is a MassPidPair");
        let s = (((pair.mass() - lo) / sub_width) as usize).min(actual - 1);
        let buf = &mut buffers[s];
        buf.push(pair);
        if buf.len() >= SUBSPILL_FLUSH_PAIRS {
            writers[s].write_all(buf.as_bytes())?;
            buf.clear();
        }
    }

    for (writer, buf) in writers.iter_mut().zip(buffers.iter()) {
        if !buf.is_empty() {
            writer.write_all(buf.as_bytes())?;
        }
        writer.flush()?;
    }

    Ok(sub_paths)
}

/// Estimate the average number of `(mass, pid)` pairs a protein produces under the *configured*
/// protease, by digesting a growing random sample until the estimate stabilises. Protease-agnostic
/// (it runs the real cleave), so it is correct for tryptic, semi-tryptic, unspecific, and any
/// future protease.
async fn estimate_pairs_per_protein(
    protein_access: &Arc<Box<dyn IsProteinAccess>>,
    protease: &Arc<Protease>,
) -> Result<f64, Error> {
    let ids = protein_access.ids().await?;
    let total = ids.len();
    if total == 0 {
        return Ok(0.0);
    }

    let mut sample_size = SAMPLE_START.min(total);
    let mut prev: Option<f64> = None;
    loop {
        // Stride through the id space so the sample spans the whole proteome.
        let stride = (total / sample_size).max(1);
        let sample_ids: Vec<i32> = ids
            .iter()
            .copied()
            .step_by(stride)
            .take(sample_size)
            .collect();

        let mut proteins = protein_access.by_ids(&sample_ids).await?;
        let mut pairs = 0u64;
        let mut n = 0u64;
        while let Some(protein) = proteins.next().await.transpose()? {
            pairs += protease
                .cleave_masses_only(protein.sequence().as_ref())
                .count()? as u64;
            n += 1;
        }
        if n == 0 {
            return Ok(0.0);
        }

        let est = pairs as f64 / n as f64;
        let stable = prev.is_some_and(|pv| (est - pv).abs() <= ESTIMATE_TOLERANCE * pv);
        if stable || sample_size >= total {
            return Ok(est);
        }
        prev = Some(est);
        sample_size = (sample_size * 4).min(total);
    }
}

async fn find_errored_thread(threads: Vec<tokio::task::JoinHandle<Result<(), Error>>>) -> Error {
    for thread in threads {
        if thread.is_finished() {
            match thread.await {
                Ok(Ok(())) => continue,
                Ok(Err(err)) => return err,
                Err(_err) => continue,
            }
        }
    }
    Error::NoErroredThread
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroUsize,
        sync::Arc,
    };

    use fallible_iterator::FallibleIterator;
    use futures::StreamExt;
    use uniprot_reader::asynchronous::reader::AsyncReader;

    use crate::{
        database_build::{InMemoryProteinAccess, IsProteinAccess},
        protease::Protease,
        protein::Protein,
    };

    use super::*;

    async fn load_fixture_proteins() -> Vec<Protein> {
        let proteins_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("some_human_proteins.uniprot.txt");

        let mut buf_reader =
            tokio::io::BufReader::new(tokio::fs::File::open(proteins_file).await.unwrap());
        let reader = AsyncReader::new(&mut buf_reader);
        reader
            .enumerate()
            .map(|(protein_id, entry)| {
                Protein::try_from((protein_id as i32, entry.unwrap().entry())).unwrap()
            })
            .collect::<Vec<_>>()
            .await
    }

    fn trypsin() -> Protease {
        Protease::by_name(
            "trypsin",
            Some(NonZeroUsize::new(6).unwrap()),
            Some(NonZeroUsize::new(50).unwrap()),
            Some(2),
            false,
        )
        .unwrap()
    }

    fn manual_index(proteins: &[Protein], protease: &Protease) -> HashMap<i64, HashSet<i32>> {
        let mut manual: HashMap<i64, HashSet<i32>> = HashMap::new();
        for protein in proteins {
            let protein_id = protein.id().unwrap();
            protease
                .cleave_masses_only(protein.sequence().as_ref())
                .for_each(|mass| {
                    manual.entry(mass).or_default().insert(protein_id);
                    Ok(())
                })
                .unwrap();
        }
        manual
    }

    /// Exercise the block-framed pid store directly with a tiny block size so reads cross many
    /// block boundaries and start mid-block — the path the fixture-driven tests can't reach
    /// (the fixture has far fewer than `PID_BLOCK_ASSOCIATIONS` associations, i.e. one block).
    #[test]
    fn test_pid_store_multi_block_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pids.zst");

        // 1000 ids, blocks of 7 -> ~143 blocks; varied values incl. negatives.
        let ids: Vec<i32> = (0..1000).map(|i| (i as i32 * 7) - 1500).collect();
        let mut writer = PidStoreWriter::create(path, 7).unwrap();
        for &id in &ids {
            writer.push(id).unwrap();
        }
        let store = writer.finish().unwrap();

        // Read from several association offsets, each spanning many blocks, and verify contents.
        for &start in &[0usize, 1, 6, 7, 8, 13, 500, 993] {
            let mut reader = store.reader_at(start as u64).unwrap();
            let got = reader.read_n(ids.len() - start).unwrap();
            assert_eq!(got, ids[start..], "mismatch reading from offset {start}");
        }

        // Many small reads in sequence must also reconstruct the full stream across blocks.
        let mut reader = store.reader_at(0).unwrap();
        let mut acc = Vec::new();
        for chunk in [3usize, 4, 5, 9, 100, 1, 878] {
            acc.extend(reader.read_n(chunk).unwrap());
        }
        assert_eq!(acc, ids);
    }

    /// Build via the disk-backed pipeline and verify every `(mass, ids)` streamed back matches a
    /// directly-computed reference index. A generous budget keeps this to a single bucket.
    #[tokio::test]
    async fn test_mass_index_single_bucket() {
        let proteins = load_fixture_proteins().await;
        let protease = trypsin();
        let manual = manual_index(&proteins, &protease);

        let protein_access: Arc<Box<dyn IsProteinAccess>> = Arc::new(Box::new(
            InMemoryProteinAccess::with_proteins(proteins.into_iter()),
        ));

        let mass_index = MassIndex::build(
            protein_access,
            Arc::new(protease),
            NonZeroUsize::new(4).unwrap(),
            std::env::temp_dir(),
            256 * 1024 * 1024,
        )
        .await
        .unwrap();

        let total = mass_index.len();
        assert_eq!(total, manual.len(), "distinct mass count must match");

        let mut reader = mass_index.claim_reader(0, total).unwrap();
        let mut seen = 0usize;
        while let Some((mass, ids)) = reader.next_entry().unwrap() {
            let expected = manual.get(&mass).unwrap();
            assert_eq!(expected.len(), ids.len(), "mass {mass}: id count");
            for id in &ids {
                assert!(expected.contains(id), "mass {mass}: unexpected id {id}");
            }
            seen += 1;
        }
        assert_eq!(seen, total);
    }

    /// A tiny budget forces many small buckets (and likely sub-spills). Verify the concatenated
    /// index is still globally ascending, ids match, and `claim_end` tiles `[0, total)` exactly —
    /// the invariants the disjoint-contiguous-partition build relies on.
    #[tokio::test]
    async fn test_mass_index_multi_bucket_and_claims() {
        let proteins = load_fixture_proteins().await;
        let protease = trypsin();
        let manual = manual_index(&proteins, &protease);

        let protein_access: Arc<Box<dyn IsProteinAccess>> = Arc::new(Box::new(
            InMemoryProteinAccess::with_proteins(proteins.into_iter()),
        ));

        // ~341 pairs per bucket -> many buckets for the fixture.
        let mass_index = MassIndex::build(
            protein_access,
            Arc::new(protease),
            NonZeroUsize::new(4).unwrap(),
            std::env::temp_dir(),
            4096,
        )
        .await
        .unwrap();

        let total = mass_index.len();
        assert!(total > 0, "expected a non-empty index for the fixture");
        assert_eq!(total, manual.len());

        // Strictly ascending masses across all buckets, ids match the reference.
        let mut reader = mass_index.claim_reader(0, total).unwrap();
        let mut prev: Option<i64> = None;
        let mut count = 0usize;
        while let Some((mass, ids)) = reader.next_entry().unwrap() {
            if let Some(p) = prev {
                assert!(mass > p, "masses must be strictly ascending across buckets");
            }
            prev = Some(mass);
            let expected = manual.get(&mass).unwrap();
            assert_eq!(ids.len(), expected.len(), "mass {mass}: id count");
            for id in &ids {
                assert!(expected.contains(id), "mass {mass}: unexpected id {id}");
            }
            count += 1;
        }
        assert_eq!(count, total);

        // claim_end must tile [0, total) — contiguous, gap-free, disjoint — with a small target to
        // force many chunks; each claim's reader must reproduce the same masses.
        let target = 50u64;
        let mut pos = 0usize;
        let mut walked = 0usize;
        while pos < total {
            let end = mass_index.claim_end(pos, target);
            assert!(end > pos, "claim must advance past {pos}");
            assert!(end <= total, "claim {end} must not exceed total {total}");
            let mut chunk = mass_index.claim_reader(pos, end).unwrap();
            while let Some((mass, _ids)) = chunk.next_entry().unwrap() {
                assert!(manual.contains_key(&mass));
                walked += 1;
            }
            pos = end;
        }
        assert_eq!(pos, total, "claims must cover the whole index with no gap");
        assert_eq!(
            walked, total,
            "claim readers must cover every mass exactly once"
        );
    }
}
