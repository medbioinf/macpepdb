use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use rayon::prelude::ParallelSliceMut;
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    amino_acid::TRYPTOPHAN, database_build::IsProteinAccess, mass::to_float as mass_to_float,
    molecules::WATER_MONO_MASS, protease::Protease,
};

/// Progress of the scatter (digestion) pass: proteins cleaved, out of the total protein count.
pub static SCATTER_PROGRESS_METRIC: &str = "mass_index::scatter::processed_proteins";
/// Progress of the finalize pass: associations written to the on-disk pid store. Registered as a
/// counter (the total isn't known until finalize runs), so it shows a live count + rate while the
/// scatter bar sits at 100%.
pub static FINALIZE_PROGRESS_METRIC: &str = "mass_index::finalize::associations";

/// Per digest-worker pair buffer before it is flushed to the collector (~100 MB).
static LOCAL_MASS_LIMIT: usize = 8_333_333; // 100 MB for i64 + i32
/// Report scatter progress every N proteins rather than per protein. The metrics recorder forwards
/// every update over a channel, so a per-protein increment from 128 workers is ~71 M contended
/// cross-thread sends; batching makes it a handful per worker.
const PROGRESS_REPORT_EVERY: u64 = 8192;

/// Associations per compressed `pids` block. Independent of (but chosen to mirror) the columnar
/// stripe size so a block is a natural unit; a worker decompresses only the blocks its claim spans.
const PID_BLOCK_ASSOCIATIONS: usize = 150_000;
/// zstd level for the on-disk `protein_ids` blocks — read back during the digestion-bound stage 3,
/// so a fast level is plenty; the win is fewer disk bytes.
const ZSTD_PID_LEVEL: i32 = 1;
/// zstd level for the scatter bucket frames. Bucket files are write-once / read-once scratch and
/// the pairs compress well (within a bucket the masses span only `bucket_width`, so the `mass_hi`
/// plane is near-constant and `mass_lo` is tightly banded), so a fast level is the right trade:
/// the saved IO outweighs the CPU, which runs in parallel off the build's critical path.
const ZSTD_BUCKET_LEVEL: i32 = 1;

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
    #[error("Protease error in mass index: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in mass index: {0}")]
    ProteinAccess(#[from] crate::database_build::Error),
}

fn bucket_path(dir: &Path, bucket: usize) -> PathBuf {
    dir.join(format!("bucket_{bucket}.bin"))
}

fn pids_path(dir: &Path) -> PathBuf {
    dir.join("pids.zst")
}

/// Append already-serialized bytes (one frame) to a bucket file, opening and closing it per call.
/// This keeps the scatter's open file-descriptor count at ~1 regardless of how many buckets exist
/// — bucket counts can reach thousands on low-memory builds and must never exhaust the process FD
/// limit. Frames are chunky (>= `scatter_flush_pairs` pairs), so the open/close overhead is
/// amortised.
fn append_bytes(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(bytes)?;
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

/// In-place dedup of a `MassPidPair` slice already sorted by mass and holding a single pid (one
/// protein's run): compact the first pair of each equal-mass group to the front and return the
/// kept count. Stable equivalent of the unstable `[T]::partition_dedup_by_key`.
fn partition_dedup_by_mass(run: &mut [MassPidPair]) -> usize {
    if run.is_empty() {
        return 0;
    }
    let mut write = 0;
    for read in 1..run.len() {
        if run[read].mass() != run[write].mass() {
            write += 1;
            run[write] = run[read];
        }
    }
    write + 1
}

// ---------------------------------------------------------------------------------------------
// Bucket frames: the scatter writes each flush as one self-describing, zstd-compressed frame:
//     [u32 n_pairs][u32 comp_len][ zstd(SoA payload) ]      (all little-endian)
// The payload is struct-of-arrays — all `mass_lo`, then all `mass_hi`, then all `pid` — because
// splitting the planes is what makes the data compress: within a bucket the masses span only
// `bucket_width`, so the `mass_hi` plane is near-constant and the `mass_lo` plane is tightly
// banded, far more redundant than interleaved 12-byte records. Bucket files are write-once /
// read-once scratch, so a fast zstd level is the right trade (see `ZSTD_BUCKET_LEVEL`).
// ---------------------------------------------------------------------------------------------

/// Pack a run of pairs into the struct-of-arrays payload (uncompressed).
fn pairs_to_soa(pairs: &[MassPidPair]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pairs.len() * MassPidPair::SIZE);
    out.extend(pairs.iter().flat_map(|p| p.mass_lo.to_le_bytes()));
    out.extend(pairs.iter().flat_map(|p| p.mass_hi.to_le_bytes()));
    out.extend(pairs.iter().flat_map(|p| p.pid.to_le_bytes()));
    out
}

/// Reconstruct `n` pairs from a struct-of-arrays payload produced by `pairs_to_soa`.
fn soa_to_pairs(raw: &[u8], n: usize) -> Vec<MassPidPair> {
    let lo = &raw[0..4 * n];
    let hi = &raw[4 * n..8 * n];
    let pid = &raw[8 * n..12 * n];
    (0..n)
        .map(|i| MassPidPair {
            mass_lo: u32::from_le_bytes(lo[4 * i..4 * i + 4].try_into().unwrap()),
            mass_hi: u32::from_le_bytes(hi[4 * i..4 * i + 4].try_into().unwrap()),
            pid: i32::from_le_bytes(pid[4 * i..4 * i + 4].try_into().unwrap()),
        })
        .collect()
}

/// Compress a run of pairs into one self-describing frame (header + `zstd(SoA)`).
fn compress_frame(pairs: &[MassPidPair]) -> Result<Vec<u8>, Error> {
    let payload = pairs_to_soa(pairs);
    let compressed = zstd::encode_all(payload.as_slice(), ZSTD_BUCKET_LEVEL)?;
    let mut frame = Vec::with_capacity(8 + compressed.len());
    frame.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
    frame.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    frame.extend_from_slice(&compressed);
    Ok(frame)
}

/// Decode a block-framed bucket file frame by frame, invoking `on_pairs` with each frame's pairs.
/// Streaming (one frame resident at a time), so it works on oversized buckets that the sort budget
/// would reject loading whole.
fn stream_frames<F>(path: &Path, mut on_pairs: F) -> Result<(), Error>
where
    F: FnMut(&[MassPidPair]) -> Result<(), Error>,
{
    let mut reader = BufReader::new(File::open(path)?);
    let mut header = [0u8; 8];
    loop {
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
        let n = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let mut compressed = vec![0u8; comp_len];
        reader.read_exact(&mut compressed)?;
        let raw = zstd::decode_all(compressed.as_slice())?;
        on_pairs(&soa_to_pairs(&raw, n))?;
    }
    Ok(())
}

/// Read every `MassPidPair` out of a block-framed bucket file into memory. Only called on buckets
/// that fit the budget (oversized buckets are handled by `restream_into_subbuckets` without a full
/// load). `n_hint` is the bucket's known pair count (from the scatter's per-bucket counter), used
/// to preallocate.
fn read_pairs(path: &Path, n_hint: usize) -> Result<Vec<MassPidPair>, Error> {
    let mut pairs = Vec::with_capacity(n_hint);
    stream_frames(path, |frame| {
        pairs.extend_from_slice(frame);
        Ok(())
    })?;
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
        let bucket_counts = scatter(
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
        let finalize_metric = metrics::counter!(FINALIZE_PROGRESS_METRIC);
        finalize_metric.absolute(0);

        for (bucket, &n_pairs) in bucket_counts.iter().enumerate() {
            let lo = bucket as i64 * bucket_width;
            let hi = (bucket as i64 + 1) * bucket_width;
            finalize_bucket(
                bucket_path(&dir, bucket),
                (lo, hi),
                n_pairs,
                budget_pairs,
                0,
                &sort_pool,
                &mut masses,
                &mut indptr,
                &mut pid_writer,
                &mut subspill_count,
                &finalize_metric,
            )?;
        }

        let pids = pid_writer.finish()?;
        masses.shrink_to_fit();
        indptr.shrink_to_fit();

        if subspill_count > 0 {
            tracing::warn!(
                "Mass index sub-spill fired on {} bucket(s): mass skew made some equal-width bands exceed the sort budget (independent of estimate accuracy). Correctness is unaffected; finer initial bands (a larger BUCKET_MARGIN) would reduce the extra disk passes.",
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
/// per-mass-range bucket files on disk, returning the per-bucket pair count.
///
/// Dataflow: parallel digest workers -> one collector (mpsc, N producers -> 1 consumer) that owns
/// the single bounded set of per-bucket buffers and does only cheap per-pair routing. On flush the
/// collector offloads compression to a bounded pool of blocking tasks (CPU on otherwise-idle cores
/// during this IO-bound pass), each of which forwards its compressed frame to a single writer task
/// that appends it. Keeping compression off the collector/writer spine means the saved IO is a net
/// win on slow disks and the single core never becomes the ceiling on fast ones. A bucket that
/// receives no pairs is never created — finalize treats a zero count as empty.
async fn scatter(
    protein_access: Arc<Box<dyn IsProteinAccess>>,
    protease: Arc<Protease>,
    num_threads: NonZeroUsize,
    dir: PathBuf,
    bucket_width: i64,
    num_buckets: usize,
    scatter_flush_pairs: usize,
) -> Result<Vec<u64>, Error> {
    let n = num_threads.get();
    let scatter_progress_metric = Arc::new(metrics::counter!(SCATTER_PROGRESS_METRIC));
    scatter_progress_metric.absolute(0);

    let ids = protein_access.ids().await?;
    let chunk_size = ids.len().div_ceil(n).max(1);

    // Writer: a single task appends compressed frames to bucket files. Serial writes (no per-bucket
    // locks); the frames are already compressed, so one writer keeps up even on fast disks. Returns
    // the total compressed bytes written for the ratio log.
    let (frame_tx, mut frame_rx) = mpsc::channel::<(usize, Vec<u8>)>(n);
    let writer = {
        let dir = dir.clone();
        tokio::spawn(async move {
            let mut compressed_bytes: u64 = 0;
            while let Some((idx, frame)) = frame_rx.recv().await {
                compressed_bytes += frame.len() as u64;
                append_bytes(&bucket_path(&dir, idx), &frame)?;
            }
            Ok::<u64, Error>(compressed_bytes)
        })
    };

    // Collector: routes pairs into the single bounded set of per-bucket buffers. The Semaphore caps
    // in-flight compressions (and therefore buffers handed off), bounding extra memory to roughly
    // `n × flush size`; the channel closing (all worker senders dropped) is the EOF signal.
    let (pair_tx, mut pair_rx) = mpsc::channel::<Vec<MassPidPair>>(n);
    let collector = {
        let frame_tx = frame_tx.clone();
        tokio::spawn(async move {
            let mut buffers: Vec<Vec<MassPidPair>> = (0..num_buckets).map(|_| Vec::new()).collect();
            let mut counts: Vec<u64> = vec![0u64; num_buckets];
            let sem = Arc::new(Semaphore::new(n));
            let mut compressors: JoinSet<Result<(), Error>> = JoinSet::new();

            // Hand a filled buffer to a blocking compressor that forwards its frame to the writer.
            // The permit (acquired by the caller) is released when the task finishes.
            let spawn_compress =
                |compressors: &mut JoinSet<Result<(), Error>>,
                 idx: usize,
                 buf: Vec<MassPidPair>,
                 permit: tokio::sync::OwnedSemaphorePermit,
                 frame_tx: mpsc::Sender<(usize, Vec<u8>)>| {
                    compressors.spawn(async move {
                        let frame = tokio::task::spawn_blocking(move || compress_frame(&buf))
                            .await
                            .map_err(|err| Error::Join(err.to_string()))??;
                        // Writer gone (it errored) -> drop the frame; its error surfaces on join.
                        let _ = frame_tx.send((idx, frame)).await;
                        drop(permit);
                        Ok(())
                    });
                };

            while let Some(batch) = pair_rx.recv().await {
                for pair in batch {
                    let idx = ((pair.mass() / bucket_width) as usize).min(num_buckets - 1);
                    let buf = &mut buffers[idx];
                    buf.push(pair);
                    if buf.len() >= scatter_flush_pairs {
                        counts[idx] += buf.len() as u64;
                        let to_compress = std::mem::take(buf);
                        let permit = sem.clone().acquire_owned().await.expect("semaphore open");
                        spawn_compress(
                            &mut compressors,
                            idx,
                            to_compress,
                            permit,
                            frame_tx.clone(),
                        );
                        // Reap finished compressors to surface errors early and bound the JoinSet.
                        while let Some(res) = compressors.try_join_next() {
                            res.map_err(|err| Error::Join(err.to_string()))??;
                        }
                    }
                }
            }

            // Flush the trailing non-empty buffers.
            for idx in 0..num_buckets {
                if buffers[idx].is_empty() {
                    continue;
                }
                counts[idx] += buffers[idx].len() as u64;
                let to_compress = std::mem::take(&mut buffers[idx]);
                let permit = sem.clone().acquire_owned().await.expect("semaphore open");
                spawn_compress(&mut compressors, idx, to_compress, permit, frame_tx.clone());
            }

            // Drain every compressor so all frames reach the writer before this task's `frame_tx`
            // clone drops (which, with the workers done, lets the writer see EOF).
            while let Some(res) = compressors.join_next().await {
                res.map_err(|err| Error::Join(err.to_string()))??;
            }

            Ok::<Vec<u64>, Error>(counts)
        })
    };
    // The collector holds its own `frame_tx` clone; drop the original so the writer's channel can
    // close once the collector finishes.
    drop(frame_tx);

    // Proteins: no feeder. Partition the id space across workers; each worker pulls its own slice
    // via `by_ids` (a local HashMap walk for the in-memory access — no syscall, no cross-thread
    // handoff) and digests it flat-out, sending coarse pair batches to the collector.
    let mut workers = Vec::with_capacity(n);
    for chunk in ids.chunks(chunk_size) {
        let chunk = chunk.to_vec();
        let protein_access = protein_access.clone();
        let protease = protease.clone();
        let pair_tx = pair_tx.clone();
        let scatter_progress_metric = scatter_progress_metric.clone();
        workers.push(tokio::spawn(async move {
            let mut local: Vec<MassPidPair> = Vec::with_capacity(LOCAL_MASS_LIMIT);
            let mut since_report: u64 = 0;
            let mut proteins = protein_access.by_ids(&chunk).await?;
            while let Some(protein) = proteins.next().await.transpose()? {
                let protein_id = protein.id().ok_or(Error::MissingProteinId)?;
                let run_start = local.len();
                protease
                    .cleave_masses_only(protein.sequence().as_ref())
                    .for_each(|mass| {
                        local.push(MassPidPair::new(mass, protein_id));
                        Ok(())
                    })?;
                let run = &mut local[run_start..];
                run.sort_unstable_by_key(|pair| pair.mass());
                let kept = partition_dedup_by_mass(run);
                local.truncate(run_start + kept);
                if local.len() >= LOCAL_MASS_LIMIT {
                    let batch = std::mem::replace(&mut local, Vec::with_capacity(LOCAL_MASS_LIMIT));
                    // Collector gone (it errored) -> stop; its error surfaces when we join below.
                    if pair_tx.send(batch).await.is_err() {
                        return Ok(());
                    }
                }
                since_report += 1;
                if since_report >= PROGRESS_REPORT_EVERY {
                    scatter_progress_metric.increment(since_report);
                    since_report = 0;
                }
            }
            if since_report > 0 {
                scatter_progress_metric.increment(since_report);
            }
            if !local.is_empty() {
                let _ = pair_tx.send(local).await;
            }
            Ok::<_, Error>(())
        }));
    }
    // Drop the original pair sender so the collector's channel closes once all workers finish.
    drop(pair_tx);

    let mut first_err: Option<Error> = None;
    for worker in workers {
        match worker.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                first_err.get_or_insert(err);
            }
            Err(err) => {
                first_err.get_or_insert(Error::Join(err.to_string()));
            }
        }
    }
    let counts = match collector.await {
        Ok(Ok(counts)) => Some(counts),
        Ok(Err(err)) => {
            first_err.get_or_insert(err);
            None
        }
        Err(err) => {
            first_err.get_or_insert(Error::Join(err.to_string()));
            None
        }
    };
    let compressed_bytes = match writer.await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            first_err.get_or_insert(err);
            0
        }
        Err(err) => {
            first_err.get_or_insert(Error::Join(err.to_string()));
            0
        }
    };

    if let Some(err) = first_err {
        return Err(err);
    }
    let counts = counts.expect("collector succeeded => counts present");

    let raw_bytes: u64 = counts.iter().sum::<u64>() * MassPidPair::SIZE as u64;
    if raw_bytes > 0 {
        tracing::info!(
            "Mass index scatter buckets: {} MB raw -> {} MB zstd-{} ({:.0}%)",
            raw_bytes / (1024 * 1024),
            compressed_bytes / (1024 * 1024),
            ZSTD_BUCKET_LEVEL,
            compressed_bytes as f64 / raw_bytes as f64 * 100.0
        );
    }

    Ok(counts)
}

/// Finalize one bucket file into the global metadata + on-disk pid store. If the bucket is larger
/// than the sort budget, re-scatter it into finer sub-buckets over its mass range and recurse
/// (the sub-spill guard) instead of loading it whole.
#[allow(clippy::too_many_arguments)]
fn finalize_bucket(
    path: PathBuf,
    range: (i64, i64),
    n_pairs: u64,
    budget_pairs: u64,
    depth: usize,
    sort_pool: &rayon::ThreadPool,
    masses: &mut Vec<i64>,
    indptr: &mut Vec<u64>,
    pid_writer: &mut PidStoreWriter,
    subspill_count: &mut usize,
    finalize_metric: &metrics::Counter,
) -> Result<(), Error> {
    let (lo, hi) = range;
    // A bucket that received no pairs is never created by the scatter collector. Pair counts come
    // from the scatter's per-bucket counter (the compressed file size no longer reveals them).
    if n_pairs == 0 {
        return Ok(());
    }

    if n_pairs > budget_pairs && hi - lo > 1 && depth < MAX_SUBSPILL_DEPTH {
        *subspill_count += 1;
        let fanout = SUBSPILL_FANOUT.min((hi - lo) as usize).max(2);
        let sub_width = ((hi - lo) / fanout as i64).max(1);
        let actual = (((hi - lo) - 1) / sub_width + 1) as usize;
        let sub_buckets = restream_into_subbuckets(&path, lo, sub_width, actual)?;
        std::fs::remove_file(&path)?;

        for (s, (sub_path, sub_count)) in sub_buckets.into_iter().enumerate() {
            let slo = lo + s as i64 * sub_width;
            let shi = (lo + (s as i64 + 1) * sub_width).min(hi);
            finalize_bucket(
                sub_path,
                (slo, shi),
                sub_count,
                budget_pairs,
                depth + 1,
                sort_pool,
                masses,
                indptr,
                pid_writer,
                subspill_count,
                finalize_metric,
            )?;
        }
        return Ok(());
    }

    let mut pairs = read_pairs(&path, n_pairs as usize)?;
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

    finalize_metric.increment(pairs.len() as u64);
    Ok(())
}

/// Stream an oversized bucket file and re-scatter its pairs into `actual` finer sub-bucket files
/// over `[lo, lo + actual*sub_width)`, without loading the whole file into RAM. Returns each
/// sub-bucket's path paired with its pair count; an empty sub-bucket gets no file (count 0).
fn restream_into_subbuckets(
    path: &Path,
    lo: i64,
    sub_width: i64,
    actual: usize,
) -> Result<Vec<(PathBuf, u64)>, Error> {
    let dir = path.parent().expect("bucket file has a parent dir");
    let stem = path
        .file_stem()
        .expect("bucket file has a stem")
        .to_string_lossy();
    let sub_paths: Vec<PathBuf> = (0..actual)
        .map(|s| dir.join(format!("{stem}_s{s}.bin")))
        .collect();

    let mut buffers: Vec<Vec<MassPidPair>> = (0..actual).map(|_| Vec::new()).collect();
    let mut counts: Vec<u64> = vec![0u64; actual];

    stream_frames(path, |frame| {
        for &pair in frame {
            let s = (((pair.mass() - lo) / sub_width) as usize).min(actual - 1);
            counts[s] += 1;
            buffers[s].push(pair);
            if buffers[s].len() >= SUBSPILL_FLUSH_PAIRS {
                let compressed = compress_frame(&buffers[s])?;
                append_bytes(&sub_paths[s], &compressed)?;
                buffers[s].clear();
            }
        }
        Ok(())
    })?;

    for (s, buf) in buffers.iter().enumerate() {
        if !buf.is_empty() {
            let compressed = compress_frame(buf)?;
            append_bytes(&sub_paths[s], &compressed)?;
        }
    }

    Ok(sub_paths.into_iter().zip(counts).collect())
}

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
            // Mirror the scatter's per-protein dedup: count distinct masses, not raw peptides.
            let mut masses: Vec<i64> = protease
                .cleave_masses_only(protein.sequence().as_ref())
                .collect()?;
            masses.sort_unstable();
            masses.dedup();
            pairs += masses.len() as u64;
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

    #[test]
    fn test_partition_dedup_by_mass() {
        let pid = 42;
        let mk = |masses: &[i64]| {
            masses
                .iter()
                .map(|&m| MassPidPair::new(m, pid))
                .collect::<Vec<_>>()
        };

        // Empty
        assert_eq!(partition_dedup_by_mass(&mut []), 0);

        // Sorted run with adjacent duplicates (the only shape the caller produces).
        let mut run = mk(&[1, 1, 2, 3, 3, 3, 5]);
        let kept = partition_dedup_by_mass(&mut run);
        assert_eq!(kept, 4);
        assert_eq!(
            run[..kept].iter().map(|p| p.mass()).collect::<Vec<_>>(),
            vec![1, 2, 3, 5]
        );

        // No duplicates -> unchanged length.
        let mut run = mk(&[1, 2, 3]);
        assert_eq!(partition_dedup_by_mass(&mut run), 3);

        // All identical -> collapses to one.
        let mut run = mk(&[7, 7, 7, 7]);
        assert_eq!(partition_dedup_by_mass(&mut run), 1);
    }

    /// Round-trip a multi-frame bucket file through `compress_frame`/`append_bytes` and
    /// `stream_frames`/`read_pairs`, including masses whose high 32 bits are set (so the
    /// `mass_hi` plane is exercised, not just `mass_lo`).
    #[test]
    fn test_bucket_frame_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bucket_0.bin");

        // Three frames' worth of pairs, with masses spanning the 32-bit boundary.
        let pairs: Vec<MassPidPair> = (0..2500)
            .map(|i| MassPidPair::new((i as i64) * 1_000_003 + (1i64 << 33), (i % 7) as i32 - 3))
            .collect();
        for chunk in pairs.chunks(1000) {
            let frame = compress_frame(chunk).unwrap();
            append_bytes(&path, &frame).unwrap();
        }

        // read_pairs concatenates every frame in order.
        let got = read_pairs(&path, pairs.len()).unwrap();
        assert_eq!(got.len(), pairs.len());
        for (a, b) in got.iter().zip(pairs.iter()) {
            assert_eq!(a.mass(), b.mass());
            assert_eq!(a.pid(), b.pid());
        }

        // stream_frames yields the same pairs frame by frame.
        let mut streamed = Vec::new();
        stream_frames(&path, |frame| {
            streamed.extend_from_slice(frame);
            Ok(())
        })
        .unwrap();
        assert_eq!(streamed.len(), pairs.len());
        assert!(streamed.iter().zip(pairs.iter()).all(|(a, b)| a == b));
    }

    /// Exercise the block-framed pid store directly with a tiny block size so reads cross many
    /// block boundaries and start mid-block — the path the fixture-driven tests can't reach
    /// (the fixture has far fewer than `PID_BLOCK_ASSOCIATIONS` associations, i.e. one block).
    #[test]
    fn test_pid_store_multi_block_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pids.zst");

        // 1000 ids, blocks of 7 -> ~143 blocks; varied values incl. negatives.
        let ids: Vec<i32> = (0..1000).map(|i| (i * 7) - 1500).collect();
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

    /// A 16-byte memory budget drives `budget_pairs` to 1, so essentially every band overflows and
    /// recurses through `restream_into_subbuckets` to the depth cap. Verifies the framed sub-spill
    /// path stays correct (and that pair counts thread through sub-buckets) by matching the
    /// reference index.
    #[tokio::test]
    async fn test_mass_index_forced_subspill() {
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
            16, // budget_pairs == 1 -> maximal sub-spill
        )
        .await
        .unwrap();

        let total = mass_index.len();
        assert_eq!(total, manual.len(), "distinct mass count must match");

        let mut reader = mass_index.claim_reader(0, total).unwrap();
        let mut prev: Option<i64> = None;
        let mut seen = 0usize;
        while let Some((mass, ids)) = reader.next_entry().unwrap() {
            if let Some(p) = prev {
                assert!(
                    mass > p,
                    "masses must stay strictly ascending after sub-spill"
                );
            }
            prev = Some(mass);
            let expected = manual.get(&mass).unwrap();
            assert_eq!(ids.len(), expected.len(), "mass {mass}: id count");
            for id in &ids {
                assert!(expected.contains(id), "mass {mass}: unexpected id {id}");
            }
            seen += 1;
        }
        assert_eq!(seen, total);
    }
}
