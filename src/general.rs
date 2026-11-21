// Shopping Sessions: native pipeline implementing rules.md.
//
// Reads $DATA_DIR (default /data), writes $OUTPUT_DIR (default /output):
// user_daily/ and campaign_daily/ as Parquet.
//
// Shape of the pipeline:
//   1. mmap every CSV, cut the files into line-aligned chunks, parse chunks in parallel.
//   2. Links: shard IDs by hash, give each a global number, union-find, name = smallest ID.
//   3. Events: per copy, work out everything that does not depend on dedup (validity, ts,
//      price, category); bucket by hash(event_id); per bucket keep the smallest ingest_ts
//      copy; keep it only if valid. Re-bucket survivors by hash(person name).
//   4. Clicks: parse, validate, bucket by hash(person name).
//   5. Per person bucket (parallel): sort events and clicks by person and time, cut
//      sessions, credit with a pointer, assists with a sliding 7-day window.
//   6. Write both tables.

use ahash::RandomState;
use hashbrown::HashTable;
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NP_DEDUP: usize = 512;
const NP_PERSON: usize = 256;
const NSHARD: usize = 256;

type S = &'static [u8];

pub static mut T0: Option<Instant> = None;
pub static mut VERBOSE: bool = false;

fn log(msg: &str) {
    unsafe {
        if VERBOSE {
            #[allow(static_mut_refs)]
            let t = T0.unwrap().elapsed().as_secs_f64();
            eprintln!("[{t:7.3}s] {msg}");
        }
    }
}

pub fn min_len(n: usize) -> usize {
    (n / (rayon::current_num_threads() * 2)).max(1)
}

fn hasher() -> RandomState {
    RandomState::with_seeds(0x243f6a8885a308d3, 0x13198a2e03707344, 0xa4093822299f31d0, 0x082efa98ec4e6c89)
}

// ---------------------------------------------------------------------------
// Input files and CSV dialect
// ---------------------------------------------------------------------------

pub fn map_file(p: &Path) -> S {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {p:?}: {e}"));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f).unwrap_or_else(|e| panic!("mmap {p:?}: {e}")) };
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
    }
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

pub fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// Line-aligned pieces of the files' data, each file's header line skipped.
pub fn chunks(datas: &[S], target: usize) -> Vec<S> {
    let mut out = Vec::new();
    for &d in datas {
        let start = match memchr::memchr(b'\n', d) {
            Some(i) => i + 1,
            None => continue, // header only (or nothing)
        };
        let mut pos = start;
        while pos < d.len() {
            let mut end = pos.saturating_add(target).min(d.len());
            if end < d.len() {
                end = match memchr::memchr(b'\n', &d[end..]) {
                    Some(i) => end + i + 1,
                    None => d.len(),
                };
            }
            out.push(&d[pos..end]);
            pos = end;
        }
    }
    out
}

/// Calls `f` with the fields of each well-formed line in `chunk` (exactly N fields, each
/// either quote-free or wholly wrapped in quotes; quotes removed). Empty lines are skipped.
#[inline]
pub fn for_each_record<const N: usize>(chunk: S, mut f: impl FnMut(&[S; N])) {
    let mut fields: [S; N] = [&[]; N];
    let len = chunk.len();
    let mut i = 0usize;
    while i < len {
        let ls = i;
        let mut fs = i;
        let mut n = 0usize;
        let mut q = 0u32;
        let mut ok = true;
        loop {
            let end = i == len || chunk[i] == b'\n';
            if end || chunk[i] == b',' {
                if ok {
                    let fld = &chunk[fs..i];
                    let v = if q == 0 {
                        fld
                    } else if q == 2 && fld.len() >= 2 && fld[0] == b'"' && fld[fld.len() - 1] == b'"' {
                        &fld[1..fld.len() - 1]
                    } else {
                        ok = false;
                        fld
                    };
                    if n == N {
                        ok = false;
                    } else {
                        fields[n] = v;
                        n += 1;
                    }
                }
                q = 0;
                fs = i + 1;
                if end {
                    break;
                }
            } else if chunk[i] == b'"' {
                q += 1;
            }
            i += 1;
        }
        if ok && n == N && i > ls {
            f(&fields);
        }
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i], b[i + 1]);
    if a.is_ascii_digit() && c.is_ascii_digit() { Some(((a - b'0') * 10 + (c - b'0')) as i64) } else { None }
}

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// F1, F2 or F3 to epoch ms; None for anything else (a too-long F3 is out of every window,
/// so None is the same verdict).
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    if b[0].is_ascii_digit() && b.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < b.len() && b[i] == b'0' {
            i += 1;
        }
        let r = &b[i..];
        if r.len() > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in r {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    let off: i64 = match b.len() {
        20 => {
            if b[19] != b'Z' {
                return None;
            }
            0
        }
        25 => {
            let sg = match b[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            sg * (oh * 3600 + om * 60) * 1000
        }
        _ => return None,
    };
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap { 29 } else { 28 }
        }
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some(days_from_civil(y, mo, d) * DAY + (h * 3600 + mi * 60 + s) * 1000 - off)
}

fn parse_iso_manifest(s: &str) -> i64 {
    parse_ts(s.as_bytes()).unwrap_or_else(|| panic!("bad manifest time {s}"))
}

/// Digits only, 1..=10,000,000.
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    let r = &b[i..];
    if r.is_empty() || r.len() > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in r {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) { Some(v) } else { None }
}

// ---------------------------------------------------------------------------
// Linked IDs
// ---------------------------------------------------------------------------

fn hasher2() -> RandomState {
    RandomState::with_seeds(0x452821e638d01377, 0xbe5466cf34e90c6c, 0xc0ac29b7c97c50dd, 0x3f84d5b5b5470917)
}

/// One cache line per lookup: linked ID (by two independent hashes) -> person name.
#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct Slot {
    h1: u64,
    nh: u64,
    ptr: usize,
    len: u32,
    chk: u32,
}

struct Links {
    flat: &'static mut [Slot],
    mask: usize,
}

/// Zeroed slots in an anonymous mapping, with transparent huge pages asked for
/// (random lookups into a few hundred MB otherwise miss the TLB every time).
fn alloc_slots(n: usize) -> &'static mut [Slot] {
    let bytes = n * std::mem::size_of::<Slot>();
    let m = memmap2::MmapMut::map_anon(bytes.max(4096)).expect("map_anon");
    #[cfg(target_os = "linux")]
    {
        let _ = m.advise(memmap2::Advice::HugePage);
    }
    let m: &'static mut memmap2::MmapMut = Box::leak(Box::new(m));
    unsafe { std::slice::from_raw_parts_mut(m.as_mut_ptr() as *mut Slot, n) }
}

impl Links {
    #[inline]
    fn lookup(&self, h1: u64, chk: u32) -> Option<(u64, S)> {
        let key = h1 | 1;
        let mut i = (h1 >> 7) as usize & self.mask;
        loop {
            let s = &self.flat[i];
            if s.h1 == 0 {
                return None;
            }
            if s.h1 == key && s.chk == chk {
                return Some((s.nh, unsafe { std::slice::from_raw_parts(s.ptr as *const u8, s.len as usize) }));
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Set of u64 (event-ID hashes of duplicated IDs), with a bitmap in front.
struct DupSet {
    bits: Vec<u64>,
    flat: Vec<u64>,
    mask: usize,
}

const DUP_BITS: u32 = 26;

impl DupSet {
    fn new(keys: &[u64]) -> Self {
        let mut bits = vec![0u64; 1 << (DUP_BITS - 6)];
        let cap = (keys.len() * 2).next_power_of_two().max(16);
        let mut flat = vec![0u64; cap];
        let mask = cap - 1;
        for &h in keys {
            let b = (h >> (64 - DUP_BITS)) as usize;
            bits[b >> 6] |= 1 << (b & 63);
            let key = h | 1;
            let mut i = (h >> 7) as usize & mask;
            while flat[i] != 0 && flat[i] != key {
                i = (i + 1) & mask;
            }
            flat[i] = key;
        }
        DupSet { bits, flat, mask }
    }
    #[inline]
    fn contains(&self, h: u64) -> bool {
        let b = (h >> (64 - DUP_BITS)) as usize;
        if self.bits[b >> 6] & (1 << (b & 63)) == 0 {
            return false;
        }
        let key = h | 1;
        let mut i = (h >> 7) as usize & self.mask;
        loop {
            let v = self.flat[i];
            if v == 0 {
                return false;
            }
            if v == key {
                return true;
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let gp = p[p[x as usize] as usize];
        p[x as usize] = gp;
        x = gp;
    }
    x
}

fn build_links(chs: &[S], rs: &RandomState) -> Links {
    // Parse; bucket each ID by shard; keep the edges.
    type Acc = (Vec<Vec<(u64, S)>>, Vec<(u64, S, u64, S)>);
    let parts: Vec<Acc> = chs
        .par_iter()
        .fold(
            || ((0..NSHARD).map(|_| Vec::new()).collect::<Vec<_>>(), Vec::new()),
            |mut acc: Acc, &ch| {
                for_each_record::<2>(ch, |f| {
                    let (a, b) = (f[0], f[1]);
                    if a.is_empty() || b.is_empty() {
                        return;
                    }
                    let ha = rs.hash_one(a);
                    let hb = rs.hash_one(b);
                    acc.0[(ha >> 56) as usize % NSHARD].push((ha, a));
                    acc.0[(hb >> 56) as usize % NSHARD].push((hb, b));
                    acc.1.push((ha, a, hb, b));
                });
                acc
            },
        )
        .collect();
    // Per shard: distinct IDs, local numbering.
    let shard_ids: Vec<(HashTable<(S, u32)>, Vec<S>)> = (0..NSHARD)
        .into_par_iter()
        .map(|sh| {
            let mut t: HashTable<(S, u32)> = HashTable::new();
            let mut ids: Vec<S> = Vec::new();
            for p in &parts {
                for &(h, s) in &p.0[sh] {
                    let n = ids.len() as u32;
                    let e = t.entry(h, |&(x, _)| x == s, |&(x, _)| rs.hash_one(x));
                    if let hashbrown::hash_table::Entry::Vacant(v) = e {
                        v.insert((s, n));
                        ids.push(s);
                    }
                }
            }
            (t, ids)
        })
        .collect();
    let mut offs = vec![0u32; NSHARD + 1];
    for sh in 0..NSHARD {
        offs[sh + 1] = offs[sh] + shard_ids[sh].1.len() as u32;
    }
    let n = offs[NSHARD] as usize;
    let mut shards = Vec::with_capacity(NSHARD);
    let mut id_of: Vec<S> = Vec::with_capacity(n);
    for (sh, (mut t, ids)) in shard_ids.into_iter().enumerate() {
        let o = offs[sh];
        for e in t.iter_mut() {
            e.1 += o;
        }
        id_of.extend_from_slice(&ids);
        shards.push(t);
    }
    let gid = |h: u64, s: S| -> u32 {
        let sh = (h >> 56) as usize % NSHARD;
        shards[sh].find(h, |&(x, _)| x == s).unwrap().1
    };
    let edges: Vec<(u32, u32)> = parts
        .par_iter()
        .flat_map_iter(|p| p.1.iter().map(|&(ha, a, hb, b)| (gid(ha, a), gid(hb, b))))
        .collect();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for &(a, b) in &edges {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb { parent[rb as usize] = ra } else { parent[ra as usize] = rb }
        }
    }
    let mut best: Vec<S> = vec![&[]; n];
    let mut has = vec![false; n];
    let mut root = vec![0u32; n];
    for g in 0..n as u32 {
        let r = find(&mut parent, g);
        root[g as usize] = r;
        let s = id_of[g as usize];
        if !has[r as usize] || s < best[r as usize] {
            best[r as usize] = s;
            has[r as usize] = true;
        }
    }
    let rs2 = hasher2();
    let cap = (n * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let flat = alloc_slots(cap);
    let nhs: Vec<u64> = (0..n).into_par_iter().map(|r| if has[r] { rs.hash_one(best[r]) } else { 0 }).collect();
    let keys: Vec<(u64, u32)> = id_of.par_iter().map(|&s| (rs.hash_one(s), rs2.hash_one(s) as u32)).collect();
    drop(shards);
    for g in 0..n {
        let (h1, chk) = keys[g];
        let r = root[g] as usize;
        let name = best[r];
        let mut i = (h1 >> 7) as usize & mask;
        while flat[i].h1 != 0 {
            i = (i + 1) & mask;
        }
        flat[i] = Slot { h1: h1 | 1, nh: nhs[r], ptr: name.as_ptr() as usize, len: name.len() as u32, chk };
    }
    log(&format!("links: {} ids, {} edges", n, edges.len()));
    Links { flat, mask }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct EvCopy {
    h: u64,
    eid: S,
    ingest: i64,
    user: S,
    ts: i64,
    price: u32,
    cat: u16,
    valid: bool,
}

#[derive(Clone, Copy)]
struct PEv {
    nh: u64,
    name: S,
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct PCl {
    nh: u64,
    name: S,
    ts: i64,
    cid: S,
    camp: u16,
}

struct URow {
    name: S,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

struct CAcc {
    // [camp * nd + day]: sessions, purchasing, revenue, assisted
    s: Vec<i64>,
    p: Vec<i64>,
    r: Vec<i64>,
    a: Vec<i64>,
}

impl CAcc {
    fn new(n: usize) -> Self {
        CAcc { s: vec![0; n], p: vec![0; n], r: vec![0; n], a: vec![0; n] }
    }
    fn add(mut self, o: CAcc) -> Self {
        for i in 0..self.s.len() {
            self.s[i] += o.s[i];
            self.p[i] += o.p[i];
            self.r[i] += o.r[i];
            self.a[i] += o.a[i];
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Parquet output
// ---------------------------------------------------------------------------

pub mod out {
    use parquet::basic::Compression;
    use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::fs::File;
    use std::sync::Arc;

    pub enum Col {
        Str(Vec<ByteArray>),
        OptStr(Vec<ByteArray>, Vec<i16>),
        I32(Vec<i32>),
        I64(Vec<i64>),
    }

    pub fn write(path: &str, schema: &str, cols: Vec<Col>) {
        let schema = Arc::new(parse_message_type(schema).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .build(),
        );
        let f = File::create(path).unwrap();
        let mut w = SerializedFileWriter::new(f, schema, props).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut it = cols.into_iter();
        while let Some(mut c) = rg.next_column().unwrap() {
            match it.next().unwrap() {
                Col::Str(v) => {
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                Col::OptStr(v, d) => {
                    c.typed::<ByteArrayType>().write_batch(&v, Some(&d), None).unwrap();
                }
                Col::I32(v) => {
                    c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                Col::I64(v) => {
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
            }
            c.close().unwrap();
        }
        rg.close().unwrap();
        w.close().unwrap();
    }
}

// ---------------------------------------------------------------------------

pub fn run() {
    unsafe {
        T0 = Some(Instant::now());
        VERBOSE = std::env::var("VERBOSE").map(|v| v == "1").unwrap_or(false);
    }
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let outd = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    if let Ok(n) = std::env::var("NCPU").map(|v| v.parse::<usize>().unwrap_or(0)) {
        if n > 0 {
            rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
        }
    }
    let rs = hasher();

    // Manifest: {"window_start": "...", "window_end": "..."}
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let field = |k: &str| -> String {
        let i = man.find(&format!("\"{k}\"")).unwrap_or_else(|| panic!("no {k}"));
        let rest = &man[i + k.len() + 2..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        rest[a..b].to_string()
    };
    let ws = parse_iso_manifest(&field("window_start"));
    let we = parse_iso_manifest(&field("window_end"));
    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // Map everything up front.
    let map_dir = |sub: &str| -> Vec<S> { csv_files(&data.join(sub)).iter().map(|p| map_file(p)).collect() };
    let (ev_data, (cl_data, ln_data)) = rayon::join(|| map_dir("events"), || rayon::join(|| map_dir("clicks"), || map_dir("links")));
    let prod_data = map_file(&data.join("products.csv"));
    let camp_data = map_file(&data.join("campaigns.csv"));
    let target = 4 << 20;
    let ev_chunks = chunks(&ev_data, target);
    let cl_chunks = chunks(&cl_data, target);
    let ln_chunks = chunks(&ln_data, 1 << 20);
    log(&format!("mapped: {} event chunks, {} click chunks", ev_chunks.len(), cl_chunks.len()));

    // Products and campaigns (clean, small).
    let mut cat_names: Vec<S> = Vec::new();
    let mut cat_idx: HashMap<S, u16> = HashMap::new();
    let mut prod: HashMap<S, u16, RandomState> = HashMap::with_hasher(hasher());
    for ch in chunks(&[prod_data], usize::MAX) {
        for_each_record::<2>(ch, |f| {
            let c = *cat_idx.entry(f[1]).or_insert_with(|| {
                cat_names.push(f[1]);
                (cat_names.len() - 1) as u16
            });
            prod.insert(f[0], c);
        });
    }
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown");
    // Rank of each category name, byte-wise, for tie-breaks.
    let mut order: Vec<u16> = (0..cat_names.len() as u16).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut cat_rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u16;
    }
    let mut camp_names: Vec<S> = Vec::new();
    let mut camp_chan: Vec<S> = Vec::new();
    let mut camp: HashMap<S, u16, RandomState> = HashMap::with_hasher(hasher());
    for ch in chunks(&[camp_data], usize::MAX) {
        for_each_record::<2>(ch, |f| {
            if !camp.contains_key(f[0]) {
                camp.insert(f[0], camp_names.len() as u16);
                camp_names.push(f[0]);
                camp_chan.push(f[1]);
            }
        });
    }
    let organic = camp_names.len() as u16;
    let ncamp = camp_names.len() + 1;

    // Links.
    let links = build_links(&ln_chunks, &rs);

    // Events, pass 1: hash every line's first field to find the event IDs that occur more
    // than once. A malformed line can only add a false "duplicate", which the exact path
    // below handles correctly anyway.
    let hash_parts: Vec<Vec<Vec<u64>>> = ev_chunks
        .par_iter()
        .with_min_len(min_len(ev_chunks.len()))
        .fold(
            || (0..NP_DEDUP).map(|_| Vec::new()).collect::<Vec<Vec<u64>>>(),
            |mut acc, &ch| {
                let mut pos = 0usize;
                while pos < ch.len() {
                    let rest = &ch[pos..];
                    let nl = memchr::memchr(b'\n', rest).unwrap_or(rest.len());
                    let line = &rest[..nl];
                    pos += nl + 1;
                    let mut f = match memchr::memchr(b',', line) {
                        Some(i) => &line[..i],
                        None => continue,
                    };
                    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                        f = &f[1..f.len() - 1];
                    }
                    if f.is_empty() {
                        continue;
                    }
                    let h = rs.hash_one(f);
                    acc[(h >> 40) as usize % NP_DEDUP].push(h);
                }
                acc
            },
        )
        .collect();
    let dups: Vec<Vec<u64>> = (0..NP_DEDUP)
        .into_par_iter()
        .map(|b| {
            let mut v: Vec<u64> = Vec::with_capacity(hash_parts.iter().map(|t| t[b].len()).sum());
            for t in &hash_parts {
                v.extend_from_slice(&t[b]);
            }
            v.sort_unstable();
            let mut d = Vec::new();
            let mut i = 0;
            while i < v.len() {
                let mut j = i + 1;
                while j < v.len() && v[j] == v[i] {
                    j += 1;
                }
                if j - i > 1 {
                    d.push(v[i]);
                }
                i = j;
            }
            d
        })
        .collect();
    drop(hash_parts);
    let dup_keys: Vec<u64> = dups.concat();
    drop(dups);
    let dups = DupSet::new(&dup_keys);
    log(&format!("events pass 1: {} duplicated ids", dup_keys.len()));

    let rs2 = hasher2();
    let person_of = |user: S| -> (u64, S) {
        let h = rs.hash_one(user);
        match links.lookup(h, rs2.hash_one(user) as u32) {
            Some(x) => x,
            None => (h, user),
        }
    };
    // Pass 2: parse every copy. Unique IDs go straight to person buckets when valid;
    // copies of duplicated IDs are kept for exact dedup.
    let parts: Vec<(Vec<Vec<PEv>>, Vec<EvCopy>)> = ev_chunks
        .par_iter()
        .with_min_len(min_len(ev_chunks.len()))
        .fold(
            || ((0..NP_PERSON).map(|_| Vec::new()).collect::<Vec<Vec<PEv>>>(), Vec::<EvCopy>::new()),
            |mut acc, &ch| {
                for_each_record::<7>(ch, |f| {
                    let eid = f[0];
                    if eid.is_empty() {
                        return;
                    }
                    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                    let user = f[1];
                    let ts = parse_ts(f[2]);
                    let mut valid = !user.is_empty() && !f[5].is_empty();
                    let mut price = 0u32;
                    let mut cat = 0u16;
                    let tsv = ts.unwrap_or(i64::MIN);
                    if valid {
                        valid = ts.is_some() && tsv >= ws && tsv < we && ingest.saturating_sub(tsv) <= DAY;
                    }
                    if valid {
                        match f[4] {
                            b"view" | b"cart" => {}
                            b"purchase" => match parse_price(f[6]) {
                                Some(p) => {
                                    price = p;
                                    cat = *prod.get(f[5]).unwrap_or(&unknown);
                                }
                                None => valid = false,
                            },
                            _ => valid = false,
                        }
                    }
                    let h = rs.hash_one(eid);
                    let is_dup = dups.contains(h);
                    if is_dup {
                        acc.1.push(EvCopy { h, eid, ingest, user, ts: tsv, price, cat, valid });
                    } else if valid {
                        let (nh, name) = person_of(user);
                        acc.0[(nh >> 40) as usize % NP_PERSON].push(PEv { nh, name, ts: tsv, price, cat });
                    }
                });
                acc
            },
        )
        .collect();
    log("events parsed");
    let mut pev_parts: Vec<Vec<Vec<PEv>>> = Vec::with_capacity(parts.len() + 1);
    let mut copies: Vec<EvCopy> = Vec::new();
    for (p, c) in parts {
        pev_parts.push(p);
        copies.extend(c);
    }
    // Exact dedup: smallest ingest_ts per event_id; equal ingest_ts copies are identical.
    copies.par_sort_unstable_by(|a, b| a.h.cmp(&b.h).then_with(|| a.eid.cmp(b.eid)).then(a.ingest.cmp(&b.ingest)));
    let mut extra: Vec<Vec<PEv>> = (0..NP_PERSON).map(|_| Vec::new()).collect();
    let mut i = 0;
    while i < copies.len() {
        let e = copies[i];
        let mut j = i + 1;
        while j < copies.len() && copies[j].h == e.h && copies[j].eid == e.eid {
            j += 1;
        }
        if e.valid {
            let (nh, name) = person_of(e.user);
            extra[(nh >> 40) as usize % NP_PERSON].push(PEv { nh, name, ts: e.ts, price: e.price, cat: e.cat });
        }
        i = j;
    }
    log(&format!("events deduped: {} copies of duplicated ids", copies.len()));
    drop(copies);
    pev_parts.push(extra);

    // Clicks.
    let lo_click = ws - WEEK;
    let pcl_parts: Vec<Vec<Vec<PCl>>> = cl_chunks
        .par_iter()
        .with_min_len(min_len(cl_chunks.len()))
        .fold(
            || (0..NP_PERSON).map(|_| Vec::new()).collect::<Vec<Vec<PCl>>>(),
            |mut acc, &ch| {
                for_each_record::<4>(ch, |f| {
                    let (cid, user) = (f[0], f[1]);
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let ts = match parse_ts(f[3]) {
                        Some(t) if t >= lo_click && t < we => t,
                        _ => return,
                    };
                    let c = match camp.get(f[2]) {
                        Some(&c) => c,
                        None => return,
                    };
                    let (nh, name) = person_of(user);
                    acc[(nh >> 40) as usize % NP_PERSON].push(PCl { nh, name, ts, cid, camp: c });
                });
                acc
            },
        )
        .collect();
    log("clicks parsed");

    // Per person bucket.
    let ncell = ncamp * nd;
    let (urows, cacc) = (0..NP_PERSON)
        .into_par_iter()
        .fold(
            || (Vec::<URow>::new(), CAcc::new(ncell), vec![0u32; ncamp], Vec::<u16>::new(), vec![0u32; ncamp]),
            |(mut urows, mut ca, mut cnt, mut active, mut pos), b| {
                // Local person numbering.
                let mut names: Vec<S> = Vec::new();
                let mut t: HashTable<(u64, u32)> = HashTable::new();
                let mut lid = |nh: u64, name: S, names: &mut Vec<S>| -> u32 {
                    match t.entry(nh, |&(h, i)| h == nh && names[i as usize] == name, |&(h, _)| h) {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let i = names.len() as u32;
                            names.push(name);
                            v.insert((nh, i));
                            i
                        }
                    }
                };
                let mut evs: Vec<(u64, u32, u16)> = Vec::new();
                for th in &pev_parts {
                    for e in &th[b] {
                        let l = lid(e.nh, e.name, &mut names) as u64;
                        evs.push(((l << 32) | (e.ts - ws) as u64, e.price, e.cat));
                    }
                }
                if evs.is_empty() {
                    return (urows, ca, cnt, active, pos);
                }
                let mut cls: Vec<(u64, S, u16)> = Vec::new();
                for th in &pcl_parts {
                    for c in &th[b] {
                        let l = lid(c.nh, c.name, &mut names) as u64;
                        cls.push(((l << 32) | (c.ts - lo_click) as u64, c.cid, c.camp));
                    }
                }
                evs.sort_unstable_by_key(|e| e.0);
                cls.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));
                cls.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

                let mut ci = 0usize; // clicks cursor (by person)
                let mut ei = 0usize;
                let mut catsum: Vec<(u16, u64)> = Vec::new();
                while ei < evs.len() {
                    let person = (evs[ei].0 >> 32) as u32;
                    let mut ej = ei;
                    while ej < evs.len() && (evs[ej].0 >> 32) as u32 == person {
                        ej += 1;
                    }
                    // This person's clicks: [c0, c1), times relative to lo_click.
                    while ci < cls.len() && ((cls[ci].0 >> 32) as u32) < person {
                        ci += 1;
                    }
                    let c0 = ci;
                    while ci < cls.len() && (cls[ci].0 >> 32) as u32 == person {
                        ci += 1;
                    }
                    let pc = &cls[c0..ci];
                    let cts = |k: usize| -> i64 { (pc[k].0 & 0xffff_ffff) as i64 + lo_click };
                    let mut hi = 0usize; // clicks with ts <= start
                    let mut lo = 0usize; // clicks with ts < start - WEEK
                    let mut grp = 0usize; // first click of the latest-ts group below hi
                    active.clear();

                    // user_daily accumulators for the current day
                    let mut cur_day: i64 = i64::MIN;
                    let (mut us, mut ue, mut ur) = (0i64, 0i64, 0i64);
                    catsum.clear();
                    let names_ref = |p: u32| names[p as usize];
                    let flush = |urows: &mut Vec<URow>, day: i64, us: i64, ue: i64, ur: i64, catsum: &Vec<(u16, u64)>| {
                        let mut top: Option<(u64, u16)> = None;
                        for &(c, s) in catsum {
                            top = match top {
                                None => Some((s, c)),
                                Some((bs, bc)) => {
                                    if s > bs || (s == bs && cat_rank[c as usize] < cat_rank[bc as usize]) {
                                        Some((s, c))
                                    } else {
                                        Some((bs, bc))
                                    }
                                }
                            };
                        }
                        urows.push(URow { name: names_ref(person), day: day as i32, sessions: us, events: ue, revenue: ur, top: top.map(|x| x.1) });
                    };

                    let mut i = ei;
                    while i < ej {
                        let start = (evs[i].0 & 0xffff_ffff) as i64 + ws;
                        let mut last = start;
                        let mut n = 0i64;
                        let mut rev = 0i64;
                        let mut k = i;
                        // categories go straight into the day's sums
                        let sday = start.div_euclid(DAY);
                        if sday != cur_day {
                            if cur_day != i64::MIN {
                                flush(&mut urows, cur_day, us, ue, ur, &catsum);
                            }
                            cur_day = sday;
                            us = 0;
                            ue = 0;
                            ur = 0;
                            catsum.clear();
                        }
                        while k < ej {
                            let t = (evs[k].0 & 0xffff_ffff) as i64 + ws;
                            if t - last > GAP {
                                break;
                            }
                            last = t;
                            n += 1;
                            let p = evs[k].1;
                            if p > 0 {
                                rev += p as i64;
                                let c = evs[k].2;
                                match catsum.iter_mut().find(|x| x.0 == c) {
                                    Some(x) => x.1 += p as u64,
                                    None => catsum.push((c, p as u64)),
                                }
                            }
                            k += 1;
                        }
                        us += 1;
                        ue += n;
                        ur += rev;
                        let di = (sday - day0) as usize;

                        // Advance the click window to [start - WEEK, start].
                        while hi < pc.len() && cts(hi) <= start {
                            if hi == 0 || cts(hi) != cts(hi - 1) {
                                grp = hi;
                            }
                            let c = pc[hi].2 as usize;
                            if cnt[c] == 0 {
                                pos[c] = active.len() as u32;
                                active.push(c as u16);
                            }
                            cnt[c] += 1;
                            hi += 1;
                        }
                        while lo < hi && cts(lo) < start - WEEK {
                            let c = pc[lo].2 as usize;
                            cnt[c] -= 1;
                            if cnt[c] == 0 {
                                let p = pos[c] as usize;
                                let lastc = *active.last().unwrap();
                                active[p] = lastc;
                                pos[lastc as usize] = p as u32;
                                active.pop();
                            }
                            lo += 1;
                        }
                        let credit = if hi > 0 && start - cts(grp) <= WEEK { pc[grp].2 } else { organic };
                        let cell = credit as usize * nd + di;
                        ca.s[cell] += 1;
                        ca.r[cell] += rev;
                        if rev > 0 {
                            ca.p[cell] += 1;
                        }
                        for &c in &active {
                            ca.a[c as usize * nd + di] += 1;
                        }
                        i = k;
                    }
                    if cur_day != i64::MIN {
                        flush(&mut urows, cur_day, us, ue, ur, &catsum);
                    }
                    // reset counts
                    for k in lo..hi {
                        cnt[pc[k].2 as usize] = 0;
                    }
                    active.clear();
                    ei = ej;
                }
                (urows, ca, cnt, active, pos)
            },
        )
        .map(|(u, c, _, _, _)| (u, c))
        .reduce(|| (Vec::new(), CAcc::new(ncell)), |mut a, b| {
            a.0.extend(b.0);
            (a.0, a.1.add(b.1))
        });
    log(&format!("sessions done: {} user rows", urows.len()));

    // Write.
    use out::Col;
    use parquet::data_type::ByteArray;
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let cat_names = Arc::new(cat_names);
    let ud_path = ud.join("part-0.parquet").to_string_lossy().to_string();
    let cd_path = cd.join("part-0.parquet").to_string_lossy().to_string();
    let st = |b: &'static [u8]| ByteArray::from(bytes::Bytes::from_static(b));
    let cat_static: Vec<&'static [u8]> = cat_names.iter().copied().collect();
    let nparts = rayon::current_num_threads().max(1);
    let per = urows.len().div_ceil(nparts).max(1);
    rayon::join(
        || {
            urows.par_chunks(per).enumerate().for_each(|(k, rows)| {
                let mut top_v = Vec::new();
                let mut top_d = Vec::with_capacity(rows.len());
                for r in rows {
                    match r.top {
                        Some(c) => {
                            top_v.push(st(cat_static[c as usize]));
                            top_d.push(1i16);
                        }
                        None => top_d.push(0),
                    }
                }
                let path = ud.join(format!("part-{k}.parquet")).to_string_lossy().to_string();
                out::write(
                    &path,
                    "message user_daily { required binary user_id (UTF8); required int32 day (DATE); \
                     required int64 sessions; required int64 events; required int64 revenue_cents; \
                     optional binary top_category (UTF8); }",
                    vec![
                        Col::Str(rows.iter().map(|r| st(r.name)).collect()),
                        Col::I32(rows.iter().map(|r| r.day).collect()),
                        Col::I64(rows.iter().map(|r| r.sessions).collect()),
                        Col::I64(rows.iter().map(|r| r.events).collect()),
                        Col::I64(rows.iter().map(|r| r.revenue).collect()),
                        Col::OptStr(top_v, top_d),
                    ],
                );
            });
            if urows.is_empty() {
                out::write(
                    &ud_path,
                    "message user_daily { required binary user_id (UTF8); required int32 day (DATE); \
                     required int64 sessions; required int64 events; required int64 revenue_cents; \
                     optional binary top_category (UTF8); }",
                    vec![Col::Str(vec![]), Col::I32(vec![]), Col::I64(vec![]), Col::I64(vec![]), Col::I64(vec![]), Col::OptStr(vec![], vec![])],
                );
            }
        },
        || {
            let (mut id, mut ch, mut dy, mut s, mut p, mut r, mut a) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
            for c in 0..ncamp {
                for d in 0..nd {
                    let cell = c * nd + d;
                    if cacc.s[cell] == 0 && cacc.a[cell] == 0 {
                        continue;
                    }
                    let (name, chan): (&[u8], &[u8]) = if c == organic as usize { (b"organic", b"organic") } else { (camp_names[c], camp_chan[c]) };
                    id.push(ByteArray::from(name.to_vec()));
                    ch.push(ByteArray::from(chan.to_vec()));
                    dy.push((day0 + d as i64) as i32);
                    s.push(cacc.s[cell]);
                    p.push(cacc.p[cell]);
                    r.push(cacc.r[cell]);
                    a.push(cacc.a[cell]);
                }
            }
            out::write(
                &cd_path,
                "message campaign_daily { required binary campaign_id (UTF8); required binary channel (UTF8); \
                 required int32 day (DATE); required int64 sessions; required int64 purchasing_sessions; \
                 required int64 revenue_cents; required int64 assisted_sessions; }",
                vec![Col::Str(id), Col::Str(ch), Col::I32(dy), Col::I64(s), Col::I64(p), Col::I64(r), Col::I64(a)],
            );
        },
    );
    log("written");
}
