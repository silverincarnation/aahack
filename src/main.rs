// Shopping Sessions: native pipeline implementing rules.md.
//
// Fast path (this file): every ID is packed into a u128 (bytes big-endian, length in the
// low byte), so IDs compare exactly and in byte order with integer operations, and every
// later step works on small fixed-size records:
//   1. mmap the CSVs; an AVX2 scanner finds commas, quotes and newlines 64 bytes at a time.
//   2. Links: sort the distinct IDs (byte order), union-find with the smallest index as
//      root, so each root is the person's name; a flat table maps linked ID -> name.
//   3. Events: parse and validate every copy; scatter by hash(event_id); per partition keep
//      the smallest ingest_ts copy, drop it unless valid; scatter by hash(person).
//   4. Clicks: validate, scatter by hash(person).
//   5. Per person partition: sort, cut sessions, credit and assists.
//
// If the data breaks an assumption of the fast path (an ID longer than 15 bytes, a window
// longer than 45 days, ...), the general implementation in general.rs runs instead.

mod general;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use rayon::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NP1: usize = 1024; // dedup partitions
const NP2: usize = 1024; // person partitions

type S = &'static [u8];

static FALLBACK: AtomicBool = AtomicBool::new(false);
static mut T0: Option<Instant> = None;
static mut VERBOSE: bool = false;

fn log(msg: &str) {
    unsafe {
        if VERBOSE {
            #[allow(static_mut_refs)]
            let t = T0.unwrap().elapsed().as_secs_f64();
            eprintln!("[{t:7.3}s] {msg}");
        }
    }
}

#[cold]
fn bail(why: &str) {
    if !FALLBACK.swap(true, Ordering::Relaxed) {
        log(&format!("fast path off: {why}"));
    }
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// Bytes big-endian in the top 15 bytes, length in the low byte: equal keys <=> equal
/// strings, and key order is byte-wise string order. None (and fallback) if over 15 bytes.
#[inline(always)]
fn pack(s: &[u8]) -> u128 {
    let n = s.len();
    if n > 15 {
        bail("id longer than 15 bytes");
        return 0;
    }
    let mut b = [0u8; 16];
    b[..n].copy_from_slice(s);
    b[15] = n as u8;
    u128::from_be_bytes(b)
}

/// 64-bit ID codes. The generator's IDs are a lowercase letter and 1 to 8 base-36 digits;
/// those get an order-preserving code (bit 63 set, letter in 5 bits, then 8 six-bit slots
/// holding digit + 1, 0 for none), so equal codes <=> equal strings and code order is byte
/// order. Any other ID is interned and gets a sequence number (bit 63 clear); comparing
/// those for order goes through the bytes.
const TAG: u64 = 1 << 63;

static INTERN: std::sync::Mutex<Option<(hashbrown::HashMap<Vec<u8>, u64>, Vec<Vec<u8>>)>> = std::sync::Mutex::new(None);

#[inline(always)]
fn d36(c: u8) -> u64 {
    match c {
        b'0'..=b'9' => (c - b'0') as u64 + 1,
        b'a'..=b'z' => (c - b'a') as u64 + 11,
        _ => 0,
    }
}

#[inline(always)]
fn code(s: &[u8]) -> u64 {
    let n = s.len();
    if (2..=9).contains(&n) && s[0].is_ascii_lowercase() {
        let mut v: u64 = 0;
        let mut ok = true;
        for i in 1..9 {
            let d = if i < n { d36(s[i]) } else { 0 };
            if i < n && d == 0 {
                ok = false;
            }
            v = (v << 6) | d;
        }
        if ok {
            return TAG | ((s[0] - b'a') as u64) << 48 | v;
        }
    }
    intern(s)
}

#[cold]
fn intern(s: &[u8]) -> u64 {
    let mut g = INTERN.lock().unwrap();
    let (m, v) = g.get_or_insert_with(|| (hashbrown::HashMap::new(), Vec::new()));
    if let Some(&c) = m.get(s) {
        return c;
    }
    let c = v.len() as u64;
    v.push(s.to_vec());
    m.insert(s.to_vec(), c);
    c
}

fn decode_into(c: u64, buf: &mut Vec<u8>) {
    if c & TAG != 0 {
        buf.push(b'a' + ((c >> 48) & 31) as u8);
        for i in (0..8).rev() {
            let d = (c >> (6 * i)) & 63;
            if d == 0 {
                break;
            }
            buf.push(if d <= 10 { b'0' + (d - 1) as u8 } else { b'a' + (d - 11) as u8 });
        }
    } else {
        let g = INTERN.lock().unwrap();
        buf.extend_from_slice(&g.as_ref().unwrap().1[c as usize]);
    }
}

fn decode(c: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(9);
    decode_into(c, &mut v);
    v
}

/// Byte-wise order of the IDs behind two codes.
#[inline]
fn cmp_ids(a: u64, b: u64) -> std::cmp::Ordering {
    if a & b & TAG != 0 {
        a.cmp(&b)
    } else if a == b {
        std::cmp::Ordering::Equal
    } else {
        decode(a).cmp(&decode(b))
    }
}

#[inline(always)]
fn h64(k: u64) -> u64 {
    let mut x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 29;
    x = x.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    x ^= x >> 32;
    x
}


// ---------------------------------------------------------------------------
// CSV scanning
// ---------------------------------------------------------------------------

/// Bitmasks of `\n`, `,` and `"` for the 64 bytes at `p` (fewer if `len` < 64).
#[inline(always)]
fn masks(p: &[u8], len: usize) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        if len >= 64 {
            let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
            let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
            let m = |c: i8| -> u64 {
                let v = _mm256_set1_epi8(c);
                let x = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
                let y = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
                x | (y << 32)
            };
            return (m(b'\n' as i8), m(b',' as i8), m(b'"' as i8));
        }
    }
    let (mut n, mut c, mut q) = (0u64, 0u64, 0u64);
    for i in 0..len.min(64) {
        match p[i] {
            b'\n' => n |= 1 << i,
            b',' => c |= 1 << i,
            b'"' => q |= 1 << i,
            _ => {}
        }
    }
    (n, c, q)
}

/// Calls `f` with the fields of every well-formed line of `chunk` (exactly N fields, each
/// quote-free or wholly in quotes, quotes removed). Empty lines are skipped.
#[inline(always)]
fn scan<const N: usize>(chunk: S, mut f: impl FnMut(&[S; N])) {
    let mut fields: [S; N] = [&[]; N];
    let len = chunk.len();
    let mut nf = 0usize;
    let mut ok = true;
    let mut fs = 0usize; // field start
    let mut ls = 0usize; // line start
    let mut q = 0u32; // quotes in the current field so far
    let mut base = 0usize;
    let end_field = |pos: usize, q: u32, nf: &mut usize, ok: &mut bool, fs: usize, fields: &mut [S; N]| {
        if !*ok {
            return;
        }
        if *nf == N {
            *ok = false;
            return;
        }
        let fld = &chunk[fs..pos];
        let v = if q == 0 {
            fld
        } else if q == 2 && fld.len() >= 2 && fld[0] == b'"' && fld[fld.len() - 1] == b'"' {
            &fld[1..fld.len() - 1]
        } else {
            *ok = false;
            return;
        };
        fields[*nf] = v;
        *nf += 1;
    };
    while base < len {
        let n = (len - base).min(64);
        let (nl, cm, qm) = masks(&chunk[base..], n);
        let valid = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        let (nl, cm, qm) = (nl & valid, cm & valid, qm & valid);
        let mut sep = nl | cm;
        let mut prev = 0u32; // bit where the current field (re)starts in this block
        while sep != 0 {
            let t = sep.trailing_zeros();
            sep &= sep - 1;
            let range = if t == 0 { 0 } else { (u64::MAX >> (64 - t)) & !((1u64 << prev) - 1) };
            let qq = q + (qm & range).count_ones();
            let pos = base + t as usize;
            end_field(pos, qq, &mut nf, &mut ok, fs, &mut fields);
            q = 0;
            fs = pos + 1;
            prev = t + 1;
            if (nl >> t) & 1 == 1 {
                if ok && nf == N && pos > ls {
                    f(&fields);
                }
                nf = 0;
                ok = true;
                ls = pos + 1;
            }
            if prev == 64 {
                break;
            }
        }
        if prev < 64 {
            q += (qm & (valid & !((1u64 << prev) - 1))).count_ones();
        }
        base += n;
    }
    // Last line without a trailing newline.
    if ls < len {
        end_field(len, q, &mut nf, &mut ok, fs, &mut fields);
        if ok && nf == N {
            f(&fields);
        }
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eid: u64,
    user: u64,
    ingest: i64,
    ts: u32,   // ms since window start
    pc: u32,   // price (24 bits) | cat << 24 (7 bits) | valid << 31
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    person: u64,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PCl {
    person: u64,
    cid: u64,
    ts: u32, // ms since window start - 7 days
    camp: u32,
}

struct URow {
    name: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u8>,
}

/// Flat table: linked ID -> person name.
struct Names {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
}

const EMPTY: u64 = u64::MAX >> 1; // never a code: interned numbers stay far below it

impl Names {
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        if self.keys.is_empty() {
            return k;
        }
        let mut i = h64(k) as usize & self.mask;
        loop {
            let x = self.keys[i];
            if x == k {
                return self.vals[i];
            }
            if x == EMPTY {
                return k;
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

fn build_names(chs: &[S]) -> Names {
    let pairs: Vec<(u64, u64)> = chs
        .par_iter()
        .flat_map_iter(|&ch| {
            let mut v = Vec::new();
            scan::<2>(ch, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((code(f[0]), code(f[1])));
                }
            });
            v
        })
        .collect();
    if pairs.is_empty() {
        return Names { keys: vec![], vals: vec![], mask: 0 };
    }
    let mut ids: Vec<u64> = pairs.par_iter().flat_map_iter(|&(a, b)| [a, b]).collect();
    ids.par_sort_unstable();
    ids.dedup();
    let n = ids.len();
    let gid = |k: u64| ids.binary_search(&k).unwrap() as u32;
    let edges: Vec<(u32, u32)> = pairs.par_iter().map(|&(a, b)| (gid(a), gid(b))).collect();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for &(a, b) in &edges {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb { parent[rb as usize] = ra } else { parent[ra as usize] = rb }
        }
    }
    // Name = byte-wise smallest ID of the component. Codes of generator-shaped IDs sort
    // like their bytes; only interned IDs need a byte comparison.
    let mut best: Vec<u64> = vec![EMPTY; n];
    for g in 0..n as u32 {
        let r = find(&mut parent, g) as usize;
        parent[g as usize] = r as u32;
        let k = ids[g as usize];
        if best[r] == EMPTY || cmp_ids(k, best[r]) == std::cmp::Ordering::Less {
            best[r] = k;
        }
    }
    let cap = (n * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut keys = vec![EMPTY; cap];
    let mut vals = vec![0u64; cap];
    for g in 0..n {
        let k = ids[g];
        let mut i = h64(k) as usize & mask;
        while keys[i] != EMPTY {
            i = (i + 1) & mask;
        }
        keys[i] = k;
        vals[i] = best[parent[g] as usize];
    }
    log(&format!("links: {} ids, {} edges", n, edges.len()));
    Names { keys, vals, mask }
}

fn split_into<T: Copy>(parts: usize) -> Vec<Vec<T>> {
    (0..parts).map(|_| Vec::new()).collect()
}

// ---------------------------------------------------------------------------

fn main() {
    unsafe {
        T0 = Some(Instant::now());
        VERBOSE = std::env::var("VERBOSE").map(|v| v == "1").unwrap_or(false);
    }
    if let Ok(n) = std::env::var("NCPU").map(|v| v.parse::<usize>().unwrap_or(0)) {
        if n > 0 {
            rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
        }
    }
    if std::env::var("FORCE_GENERAL").is_ok() || !fast() {
        log("running the general implementation");
        general::run();
    }
}

/// The fast path. Returns false (having written nothing) if the data needs the general one.
fn fast() -> bool {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let outd = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));

    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let field = |k: &str| -> String {
        let i = man.find(&format!("\"{k}\"")).unwrap_or_else(|| panic!("no {k}"));
        let rest = &man[i + k.len() + 2..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        rest[a..b].to_string()
    };
    let ws = general::parse_ts(field("window_start").as_bytes()).expect("window_start");
    let we = general::parse_ts(field("window_end").as_bytes()).expect("window_end");
    if we - ws > 40 * DAY || we <= ws {
        bail("window");
        return false;
    }
    let lo_click = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    let map_dir = |sub: &str| -> Vec<S> {
        general::csv_files(&data.join(sub)).par_iter().map(|p| general::map_file(p)).collect()
    };
    let (ev_data, (cl_data, ln_data)) =
        rayon::join(|| map_dir("events"), || rayon::join(|| map_dir("clicks"), || map_dir("links")));
    let ev_chunks = general::chunks(&ev_data, 2 << 20);
    let cl_chunks = general::chunks(&cl_data, 2 << 20);
    let ln_chunks = general::chunks(&ln_data, 1 << 20);
    log(&format!("mapped: {} event chunks", ev_chunks.len()));

    // Products and campaigns.
    let mut cat_names: Vec<S> = Vec::new();
    let mut cat_idx: HashMap<S, u8> = HashMap::new();
    let mut prod: hashbrown::HashMap<u128, u8> = hashbrown::HashMap::new();
    let mut long_prod = false;
    for ch in general::chunks(&[general::map_file(&data.join("products.csv"))], usize::MAX) {
        general::for_each_record::<2>(ch, |f| {
            if cat_names.len() >= 127 && !cat_idx.contains_key(f[1]) {
                bail("too many categories");
                return;
            }
            let c = *cat_idx.entry(f[1]).or_insert_with(|| {
                cat_names.push(f[1]);
                (cat_names.len() - 1) as u8
            });
            if f[0].len() > 15 {
                long_prod = true;
            } else {
                prod.insert(pack(f[0]), c);
            }
        });
    }
    if long_prod {
        bail("long product id");
    }
    let unknown = cat_names.len() as u8;
    cat_names.push(b"unknown");
    let mut order: Vec<u8> = (0..cat_names.len() as u8).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut cat_rank = vec![0u8; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u8;
    }
    let mut camp_names: Vec<S> = Vec::new();
    let mut camp_chan: Vec<S> = Vec::new();
    let mut camp: hashbrown::HashMap<u128, u32> = hashbrown::HashMap::new();
    for ch in general::chunks(&[general::map_file(&data.join("campaigns.csv"))], usize::MAX) {
        general::for_each_record::<2>(ch, |f| {
            let k = pack(f[0]);
            if !camp.contains_key(&k) {
                camp.insert(k, camp_names.len() as u32);
                camp_names.push(f[0]);
                camp_chan.push(f[1]);
            }
        });
    }
    let organic = camp_names.len();
    let ncamp = camp_names.len() + 1;
    if FALLBACK.load(Ordering::Relaxed) {
        return false;
    }

    let names = build_names(&ln_chunks);
    if FALLBACK.load(Ordering::Relaxed) {
        return false;
    }

    // Events: every copy, scattered by hash(event_id).
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.len()).sum();
    let nfold = (ev_chunks.len() / general::min_len(ev_chunks.len())).max(1);
    let ev_cap = ev_bytes / 80 / nfold / NP1 * 5 / 4 + 16;
    let ev_parts: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .with_min_len(general::min_len(ev_chunks.len()))
        .fold(
            || {
                let cap = ev_cap;
                (0..NP1).map(|_| Vec::with_capacity(cap)).collect::<Vec<Vec<Ev>>>()
            },
            |mut acc, &ch| {
                scan::<7>(ch, |f| {
                    let eid = f[0];
                    if eid.is_empty() {
                        return;
                    }
                    let ingest = general::parse_ts(f[3]).unwrap_or(i64::MAX);
                    let ts = general::parse_ts(f[2]);
                    let mut valid = !f[1].is_empty() && !f[5].is_empty();
                    let mut pc = 0u32;
                    let tsv = ts.unwrap_or(i64::MIN);
                    if valid {
                        valid = ts.is_some() && tsv >= ws && tsv < we && ingest.saturating_sub(tsv) <= DAY;
                    }
                    if valid {
                        match f[4] {
                            b"view" | b"cart" => {}
                            b"purchase" => match general::parse_price(f[6]) {
                                Some(p) => {
                                    let c = if f[5].len() > 15 { unknown } else { *prod.get(&pack(f[5])).unwrap_or(&unknown) };
                                    pc = p | ((c as u32) << 24);
                                }
                                None => valid = false,
                            },
                            _ => valid = false,
                        }
                    }
                    let k = code(eid);
                    let e = Ev {
                        eid: k,
                        user: if f[1].is_empty() { EMPTY } else { code(f[1]) },
                        ingest,
                        ts: if valid { (tsv - ws) as u32 } else { 0 },
                        pc: pc | ((valid as u32) << 31),
                    };
                    acc[(h64(k) >> 54) as usize % NP1].push(e);
                });
                acc
            },
        )
        .collect();
    if FALLBACK.load(Ordering::Relaxed) {
        return false;
    }
    log("events parsed");

    // Dedup per partition; survivors scattered by hash(person).
    let pev_parts: Vec<Vec<Vec<PEv>>> = (0..NP1)
        .into_par_iter()
        .with_min_len(NP1 / (rayon::current_num_threads() * 4).max(1))
        .fold(
            || split_into::<PEv>(NP2),
            |mut acc, p| {
                let total: usize = ev_parts.iter().map(|t| t[p].len()).sum();
                let cap = (total * 2).next_power_of_two().max(16);
                let mask = cap - 1;
                let mut slot: Vec<u32> = vec![u32::MAX; cap];
                let mut best: Vec<Ev> = Vec::with_capacity(total);
                for t in &ev_parts {
                    for e in &t[p] {
                        let mut i = (h64(e.eid) as usize) & mask;
                        loop {
                            let s = slot[i];
                            if s == u32::MAX {
                                slot[i] = best.len() as u32;
                                best.push(*e);
                                break;
                            }
                            let b = &mut best[s as usize];
                            if b.eid == e.eid {
                                if e.ingest < b.ingest {
                                    *b = *e;
                                }
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
                for e in &best {
                    if e.pc >> 31 == 0 {
                        continue;
                    }
                    let person = names.get(e.user);
                    acc[(h64(person) >> 54) as usize % NP2].push(PEv { person, ts: e.ts, pc: e.pc & 0x7fff_ffff });
                }
                acc
            },
        )
        .collect();
    drop(ev_parts);
    log("events deduped");

    // Clicks.
    let pcl_parts: Vec<Vec<Vec<PCl>>> = cl_chunks
        .par_iter()
        .with_min_len(general::min_len(cl_chunks.len()))
        .fold(
            || split_into::<PCl>(NP2),
            |mut acc, &ch| {
                scan::<4>(ch, |f| {
                    let (cid, user) = (f[0], f[1]);
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let ts = match general::parse_ts(f[3]) {
                        Some(t) if t >= lo_click && t < we => t,
                        _ => return,
                    };
                    if f[2].len() > 15 {
                        return; // not a campaign (all campaign ids are packable)
                    }
                    let c = match camp.get(&pack(f[2])) {
                        Some(&c) => c,
                        None => return,
                    };
                    let person = names.get(code(user));
                    let cid = code(cid);
                    acc[(h64(person) >> 54) as usize % NP2].push(PCl { person, cid, ts: (ts - lo_click) as u32, camp: c });
                });
                acc
            },
        )
        .collect();
    if FALLBACK.load(Ordering::Relaxed) {
        return false;
    }
    log("clicks parsed");

    // Per person partition.
    let ncell = ncamp * nd;
    let shift = (ws - lo_click) as u32; // event ts -> click ts scale
    type Acc = (Vec<URow>, Vec<[i64; 4]>, Vec<u32>, Vec<u32>, Vec<u32>);
    let (urows, cacc) = (0..NP2)
        .into_par_iter()
        .with_min_len(NP2 / (rayon::current_num_threads() * 4).max(1))
        .fold(
            || -> Acc { (Vec::new(), vec![[0i64; 4]; ncell], vec![0u32; ncamp], Vec::new(), vec![0u32; ncamp]) },
            |(mut urows, mut ca, mut cnt, mut active, mut pos): Acc, b| {
                let mut evs: Vec<PEv> = Vec::with_capacity(pev_parts.iter().map(|t| t[b].len()).sum());
                for t in &pev_parts {
                    evs.extend_from_slice(&t[b]);
                }
                if evs.is_empty() {
                    return (urows, ca, cnt, active, pos);
                }
                let mut cls: Vec<PCl> = Vec::with_capacity(pcl_parts.iter().map(|t| t[b].len()).sum());
                for t in &pcl_parts {
                    cls.extend_from_slice(&t[b]);
                }
                evs.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
                cls.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| cmp_ids(a.cid, b.cid)));
                cls.dedup_by(|a, b| a.person == b.person && a.cid == b.cid && a.ts == b.ts);

                let mut ci = 0usize;
                let mut ei = 0usize;
                let mut catsum: Vec<(u8, u64)> = Vec::new();
                while ei < evs.len() {
                    let person = evs[ei].person;
                    let mut ej = ei + 1;
                    while ej < evs.len() && evs[ej].person == person {
                        ej += 1;
                    }
                    while ci < cls.len() && cls[ci].person < person {
                        ci += 1;
                    }
                    let c0 = ci;
                    while ci < cls.len() && cls[ci].person == person {
                        ci += 1;
                    }
                    let pc = &cls[c0..ci];
                    let (mut hi, mut lo, mut grp) = (0usize, 0usize, 0usize);
                    active.clear();
                    let mut cur_day = i64::MIN;
                    let (mut us, mut ue, mut ur) = (0i64, 0i64, 0i64);
                    catsum.clear();
                    let flush = |urows: &mut Vec<URow>, day: i64, us: i64, ue: i64, ur: i64, catsum: &Vec<(u8, u64)>| {
                        let mut top: Option<(u64, u8)> = None;
                        for &(c, s) in catsum {
                            top = match top {
                                Some((bs, bc)) if !(s > bs || (s == bs && cat_rank[c as usize] < cat_rank[bc as usize])) => Some((bs, bc)),
                                _ => Some((s, c)),
                            };
                        }
                        urows.push(URow { name: person, day: day as i32, sessions: us, events: ue, revenue: ur, top: top.map(|x| x.1) });
                    };
                    let mut i = ei;
                    while i < ej {
                        let st = evs[i].ts; // ms since ws
                        let start = st as i64 + ws;
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
                        let mut last = st;
                        let mut n = 0i64;
                        let mut rev = 0i64;
                        let mut k = i;
                        while k < ej {
                            let t = evs[k].ts;
                            if (t - last) as i64 > GAP {
                                break;
                            }
                            last = t;
                            n += 1;
                            let p = evs[k].pc & 0x00ff_ffff;
                            if p > 0 {
                                rev += p as i64;
                                let c = (evs[k].pc >> 24) as u8;
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
                        let sc = st + shift; // start on the click scale
                        while hi < pc.len() && pc[hi].ts <= sc {
                            if hi == 0 || pc[hi].ts != pc[hi - 1].ts {
                                grp = hi;
                            }
                            let c = pc[hi].camp as usize;
                            if cnt[c] == 0 {
                                pos[c] = active.len() as u32;
                                active.push(c as u32);
                            }
                            cnt[c] += 1;
                            hi += 1;
                        }
                        // drop clicks before start - 7 days (sc - WEEK can't underflow: sc >= WEEK)
                        let lim = sc - WEEK as u32;
                        while lo < hi && pc[lo].ts < lim {
                            let c = pc[lo].camp as usize;
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
                        let credit = if hi > 0 && pc[grp].ts >= lim { pc[grp].camp as usize } else { organic };
                        let cell = &mut ca[credit * nd + di];
                        cell[0] += 1;
                        cell[2] += rev;
                        if rev > 0 {
                            cell[1] += 1;
                        }
                        for &c in &active {
                            ca[c as usize * nd + di][3] += 1;
                        }
                        i = k;
                    }
                    if cur_day != i64::MIN {
                        flush(&mut urows, cur_day, us, ue, ur, &catsum);
                    }
                    for k in lo..hi {
                        cnt[pc[k].camp as usize] = 0;
                    }
                    active.clear();
                    ei = ej;
                }
                (urows, ca, cnt, active, pos)
            },
        )
        .map(|(u, c, _, _, _)| (u, c))
        .reduce(
            || (Vec::new(), vec![[0i64; 4]; ncell]),
            |mut a, b| {
                if a.0.len() < b.0.len() {
                    let mut b0 = b.0;
                    b0.append(&mut a.0);
                    a.0 = b0;
                } else {
                    let mut b0 = b.0;
                    a.0.append(&mut b0);
                }
                for (x, y) in a.1.iter_mut().zip(b.1.iter()) {
                    for j in 0..4 {
                        x[j] += y[j];
                    }
                }
                a
            },
        );
    log(&format!("sessions done: {} user rows", urows.len()));

    // Write.
    use general::out::{self, Col};
    use parquet::data_type::ByteArray;
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let st = |b: &'static [u8]| ByteArray::from(bytes::Bytes::from_static(b));
    let cat_static: Vec<S> = cat_names.clone();
    let nparts = rayon::current_num_threads().max(1);
    let per = urows.len().div_ceil(nparts).max(1);
    const UD_SCHEMA: &str = "message user_daily { required binary user_id (UTF8); required int32 day (DATE); \
                 required int64 sessions; required int64 events; required int64 revenue_cents; \
                 optional binary top_category (UTF8); }";
    rayon::join(
        || {
            urows.par_chunks(per).enumerate().for_each(|(k, rows)| {
                let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 10);
                let mut offs: Vec<(usize, usize)> = Vec::with_capacity(rows.len());
                for r in rows {
                    let o = buf.len();
                    decode_into(r.name, &mut buf);
                    offs.push((o, buf.len() - o));
                }
                let buf = bytes::Bytes::from(buf);
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
                    UD_SCHEMA,
                    vec![
                        Col::Str(offs.iter().map(|&(o, n)| ByteArray::from(buf.slice(o..o + n))).collect()),
                        Col::I32(rows.iter().map(|r| r.day).collect()),
                        Col::I64(rows.iter().map(|r| r.sessions).collect()),
                        Col::I64(rows.iter().map(|r| r.events).collect()),
                        Col::I64(rows.iter().map(|r| r.revenue).collect()),
                        Col::OptStr(top_v, top_d),
                    ],
                );
            });
            if urows.is_empty() {
                let path = ud.join("part-0.parquet").to_string_lossy().to_string();
                out::write(
                    &path,
                    UD_SCHEMA,
                    vec![Col::Str(vec![]), Col::I32(vec![]), Col::I64(vec![]), Col::I64(vec![]), Col::I64(vec![]), Col::OptStr(vec![], vec![])],
                );
            }
        },
        || {
            let (mut id, mut ch, mut dy, mut s, mut p, mut r, mut a) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
            for c in 0..ncamp {
                for d in 0..nd {
                    let cell = cacc[c * nd + d];
                    if cell[0] == 0 && cell[3] == 0 {
                        continue;
                    }
                    let (name, chan): (S, S) = if c == organic { (b"organic", b"organic") } else { (camp_names[c], camp_chan[c]) };
                    id.push(st(name));
                    ch.push(st(chan));
                    dy.push((day0 + d as i64) as i32);
                    s.push(cell[0]);
                    p.push(cell[1]);
                    r.push(cell[2]);
                    a.push(cell[3]);
                }
            }
            out::write(
                &cd.join("part-0.parquet").to_string_lossy(),
                "message campaign_daily { required binary campaign_id (UTF8); required binary channel (UTF8); \
                 required int32 day (DATE); required int64 sessions; required int64 purchasing_sessions; \
                 required int64 revenue_cents; required int64 assisted_sessions; }",
                vec![Col::Str(id), Col::Str(ch), Col::I32(dy), Col::I64(s), Col::I64(p), Col::I64(r), Col::I64(a)],
            );
        },
    );
    log("written");
    true
}
