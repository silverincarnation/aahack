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

static TSORT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
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
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2", target_feature = "sse2"))]
    if n == 9 && s[0].wrapping_sub(b'a') < 26 {
        unsafe {
            use std::arch::x86_64::*;
            let x = std::ptr::read_unaligned(s.as_ptr().add(1) as *const u64);
            let v = _mm_cvtsi64_si128(x as i64);
            let dig = _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8(0x2f)), _mm_cmplt_epi8(v, _mm_set1_epi8(0x3a)));
            let low = _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8(0x60)), _mm_cmplt_epi8(v, _mm_set1_epi8(0x7b)));
            let ok = _mm_movemask_epi8(_mm_or_si128(dig, low)) & 0xff;
            if ok == 0xff {
                let val = _mm_or_si128(
                    _mm_and_si128(dig, _mm_sub_epi8(v, _mm_set1_epi8(0x2f))),
                    _mm_and_si128(low, _mm_sub_epi8(v, _mm_set1_epi8(0x56))),
                );
                let y = (_mm_cvtsi128_si64(val) as u64).swap_bytes();
                let p = _pext_u64(y, 0x3f3f_3f3f_3f3f_3f3f);
                return TAG | ((s[0] - b'a') as u64) << 48 | p;
            }
        }
    }
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
fn scan_ref<const N: usize>(chunk: S, mut f: impl FnMut(&[S; N])) {
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

/// Same contract as `scan_ref`. Field ends are collected per line; quoting is checked per
/// line: the line's quote count must be exactly two per wrapped field (len >= 2, first and
/// last byte a quote), so lines without quotes skip all quote work.
#[inline(always)]
fn scan<const N: usize>(chunk: S, mut f: impl FnMut(&[S; N])) {
    let len = chunk.len();
    let mut ends = [0usize; N];
    let mut nf = 0usize;
    let mut ls = 0usize; // line start
    let mut qbase = 0u32; // quotes before this block
    let mut qline = 0u32; // quotes before this line
    let mut base = 0usize;
    let mut fields: [S; N] = [&[]; N];
    let mut line = |ls: usize, ends: &[usize; N], quotes: u32, fields: &mut [S; N]| {
        let mut st = ls;
        if quotes == 0 {
            for i in 0..N {
                fields[i] = unsafe { chunk.get_unchecked(st..ends[i]) };
                st = ends[i] + 1;
            }
        } else {
            let mut w = 0u32;
            for i in 0..N {
                let mut v = unsafe { chunk.get_unchecked(st..ends[i]) };
                if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
                    v = &v[1..v.len() - 1];
                    w += 2;
                }
                fields[i] = v;
                st = ends[i] + 1;
            }
            if w != quotes {
                return;
            }
        }
        f(fields);
    };
    while base < len {
        let n = (len - base).min(64);
        let (nl, cm, qm) = masks(&chunk[base..], n);
        let valid = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        let (nl, cm, qm) = (nl & valid, cm & valid, qm & valid);
        let mut sep = nl | cm;
        while sep != 0 {
            let t = sep.trailing_zeros();
            let pos = base + t as usize;
            if nf < N {
                ends[nf] = pos;
            }
            nf += 1;
            if (nl >> t) & 1 == 1 {
                let qc = qbase + (qm & ((1u64 << t) - 1)).count_ones();
                if nf == N && pos > ls {
                    line(ls, &ends, qc - qline, &mut fields);
                }
                qline = qc;
                nf = 0;
                ls = pos + 1;
            }
            sep &= sep - 1;
        }
        qbase += qm.count_ones();
        base += n;
    }
    // Last line without a trailing newline.
    if ls < len {
        if nf < N {
            ends[nf] = len;
        }
        nf += 1;
        if nf == N {
            line(ls, &ends, qbase - qline, &mut fields);
        }
    }
}

fn scan_selftest() {
    let mut seed: u64 = 7;
    let mut rnd = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let al = b"ab\"\",,\n\r 0";
    for it in 0..200_000 {
        let n = (rnd() % 300) as usize;
        let mut v: Vec<u8> = (0..n).map(|_| { let r = rnd(); if r % 3 == 0 { al[(r / 3) as usize % al.len()] } else { b'a' + (r % 3) as u8 } }).collect();
        if it % 2 == 0 { v.push(b'\n'); }
        let v: &'static [u8] = Box::leak(v.into_boxed_slice());
        let mut a: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut b: Vec<Vec<Vec<u8>>> = Vec::new();
        scan_ref::<3>(v, |f| a.push(f.iter().map(|x| x.to_vec()).collect()));
        scan::<3>(v, |f| b.push(f.iter().map(|x| x.to_vec()).collect()));
        assert_eq!(a, b, "{:?}", String::from_utf8_lossy(&v));
        let mut a: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut b: Vec<Vec<Vec<u8>>> = Vec::new();
        scan_ref::<1>(v, |f| a.push(f.iter().map(|x| x.to_vec()).collect()));
        scan::<1>(v, |f| b.push(f.iter().map(|x| x.to_vec()).collect()));
        assert_eq!(a, b, "{:?}", String::from_utf8_lossy(&v));
    }
    eprintln!("scan selftest ok");
}

// ---------------------------------------------------------------------------
// Timestamps (same verdicts as general::parse_ts, fewer branches)
// ---------------------------------------------------------------------------

const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

const TY0: usize = 1970;
const TYN: usize = 200;
/// Days since 1970-01-01 of the first of each month, and month lengths, for 1970..2170.
static MDAYS: [(i32, u8); TYN * 12] = {
    let mut t = [(0i32, 0u8); TYN * 12];
    let mut i = 0;
    while i < TYN * 12 {
        let y = (TY0 + i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let dim: [u8; 12] = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        t[i] = (days_from_civil(y, m, 1) as i32, dim[(m - 1) as usize]);
        i += 1;
    }
    t
};

#[inline(always)]
fn rd64(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// Each byte's digit value if every byte selected by `m` is an ASCII digit.
#[inline(always)]
fn digits(x: u64, m: u64) -> Option<u64> {
    let d = x ^ 0x3030_3030_3030_3030;
    let ok = (x & 0xF0F0_F0F0_F0F0_F0F0) ^ 0x3030_3030_3030_3030 | ((d & 0x0F0F_0F0F_0F0F_0F0F) + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010;
    if ok & m != 0 { None } else { Some(d & m) }
}

#[inline(always)]
fn ts_fast(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 || n == 25 {
        // "YYYY-MM-" "DDTHH:MM" then ":SSZ" or ":SS+HH:MM"
        let a = rd64(b, 0);
        let c = rd64(b, 8);
        const SA: u64 = 0xFF00_00FF_0000_0000; // bytes 4 and 7
        const SC: u64 = 0x0000_FF00_00FF_0000; // bytes 10 and 13
        if a & SA != u64::from_le_bytes(*b"\0\0\0\0-\0\0-") || c & SC != u64::from_le_bytes(*b"\0\0T\0\0:\0\0") {
            return general::parse_ts(b); // all-digit F3 of this length, or malformed
        }
        let da = digits(a, 0x00FF_FF00_FFFF_FFFF)?;
        let dc = digits(c, 0xFFFF_00FF_FF00_FFFF)?;
        let pa = da.wrapping_mul(10).wrapping_add(da >> 8); // byte i: 10*d[i] + d[i+1]
        let pc = dc.wrapping_mul(10).wrapping_add(dc >> 8);
        let y = (pa & 0xFF) as usize * 100 + ((pa >> 16) & 0xFF) as usize;
        let mo = ((pa >> 40) & 0xFF) as usize;
        let d = (pc & 0xFF) as u32;
        let h = ((pc >> 24) & 0xFF) as i64;
        let mi = ((pc >> 48) & 0xFF) as i64;
        let (se, off) = if n == 20 {
            let t = u32::from_le_bytes(b[16..20].try_into().unwrap());
            if t & 0xFF00_00FF != u32::from_le_bytes(*b":\0\0Z") {
                return None;
            }
            let ds = (t ^ 0x3030_3030) >> 8;
            let (s1, s2) = (ds & 0xFF, (ds >> 8) & 0xFF);
            if s1 > 9 || s2 > 9 {
                return None;
            }
            ((s1 * 10 + s2) as i64, 0i64)
        } else {
            if b[16] != b':' {
                return None;
            }
            let e = rd64(b, 17); // "SS+HH:MM"
            let sg = match b[19] {
                b'+' => 1i64,
                b'-' => -1,
                _ => return None,
            };
            if b[22] != b':' {
                return None;
            }
            let de = digits(e, 0xFFFF_00FF_FF00_FFFF)?;
            let pe = de.wrapping_mul(10).wrapping_add(de >> 8);
            let oh = ((pe >> 24) & 0xFF) as i64;
            let om = ((pe >> 48) & 0xFF) as i64;
            if oh > 23 || om > 59 {
                return None;
            }
            ((pe & 0xFF) as i64, sg * (oh * 3600 + om * 60) * 1000)
        };
        if h > 23 || mi > 59 || se > 59 || mo == 0 || mo > 12 || d == 0 {
            return None;
        }
        let days = if y >= TY0 && y < TY0 + TYN {
            let (base, dim) = MDAYS[(y - TY0) * 12 + mo - 1];
            if d > dim as u32 {
                return None;
            }
            base as i64 + d as i64 - 1
        } else {
            let yy = y as i64;
            let leap = (yy % 4 == 0 && yy % 100 != 0) || yy % 400 == 0;
            const DIM: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
            if d > DIM[mo] + (mo == 2 && leap) as u32 {
                return None;
            }
            days_from_civil(yy, mo as i64, d as i64)
        };
        return Some(days * DAY + (h * 3600 + mi * 60 + se) * 1000 - off);
    }
    if n > 0 && n <= 18 {
        let mut v: i64 = 0;
        for &ch in b {
            let dd = ch.wrapping_sub(b'0');
            if dd > 9 {
                return None;
            }
            v = v * 10 + dd as i64;
        }
        return Some(v);
    }
    general::parse_ts(b)
}

fn code_slow(s: &[u8]) -> u64 {
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
    u64::MAX
}

fn code_selftest() {
    let al = b"09az/:`{AZ_\x7f\x80\xff";
    let mut seed: u64 = 99;
    for _ in 0..5_000_000 {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
        let mut b = [0u8; 9];
        for i in 0..9 {
            let r = (seed >> (i * 7)) as usize;
            b[i] = if r % 5 == 0 { al[(r / 5) % al.len()] } else if r % 2 == 0 { b'0' + (r / 2 % 10) as u8 } else { b'a' + (r / 2 % 26) as u8 };
        }
        let want = code_slow(&b);
        if want != u64::MAX {
            assert_eq!(code(&b), want, "{:?}", b);
        } else {
            assert!(code(&b) & TAG == 0, "{:?}", b);
        }
    }
    eprintln!("code selftest ok");
}

fn ts_selftest() {
    // Compare with the general parser on many shapes of input.
    let mut seed: u64 = 0x1234_5678;
    let mut rnd = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let base = ["2026-10-03T23:58:10Z", "2026-10-03T19:58:10-04:00", "2028-02-29T00:00:00Z", "2027-02-29T00:00:00Z",
                "2026-12-31T23:59:59+23:59", "2026-01-01T00:00:00-00:00", "1791071890000", "0001791071890000", "0000-01-01T00:00:00Z",
                "1969-12-31T23:59:59Z", "2169-12-31T23:59:59Z", "2170-01-01T00:00:00Z", "2100-02-29T00:00:00Z", "2000-02-29T12:30:45+05:30",
                "9999-12-31T23:59:59-23:59", "1900-02-28T00:00:00Z", "123456789012345678", "1234567890123456789", "0"];
    let alphabet = b"0123456789-T:Z+zt ./e";
    let mut n = 0u64;
    for _ in 0..3_000_000 {
        let mut s: Vec<u8> = base[(rnd() % base.len() as u64) as usize].as_bytes().to_vec();
        let k = rnd() % 4;
        for _ in 0..k {
            let i = (rnd() as usize) % s.len();
            match rnd() % 3 {
                0 => s[i] = alphabet[(rnd() as usize) % alphabet.len()],
                1 => s[i] = b'0' + (rnd() % 10) as u8,
                _ => {
                    if rnd() % 2 == 0 { s.remove(i); } else { s.insert(i, b'0' + (rnd() % 10) as u8); }
                }
            }
            if s.is_empty() { break; }
        }
        if ts_fast(&s) != general::parse_ts(&s) {
            panic!("ts mismatch on {:?}: {:?} vs {:?}", String::from_utf8_lossy(&s), ts_fast(&s), general::parse_ts(&s));
        }
        n += 1;
    }
    for bs in base.iter() {
        let b0 = bs.as_bytes();
        for i in 0..b0.len() {
            for v in 0..=255u8 {
                let mut s = b0.to_vec();
                s[i] = v;
                assert_eq!(ts_fast(&s), general::parse_ts(&s), "{:?}", String::from_utf8_lossy(&s));
                for j in i + 1..b0.len() {
                    for w in [b'0', b'9', b'1', b'3', b'2', b'5', b'6'] {
                        let mut t = s.clone();
                        t[j] = w;
                        assert_eq!(ts_fast(&t), general::parse_ts(&t), "{:?}", String::from_utf8_lossy(&t));
                    }
                }
            }
        }
    }
    eprintln!("ts selftest ok: {n} cases");
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

/// Field order makes the little-endian u128 view sort by (person, ts, pc).
#[derive(Clone, Copy)]
#[repr(C, align(16))]
struct PEv {
    pc: u32,
    ts: u32,
    person: u64,
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

/// Flat table: linked ID -> person name, with a one-bit-per-slot-group filter in front.
struct Names {
    slots: Vec<(u64, u64)>,
    filt: Vec<u64>,
    mask: usize,
    fmask: usize,
}

const EMPTY: u64 = u64::MAX >> 1; // never a code: interned numbers stay far below it

impl Names {
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        if self.slots.is_empty() {
            return;
        }
        let h = h64(k);
        let fb = (h >> 32) as usize & self.fmask;
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.filt.as_ptr().add(fb >> 6) as *const i8, _MM_HINT_T0);
            _mm_prefetch(self.slots.as_ptr().add(h as usize & self.mask) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        if self.slots.is_empty() {
            return k;
        }
        let h = h64(k);
        let fb = (h >> 32) as usize & self.fmask;
        if unsafe { *self.filt.get_unchecked(fb >> 6) } >> (fb & 63) & 1 == 0 {
            return k;
        }
        let mut i = h as usize & self.mask;
        loop {
            let x = unsafe { *self.slots.get_unchecked(i) };
            if x.0 == k {
                return x.1;
            }
            if x.0 == EMPTY {
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
    use std::sync::atomic::AtomicU64;
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
        return Names { slots: vec![], filt: vec![], mask: 0, fmask: 0 };
    }
    // Concurrent open addressing; an ID's slot index is its node number.
    let cap = (pairs.len() * 4).next_power_of_two().max(16);
    let mask = cap - 1;
    let keys: Vec<AtomicU64> = (0..cap).into_par_iter().map(|_| AtomicU64::new(EMPTY)).collect();
    let slot = |k: u64| -> u32 {
        let mut i = h64(k) as usize & mask;
        loop {
            let cur = keys[i].load(Ordering::Relaxed);
            if cur == k {
                return i as u32;
            }
            if cur == EMPTY {
                match keys[i].compare_exchange(EMPTY, k, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => return i as u32,
                    Err(c) if c == k => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & mask;
        }
    };
    let edges: Vec<(u32, u32)> = pairs.par_iter().map(|&(a, b)| (slot(a), slot(b))).collect();
    drop(pairs);
    let keys: Vec<u64> = keys.into_iter().map(|a| a.into_inner()).collect();
    log("links: slots");
    let mut parent: Vec<u32> = (0..cap as u32).collect();
    for &(a, b) in &edges {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb { parent[rb as usize] = ra } else { parent[ra as usize] = rb }
        }
    }
    log("links: uf");
    // Name = byte-wise smallest ID of the component. Codes of generator-shaped IDs sort
    // like their bytes; only interned IDs need a byte comparison.
    let mut best: Vec<u64> = vec![EMPTY; cap];
    let mut n = 0usize;
    for g in 0..cap {
        let k = keys[g];
        if k == EMPTY {
            continue;
        }
        n += 1;
        let r = find(&mut parent, g as u32) as usize;
        parent[g] = r as u32;
        if best[r] == EMPTY || cmp_ids(k, best[r]) == std::cmp::Ordering::Less {
            best[r] = k;
        }
    }
    let fbits = (n * 16).next_power_of_two().max(64);
    let fmask = fbits - 1;
    let filt: Vec<AtomicU64> = (0..fbits / 64).into_par_iter().map(|_| AtomicU64::new(0)).collect();
    let slots: Vec<(u64, u64)> = (0..cap)
        .into_par_iter()
        .map(|g| {
            let k = keys[g];
            if k == EMPTY {
                (EMPTY, 0)
            } else {
                let fb = (h64(k) >> 32) as usize & fmask;
                filt[fb >> 6].fetch_or(1 << (fb & 63), Ordering::Relaxed);
                (k, best[parent[g] as usize])
            }
        })
        .collect();
    let filt: Vec<u64> = filt.into_iter().map(|a| a.into_inner()).collect();
    log(&format!("links: {} ids, {} edges", n, edges.len()));
    Names { slots, filt, mask, fmask }
}

/// Scatter into partitions through small per-partition staging buffers, so each partition
/// is written a few cache lines at a time.
struct Wc<T: Copy, const B: usize> {
    parts: Vec<Vec<T>>,
    buf: Vec<[T; B]>,
    cnt: Vec<u8>,
}

impl<T: Copy, const B: usize> Wc<T, B> {
    fn new(np: usize, cap: usize) -> Self {
        Wc {
            parts: (0..np).map(|_| Vec::with_capacity(cap)).collect(),
            buf: vec![unsafe { std::mem::zeroed() }; np],
            cnt: vec![0; np],
        }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, x: T) {
        unsafe {
            let c = self.cnt.get_unchecked_mut(p);
            let b = self.buf.get_unchecked_mut(p);
            *b.get_unchecked_mut(*c as usize) = x;
            *c += 1;
            if *c as usize == B {
                *c = 0;
                self.parts.get_unchecked_mut(p).extend_from_slice(b);
            }
        }
    }
    fn finish(mut self) -> Vec<Vec<T>> {
        for p in 0..self.parts.len() {
            let c = self.cnt[p] as usize;
            let b = self.buf[p];
            self.parts[p].extend_from_slice(&b[..c]);
        }
        self.parts
    }
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
    if std::env::var("TS_SELFTEST").is_ok() {
        ts_selftest();
        code_selftest();
        scan_selftest();
        return;
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


    let ev_bytes: usize = ev_chunks.iter().map(|c| c.len()).sum();
    let nfold = (ev_chunks.len() / general::min_len(ev_chunks.len())).max(1);
    let ev_cap = ev_bytes / 80 / nfold / NP1 * 5 / 4 + 16;
    // Events: every copy, scattered by hash(event_id).
    let parse_events = || -> Vec<Vec<Vec<Ev>>> {
    let r = ev_chunks
        .par_iter()
        .with_min_len(general::min_len(ev_chunks.len()))
        .fold(
            || Wc::<Ev, 4>::new(NP1, ev_cap),
            |mut acc, &ch| {
                scan::<7>(ch, |f| {
                    let eid = f[0];
                    if eid.is_empty() {
                        return;
                    }
                    let ingest = ts_fast(f[3]).unwrap_or(i64::MAX);
                    let ts = ts_fast(f[2]);
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
                    acc.push((h64(k) >> 54) as usize % NP1, e);
                });
                acc
            },
        )
        .map(|w| w.finish())
        .collect();
    log("events parsed");
    r
    };

    let parse_clicks = |names: &Names| -> Vec<Vec<Vec<PCl>>> {
    let r = cl_chunks
        .par_iter()
        .with_min_len(general::min_len(cl_chunks.len()))
        .fold(
            || Wc::<PCl, 8>::new(NP2, 0),
            |mut acc, &ch| {
                // Ring of clicks waiting for their person lookup, so the table line is
                // prefetched a few clicks ahead.
                const R: usize = 16;
                let mut ring = [PCl { person: 0, cid: 0, ts: 0, camp: 0 }; R];
                let mut rn = 0usize;
                let emit = |acc: &mut Wc<PCl, 8>, mut c: PCl| {
                    c.person = names.get(c.person);
                    acc.push((h64(c.person) >> 54) as usize % NP2, c);
                };
                scan::<4>(ch, |f| {
                    let (cid, user) = (f[0], f[1]);
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let ts = match ts_fast(f[3]) {
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
                    let u = code(user);
                    names.prefetch(u);
                    let slot = &mut ring[rn % R];
                    if rn >= R {
                        emit(&mut acc, *slot);
                    }
                    *slot = PCl { person: u, cid: code(cid), ts: (ts - lo_click) as u32, camp: c };
                    rn += 1;
                });
                for k in rn.saturating_sub(R)..rn {
                    emit(&mut acc, ring[k % R]);
                }
                acc
            },
        )
        .map(|w| w.finish()).collect();
    log("clicks parsed");
    r
    };
    let names = build_names(&ln_chunks);
    let ev_parts = parse_events();
    let pcl_parts = parse_clicks(&names);
    if FALLBACK.load(Ordering::Relaxed) {
        return false;
    }
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
                const PF: usize = 16;
                for (j, e) in best.iter().enumerate() {
                    if let Some(f) = best.get(j + PF) {
                        names.prefetch(f.user);
                    }
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
                let tq = Instant::now();
                let ne: usize = pev_parts.iter().map(|t| t[b].len()).sum();
                if ne == 0 {
                    return (urows, ca, cnt, active, pos);
                }
                // Group by person through a small local table (dense local ids), then sort
                // each person's events by ts.
                let tcap = (ne * 2).next_power_of_two().max(16);
                let tmask = tcap - 1;
                let mut tk: Vec<u64> = vec![EMPTY; tcap];
                let mut tv: Vec<u32> = vec![0; tcap];
                let mut persons: Vec<u64> = Vec::new();
                let mut ecnt: Vec<u32> = Vec::new();
                let mut lids: Vec<u32> = Vec::with_capacity(ne);
                for t in &pev_parts {
                    for e in &t[b] {
                        let mut i = h64(e.person) as usize & tmask;
                        let l = loop {
                            let k = tk[i];
                            if k == e.person {
                                break tv[i];
                            }
                            if k == EMPTY {
                                tk[i] = e.person;
                                let l = persons.len() as u32;
                                tv[i] = l;
                                persons.push(e.person);
                                ecnt.push(0);
                                break l;
                            }
                            i = (i + 1) & tmask;
                        };
                        ecnt[l as usize] += 1;
                        lids.push(l);
                    }
                }
                let np = persons.len();
                let mut eoff: Vec<u32> = Vec::with_capacity(np + 1);
                let mut acc_n = 0u32;
                for &c in &ecnt {
                    eoff.push(acc_n);
                    acc_n += c;
                }
                eoff.push(acc_n);
                let mut wp: Vec<u32> = eoff[..np].to_vec();
                let mut evs: Vec<PEv> = Vec::with_capacity(ne);
                unsafe { evs.set_len(ne) };
                let mut li = 0usize;
                for t in &pev_parts {
                    for e in &t[b] {
                        let l = lids[li] as usize;
                        li += 1;
                        evs[wp[l] as usize] = *e;
                        wp[l] += 1;
                    }
                }
                let lookup = |p: u64| -> u32 {
                    let mut i = h64(p) as usize & tmask;
                    loop {
                        let k = tk[i];
                        if k == p {
                            return tv[i];
                        }
                        if k == EMPTY {
                            return u32::MAX;
                        }
                        i = (i + 1) & tmask;
                    }
                };
                let mut ccnt: Vec<u32> = vec![0; np];
                lids.clear();
                for t in &pcl_parts {
                    for c in &t[b] {
                        let l = lookup(c.person);
                        lids.push(l);
                        if l != u32::MAX {
                            ccnt[l as usize] += 1;
                        }
                    }
                }
                let mut coff: Vec<u32> = Vec::with_capacity(np + 1);
                acc_n = 0;
                for &c in &ccnt {
                    coff.push(acc_n);
                    acc_n += c;
                }
                coff.push(acc_n);
                let ncl = acc_n as usize;
                wp.clear();
                wp.extend_from_slice(&coff[..np]);
                let mut cls: Vec<PCl> = Vec::with_capacity(ncl);
                unsafe { cls.set_len(ncl) };
                li = 0;
                for t in &pcl_parts {
                    for c in &t[b] {
                        let l = lids[li];
                        li += 1;
                        if l != u32::MAX {
                            cls[wp[l as usize] as usize] = *c;
                            wp[l as usize] += 1;
                        }
                    }
                }
                TSORT.fetch_add(tq.elapsed().as_nanos() as u64, Ordering::Relaxed);

                let mut catsum: Vec<(u8, u64)> = Vec::new();
                for g in 0..np {
                    let person = persons[g];
                    let (ei, ej) = (eoff[g] as usize, eoff[g + 1] as usize);
                    evs[ei..ej].sort_unstable_by_key(|e| e.ts);
                    let (c0, c1) = (coff[g] as usize, coff[g + 1] as usize);
                    let cs = &mut cls[c0..c1];
                    cs.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cmp_ids(a.cid, b.cid)));
                    let mut m = 0usize;
                    for k in 0..cs.len() {
                        if m > 0 && cs[m - 1].ts == cs[k].ts && cs[m - 1].cid == cs[k].cid {
                            continue;
                        }
                        cs[m] = cs[k];
                        m += 1;
                    }
                    let pc = &cls[c0..c0 + m];
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
    log(&format!("sort cpu {:?}", std::time::Duration::from_nanos(TSORT.load(Ordering::Relaxed))));
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
