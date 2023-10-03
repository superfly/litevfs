use std::{collections::BTreeSet, ops};

use litetx as ltx;

pub(crate) const HEADER_SIZE: u64 = 100;
pub(crate) const WRITE_VERSION_OFFSET: usize = 18;
pub(crate) const READ_VERSION_OFFSET: usize = 19;
pub(crate) const COMMIT_RANGE: ops::Range<usize> = 28..32;

pub(crate) fn prefetch_candidates(
    data: &[u8],
    pgno: ltx::PageNum,
) -> Option<BTreeSet<ltx::PageNum>> {
    let bh = if pgno == ltx::PageNum::ONE {
        &data[HEADER_SIZE as usize..]
    } else {
        data
    };

    let num_cells = u16::from_be_bytes(bh[3..5].try_into().unwrap());
    match bh[0] {
        0x0d if pgno == ltx::PageNum::ONE => Some(master_table(&bh[8..], data, num_cells)),
        0x02 | 0x05 => {
            let rightmost_pointer = u32::from_be_bytes(bh[8..12].try_into().unwrap());
            let mut pgnos = interior_table_or_index(&bh[12..], data, num_cells);
            if let Ok(pgno) = ltx::PageNum::new(rightmost_pointer) {
                pgnos.insert(pgno);
            }

            Some(pgnos)
        }
        _ => None,
    }
}

// Returns the page numbers of the roots of all tables/indices/etc.
fn master_table(pointers: &[u8], data: &[u8], num_cells: u16) -> BTreeSet<ltx::PageNum> {
    pointers[..num_cells as usize * 2]
        .chunks_exact(2)
        .map(|c| u16::from_be_bytes(c.try_into().unwrap()) as usize)
        .filter_map(|cell| {
            if cell >= data.len() {
                return None;
            }
            let cell = &data[cell..];
            let (length, cell) = read_varint(cell);
            let (_rowid, cell) = read_varint(cell);

            // Has overflow page, ignore for now.
            if length as usize > data.len() - 35 {
                return None;
            }

            let (hsize, mut header) = read_varint(cell);
            let body = &cell[hsize as usize..];

            // skip type/name/tbl_name
            let mut pgno_offset: usize = 0;
            for _ in 0..3 {
                let (typ, header2) = read_varint(header);
                pgno_offset += type_size(typ);

                header = header2;
            }

            let (pgno, _) = read_varint(&body[pgno_offset..]);

            ltx::PageNum::new(pgno as u32).ok()
        })
        .collect()
}

// Returns the page numbers of the pages referenced by an interior table or index page.
fn interior_table_or_index(pointers: &[u8], data: &[u8], num_cells: u16) -> BTreeSet<ltx::PageNum> {
    pointers[..num_cells as usize * 2]
        .chunks_exact(2)
        .map(|c| u16::from_be_bytes(c.try_into().unwrap()) as usize)
        .filter_map(|cell| {
            if cell >= data.len() {
                return None;
            }
            let cell = &data[cell..];

            let pgno = u32::from_be_bytes(cell[0..4].try_into().unwrap());
            ltx::PageNum::new(pgno).ok()
        })
        .collect()
}

fn read_varint(data: &[u8]) -> (i64, &[u8]) {
    let mut n: i64 = 0;
    for (i, &b) in data.iter().enumerate() {
        if i == 8 {
            n = (n << 8) | (b as i64);
            return (n, &data[i + 1..]);
        }

        n = (n << 7) | ((b as i64) & 0x7f);
        if b < 0x80 {
            return (n, &data[i + 1..]);
        }
    }

    unreachable!();
}

fn type_size(typ: i64) -> usize {
    match typ {
        // NULL, 0 or 1
        0 | 8 | 9 => 0,
        // 8-bit int
        1 => 1,
        // 16-bit int
        2 => 2,
        // 24-bit int
        3 => 3,
        // 32-bit int
        4 => 4,
        // 48-bit int
        5 => 6,
        // 64-bit int
        6 => 8,
        // float
        7 => 8,
        // internal, should not be present in valid DBs
        10 | 11 => unreachable!(),
        n if n % 2 == 0 => ((n - 12) / 2) as usize,
        n => ((n - 13) / 2) as usize,
    }
}
