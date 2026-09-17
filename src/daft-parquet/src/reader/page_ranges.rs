//! Plan physical page reads from the reader's existing row selection.
use std::ops::Range;

use parquet::{
    arrow::arrow_reader::RowSelection,
    errors::{ParquetError, Result},
    file::{metadata::ParquetMetaData, page_index::offset_index::PageLocation},
};

#[derive(Copy, Clone)]
pub(super) struct LeafRange {
    pub(super) leaf: usize,
    pub(super) start: u64,
    pub(super) len: u64,
}

// Avoid fetching a large skipped region just to combine requests. Full scans
// retain their existing, more aggressive coalescing policy.
pub(super) const PAGE_COALESCE_GAP: u64 = 64 * 1024;
const MAX_PAGE_RANGES: usize = 32;

pub(super) fn validate_page_locations(
    pages: &[PageLocation],
    chunk: Range<u64>,
    data_offset: i64,
    rows: usize,
) -> Result<()> {
    let invalid = || ParquetError::General("Invalid Parquet offset index page locations".into());
    if pages.is_empty() || pages[0].first_row_index != 0 || pages[0].offset != data_offset {
        return Err(invalid());
    }
    let mut previous_end = chunk.start;
    let mut previous_row = None;
    for page in pages {
        let start = u64::try_from(page.offset).map_err(|_| invalid())?;
        let size = u64::try_from(page.compressed_page_size).map_err(|_| invalid())?;
        let row = usize::try_from(page.first_row_index).map_err(|_| invalid())?;
        let end = start.checked_add(size).ok_or_else(invalid)?;
        if size == 0
            || start < previous_end
            || end > chunk.end
            || row >= rows
            || previous_row.is_some_and(|previous| row <= previous)
        {
            return Err(invalid());
        }
        previous_end = end;
        previous_row = Some(row);
    }
    Ok(())
}

fn merge_page_ranges(ranges: impl IntoIterator<Item = Range<u64>>) -> Vec<Range<u64>> {
    let mut out: Vec<Range<u64>> = Vec::new();
    for range in ranges {
        if let Some(last) = out.last_mut()
            && range.start <= last.end.saturating_add(PAGE_COALESCE_GAP)
        {
            last.end = last.end.max(range.end);
        } else {
            out.push(range);
        }
    }
    out
}

pub(super) fn selected_leaf_ranges(
    metadata: &ParquetMetaData,
    rg_idx: usize,
    leaves: &[usize],
    selection: Option<&RowSelection>,
    file_len: u64,
) -> Result<Vec<LeafRange>> {
    let rows = metadata.row_group_num_rows(rg_idx)?;
    let selected_rows = selection.map_or(rows, RowSelection::row_count);
    if selected_rows == 0 {
        return Ok(Vec::new());
    }
    let selection = selection.filter(|_| selected_rows < rows);
    let rg = metadata.row_group(rg_idx);
    let mut out = Vec::new();
    for &leaf in leaves {
        let column = rg.column(leaf);
        if column
            .dictionary_page_offset()
            .unwrap_or(column.data_page_offset())
            < 0
            || column.compressed_size() < 0
        {
            return Err(ParquetError::General(
                "Invalid Parquet column byte range".into(),
            ));
        }
        let (start, len) = column.byte_range();
        let end = start
            .checked_add(len)
            .filter(|&end| end <= file_len)
            .ok_or_else(|| {
                ParquetError::General("Parquet column byte range exceeds file length".into())
            })?;
        let whole = LeafRange { leaf, start, len };
        // Repeated records can span pages. Keep the established whole-column
        // decoder for lists/maps until it can safely expand page boundaries.
        let index = metadata
            .page_index()
            .and_then(|index| index.offset_index(rg_idx, leaf));
        let (Some(selection), Some(index)) = (selection, index) else {
            out.push(whole);
            continue;
        };
        if column.column_descr().max_rep_level() != 0 {
            out.push(whole);
            continue;
        }
        validate_page_locations(
            &index.page_locations,
            start..end,
            column.data_page_offset(),
            rows,
        )?;
        let pages = selection.scan_ranges(&index.page_locations);
        // The prefix contains the dictionary, when present, and must precede
        // selected data pages. Keep it even when selecting only a late page.
        let first_data = index.page_locations[0].offset as u64;
        let prefix = (start < first_data).then_some(start..first_data);
        let ranges = merge_page_ranges(prefix.into_iter().chain(pages));
        let bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        // Dense/fragmented selections seldom justify range bookkeeping or
        // additional HTTP requests. Require at least 20% fewer fetched bytes.
        if ranges.len() > MAX_PAGE_RANGES || bytes as u128 * 5 >= len as u128 * 4 {
            out.push(whole);
        } else {
            out.extend(ranges.into_iter().map(|range| LeafRange {
                leaf,
                start: range.start,
                len: range.end - range.start,
            }));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(offset: i64, size: i32, first_row: i64) -> PageLocation {
        PageLocation {
            offset,
            compressed_page_size: size,
            first_row_index: first_row,
        }
    }

    #[test]
    fn validates_offsets_and_flat_row_boundaries() {
        let valid = vec![page(100, 100, 0), page(200, 80, 20)];
        validate_page_locations(&valid, 50..280, 100, 40).unwrap();
        for pages in [
            vec![],
            vec![page(-1, 100, 0)],
            vec![page(100, -1, 0)],
            vec![page(100, 0, 0)],
            vec![page(100, 100, 1)],
            vec![page(100, 181, 0)],
            vec![page(100, 100, 0), page(190, 80, 20)],
            vec![page(100, 100, 0), page(200, 80, 0)],
            vec![page(100, 100, 0), page(200, 80, 40)],
        ] {
            assert!(validate_page_locations(&pages, 50..280, 100, 40).is_err());
        }
    }

    #[test]
    fn coalesces_only_small_page_gaps() {
        assert_eq!(
            merge_page_ranges([4..100, 100..200, 201..300, 100_000..100_100]),
            vec![4..300, 100_000..100_100]
        );
    }
}
