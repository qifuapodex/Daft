from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft


@pytest.mark.parametrize("compression", ["uncompressed", "snappy", "zstd"])
def test_native_writer_multiple_row_groups(tmp_path, compression):
    # The Arrow 60 factory takes the file's row-group index. Exercise repeated
    # factory creation, dictionary pages and offsets within a single file.
    rows = 8192
    expected = pa.table(
        {
            "id": range(rows),
            "label": [None if i % 17 == 0 else f"label-{i % 31}" for i in range(rows)],
            "items": pa.array([None if i % 7 == 0 else [i, None] for i in range(rows)], type=pa.list_(pa.int64())),
        }
    )
    path = tmp_path / "multiple-row-groups.parquet"
    with daft.execution_config_ctx(parquet_target_row_group_size=4096, native_parquet_writer=True):
        daft.from_arrow(expected).write_parquet(str(path), single_file=True, compression=compression)
    parquet_file = papq.ParquetFile(path)
    assert parquet_file.num_row_groups > 1
    batches = [parquet_file.read_row_group(i) for i in range(parquet_file.num_row_groups)]
    assert pa.concat_tables(batches).cast(expected.schema).equals(expected)
    for i in range(parquet_file.num_row_groups):
        for j in range(parquet_file.metadata.num_columns):
            column = parquet_file.metadata.row_group(i).column(j)
            assert column.total_compressed_size > 0
            assert column.dictionary_page_offset is None or column.dictionary_page_offset < column.data_page_offset
    assert daft.read_parquet(str(path)).to_arrow().cast(expected.schema).equals(expected)
