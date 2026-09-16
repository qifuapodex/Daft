from __future__ import annotations

import random

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from daft.expressions.expressions import _resolved_col
from daft.recordbatch import MicroPartition
from tests.io.test_parquet_metadata_cache import (
    range_server as range_server,  # noqa: PLC0414 -- pytest fixture re-export
)


@pytest.fixture
def page_table():
    rng = random.Random(1729)
    rows = 8192
    return pa.table(
        {
            "id": range(rows),
            # A repeated key prevents row-group statistics from eliminating groups.
            "key": [i % 2048 for i in range(rows)],
            "payload": [None if i % 19 == 0 else rng.randbytes(128).hex() for i in range(rows)],
            "nested": pa.array(
                [None if i % 11 == 0 else [i, None, i + 1] for i in range(rows)], type=pa.list_(pa.int64())
            ),
        }
    )


@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize("dictionary", [False, True])
@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
def test_page_selection_roundtrip(tmp_path, range_server, page_table, remote, indexed, dictionary, page_version):
    path = tmp_path / "pages.parquet"
    pq.write_table(
        page_table,
        path,
        row_group_size=2048,
        data_page_size=4096,
        write_batch_size=32,
        write_page_index=indexed,
        use_dictionary=dictionary,
        data_page_version=page_version,
    )
    uri = f"{range_server[0]}/{path.name}" if remote else str(path)
    for key_predicate, expected_mask, limit in [
        (_resolved_col("key") < 35, pc.less(page_table["key"], 35), None),
        (_resolved_col("key") % 97 == 0, pa.array([i % 2048 % 97 == 0 for i in range(len(page_table))]), 17),
        (_resolved_col("key") % 97 == 98, pc.less(page_table["key"], 0), None),
    ]:
        expected = page_table.filter(expected_mask).select(["payload", "nested"])
        if limit is not None:
            expected = expected.slice(0, limit)
        actual = MicroPartition.read_parquet(
            uri, columns=["payload", "nested"], predicate=key_predicate, num_rows=limit
        )
        assert actual.to_arrow().cast(expected.schema).equals(expected)
    expected = page_table.slice(0, 13).select(["payload", "nested"])
    actual = MicroPartition.read_parquet(uri, columns=["payload", "nested"], num_rows=13)
    assert actual.to_arrow().cast(expected.schema).equals(expected)


def test_selective_http_reads_skip_payload_pages(tmp_path, range_server, page_table):
    total_bytes = {}
    for indexed in [False, True]:
        path = tmp_path / f"index-{indexed}.parquet"
        pq.write_table(
            page_table.select(["id", "key", "payload"]),
            path,
            row_group_size=2048,
            data_page_size=4096,
            write_batch_size=32,
            write_page_index=indexed,
            use_dictionary=False,
        )
        range_server[1].clear()
        actual = MicroPartition.read_parquet(
            f"{range_server[0]}/{path.name}", columns=["payload"], predicate=_resolved_col("key") < 35
        )
        expected = page_table.filter(pc.less(page_table["key"], 35)).select(["payload"])
        assert actual.to_arrow().cast(expected.schema).equals(expected)
        total_bytes[indexed] = sum(end - start for _, start, end in range_server[1])
    # Count actual HTTP response bodies, including footers and indexes. This is
    # independent of the implementation's range planner and OS cache state.
    assert total_bytes[True] < total_bytes[False] / 3, total_bytes


def test_late_selection_reads_dictionary_and_selected_pages(tmp_path, range_server):
    rng = random.Random(1729)
    labels = [rng.randbytes(64).hex() for _ in range(256)]
    table = pa.table({"id": range(262144), "payload": rng.choices(labels, k=262144)})
    path = tmp_path / "dictionary.parquet"
    pq.write_table(
        table,
        path,
        use_dictionary=["payload"],
        compression="NONE",
        row_group_size=len(table),
        data_page_size=1024,
        write_batch_size=128,
        write_page_index=True,
    )
    predicate = (_resolved_col("id") >= 200000) & (_resolved_col("id") < 200100)
    actual = MicroPartition.read_parquet(f"{range_server[0]}/{path.name}", columns=["payload"], predicate=predicate)
    expected = table.slice(200000, 100).select(["payload"])
    assert actual.to_arrow().cast(expected.schema).equals(expected)
    column = pq.ParquetFile(path).metadata.row_group(0).column(1)
    start = column.dictionary_page_offset
    assert start is not None
    end = start + column.total_compressed_size
    fetched = sum(max(0, min(b, end) - max(a, start)) for _, a, b in range_server[1])
    # Includes any overlapping footer prefetch. Fetching the full dictionary
    # and a late data page must still save bytes versus the whole column.
    assert fetched < column.total_compressed_size * 0.75


def test_repeated_row_spanning_pages_uses_full_column_fallback(tmp_path, range_server):
    values = [None, [], [1, None]] * 21 + [[]]
    values[31] = list(range(80000))
    table = pa.table({"id": range(len(values)), "nested": pa.array(values, type=pa.list_(pa.int64()))})
    path = tmp_path / "large-nested.parquet"
    pq.write_table(table, path, data_page_size=1024, write_batch_size=32, write_page_index=True, use_dictionary=False)
    for uri in [str(path), f"{range_server[0]}/{path.name}"]:
        actual = MicroPartition.read_parquet(uri, columns=["nested"], predicate=_resolved_col("id") == 31)
        expected = table.slice(31, 1).select(["nested"])
        assert actual.to_arrow().cast(expected.schema).equals(expected)
