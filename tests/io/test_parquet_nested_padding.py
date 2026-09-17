from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft


@pytest.mark.parametrize(
    "values",
    [
        pa.array([None, [], [None, "a"], ["b"], []], type=pa.list_(pa.string())),
        pa.array([None, [], [None, [], [1, None]], [[2]], [[]]], type=pa.list_(pa.list_(pa.int64()))),
        pa.array(
            [None, [], [None, {"n": None, "items": []}], [{"n": 2, "items": [None, "a"]}], []],
            type=pa.list_(pa.struct([("n", pa.int64()), ("items", pa.list_(pa.string()))])),
        ),
        pa.array(
            [None, [], [("a", None), ("b", [])], [("c", [1, None])], []],
            type=pa.map_(pa.string(), pa.list_(pa.int64())),
        ),
    ],
    ids=["list_string", "list_list", "list_struct_list", "map_list"],
)
@pytest.mark.parametrize("use_dictionary", [False, True])
@pytest.mark.parametrize("data_page_version", ["1.0", "2.0"])
@pytest.mark.parametrize("filtered", [False, True])
def test_nested_null_padding_across_pages(tmp_path, values, use_dictionary, data_page_version, filtered):
    # Null/empty outer containers must not create phantom child entries. Cross
    # both page and row-group boundaries, then exercise read_records/skip_records.
    values = pa.concat_arrays([values] * 31)
    table = pa.table({"id": range(len(values)), "values": values})
    path = tmp_path / "nested.parquet"
    pq.write_table(
        table,
        path,
        row_group_size=17,
        data_page_size=64,
        write_batch_size=4,
        write_page_index=True,
        use_dictionary=use_dictionary,
        data_page_version=data_page_version,
    )
    expected = daft.from_arrow(table)
    actual = daft.read_parquet(str(path), _chunk_size=7)
    if filtered:
        expected = expected.where(daft.col("id") % 3 != 0).limit(39)
        actual = actual.where(daft.col("id") % 3 != 0).limit(39)
    assert actual.to_pydict() == expected.to_pydict()
