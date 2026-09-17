from __future__ import annotations

import pickle
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.daft import DistributedPhysicalPlan, FileFormat, PyDaftExecutionConfig, PyFormatSinkOption, WriteMode
from tests.conftest import get_tests_daft_runner_name


def assert_data_page_headers(path, version):
    metadata = pq.ParquetFile(path).metadata
    count = 0
    with path.open("rb") as file:
        for i in range(metadata.num_row_groups):
            for j in range(metadata.num_columns):
                column = metadata.row_group(i).column(j)
                file.seek(column.data_page_offset)
                # Both Arrow implementations put PageHeader.type first in compact
                # Thrift: field 1, i32 (0x15), then zigzag DATA_PAGE=0 / DATA_PAGE_V2=3.
                # Check real pages: PyArrow's metadata.format_version is "2.6"
                # for BOTH page versions, so it cannot verify this option.
                assert file.read(2) == (b"\x15\x00" if version == "1.0" else b"\x15\x06")
                assert not any(encoding.startswith("DELTA") for encoding in column.encodings)
                count += 1
    assert count > 0


@pytest.mark.parametrize("native", [True, False])
@pytest.mark.parametrize("version", [None, "1.0", "2.0"])
@pytest.mark.parametrize("compression", ["none", "snappy", "zstd"])
def test_page_version_reaches_each_writer(tmp_path, native, version, compression):
    rows = 2048
    expected = pa.table(
        {
            "id": range(rows),
            "label": [None if i % 17 == 0 else f"label-{i % 31}" * 16 for i in range(rows)],
            "items": pa.array([None if i % 7 == 0 else [i, None] for i in range(rows)]),
        }
    )
    options = {} if version is None else {"data_page_version": version}
    frame = daft.from_arrow(expected)
    if get_tests_daft_runner_name() == "ray":
        frame = frame.into_partitions(2)
    with daft.execution_config_ctx(native_parquet_writer=native, parquet_target_row_group_size=4096):
        frame.write_parquet(str(tmp_path), compression=compression, **options)
    files = sorted(tmp_path.glob("*.parquet"))
    assert files
    if get_tests_daft_runner_name() == "ray":
        assert len(files) == 2
    for file in files:
        assert_data_page_headers(file, version or "1.0")
    actual = pa.concat_tables([pq.read_table(file) for file in files]).sort_by("id").cast(expected.schema)
    assert actual.equals(expected)
    actual_daft = daft.read_parquet(str(tmp_path)).to_arrow().sort_by("id").cast(expected.schema)
    assert actual_daft.equals(expected)


@pytest.mark.parametrize("version", [None, "", "2", "3.0", 2, 2.0, True])
def test_invalid_page_version_fails_before_writing(tmp_path, version):
    target = tmp_path / "output"
    with pytest.raises((ValueError, TypeError), match="data_page_version"):
        daft.from_pydict({"id": [1]}).write_parquet(str(target), data_page_version=version)
    assert not target.exists()
    with pytest.raises((ValueError, TypeError)):
        PyFormatSinkOption.parquet(data_page_version=version)


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_page_version_in_serialized_write_plan(version):
    config = PyDaftExecutionConfig()
    frame = daft.range(0, 16, partitions=2)
    payloads = {}
    for page_version in ["1.0", "2.0"]:
        builder = frame._builder.write_tabular(
            root_dir="/tmp/daft-page-version-plan-test",
            partition_cols=None,
            write_mode=WriteMode.Append,
            write_success_file=False,
            file_format=FileFormat.Parquet,
            file_format_option=PyFormatSinkOption.parquet(data_page_version=page_version),
            compression="zstd",
            io_config=None,
            single_file=False,
        )
        plan = DistributedPhysicalPlan.from_logical_plan_builder(
            builder.optimize(config)._builder, "page-version", config
        )
        payloads[page_version] = plan.__reduce__()[1][0]
        if page_version == version:
            restored = pickle.loads(pickle.dumps(plan))
            factory, (payload,) = restored.__reduce__()
            assert factory.__name__ == "_from_serialized_parquet_data_page_v4"
            assert payload == payloads[page_version]
            with pytest.raises(ValueError, match="incompatible"):
                DistributedPhysicalPlan._from_serialized_local_write_buffer_v3(payload)
    assert payloads["1.0"] != payloads["2.0"]


@pytest.mark.parametrize("native", [True, False])
@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_page_version_in_partitioned_files(tmp_path, native, version):
    expected = {"id": list(range(64)), "group": [i % 2 for i in range(64)]}
    with daft.execution_config_ctx(native_parquet_writer=native):
        daft.from_pydict(expected).write_parquet(
            str(tmp_path), partition_cols=["group"], compression="zstd", data_page_version=version
        )
    files = sorted(tmp_path.rglob("*.parquet"))
    assert len(files) >= 2
    for file in files:
        assert_data_page_headers(file, version)
    actual = pq.read_table(tmp_path).sort_by("id").to_pydict()
    assert actual == expected


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="single_file requires the native runner")
@pytest.mark.parametrize("buffer_size", [4096, 4 * 1024 * 1024])
def test_v2_oversized_page_and_empty_file(tmp_path, buffer_size):
    expected = pa.table({"id": [1, 2, 3], "value": ["oversized" * 600_000, None, "tail"]})
    with daft.execution_config_ctx(native_parquet_writer=True, local_write_buffer_size_bytes=buffer_size):
        path = tmp_path / "large.parquet"
        daft.from_arrow(expected).write_parquet(
            str(path), single_file=True, compression="zstd", data_page_version="2.0"
        )
        assert_data_page_headers(path, "2.0")
        assert pq.read_table(path).cast(expected.schema).equals(expected)
        assert daft.read_parquet(str(path)).to_arrow().cast(expected.schema).equals(expected)
        empty = tmp_path / "empty.parquet"
        daft.from_arrow(expected).where(daft.col("id") < 0).write_parquet(
            str(empty), single_file=True, data_page_version="2.0"
        )
        assert pq.read_table(empty).cast(expected.schema).equals(expected.slice(0, 0))


def test_v2_duckdb_reader_compatibility(tmp_path):
    import duckdb

    expected = pa.table(
        {
            "id": [1, 2, 3],
            "day": [date(2026, 9, 1), None, date(2026, 9, 3)],
            "amount": pa.array([Decimal("1.25"), None, Decimal("-2.50")], type=pa.decimal128(12, 2)),
            "items": pa.array([[1, None], None, []], type=pa.list_(pa.int64())),
            "label": ["你好", None, "tail"],
        }
    )
    with daft.execution_config_ctx(native_parquet_writer=True):
        daft.from_arrow(expected).write_parquet(str(tmp_path), compression="zstd", data_page_version="2.0")
    with duckdb.connect(config={"threads": 2}) as connection:
        actual = connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY id", [str(tmp_path / "*.parquet")]
        ).to_arrow_table()
    assert actual.cast(expected.schema).equals(expected)
