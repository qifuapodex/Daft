from __future__ import annotations

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq
import pytest

import daft
from daft.context import get_context
from daft.daft import PyDaftExecutionConfig


def test_local_write_buffer_default_and_context_restore():
    assert PyDaftExecutionConfig().local_write_buffer_size_bytes == 4 * 1024 * 1024
    original = get_context().daft_execution_config.local_write_buffer_size_bytes
    with daft.execution_config_ctx(local_write_buffer_size_bytes=128 * 1024):
        snapshot = get_context().daft_execution_config
        with daft.execution_config_ctx(local_write_buffer_size_bytes=None):
            assert get_context().daft_execution_config.local_write_buffer_size_bytes == 128 * 1024
        with daft.execution_config_ctx(local_write_buffer_size_bytes=4096):
            assert get_context().daft_execution_config.local_write_buffer_size_bytes == 4096
            assert snapshot.local_write_buffer_size_bytes == 128 * 1024
        assert get_context().daft_execution_config.local_write_buffer_size_bytes == 128 * 1024
    assert get_context().daft_execution_config.local_write_buffer_size_bytes == original


@pytest.mark.parametrize("size", [0, -1])
def test_local_write_buffer_rejects_nonpositive_sizes(size):
    original = get_context().daft_execution_config.local_write_buffer_size_bytes
    with pytest.raises((ValueError, OverflowError)), daft.execution_config_ctx(local_write_buffer_size_bytes=size):
        pass
    assert get_context().daft_execution_config.local_write_buffer_size_bytes == original


@pytest.mark.parametrize("size", [None, 4096, 128 * 1024])
@pytest.mark.parametrize("format", ["parquet", "csv", "json"])
def test_native_file_write_buffer_roundtrip(tmp_path, size, format):
    # Text output crosses the default buffer boundary; a final partial buffer
    # must also be flushed. Independent readers verify every output value.
    data = {"id": list(range(8193)), "payload": [f'{i}: "value", 汉字 ' + "x" * 512 for i in range(8193)]}
    frame = daft.from_pydict(data)
    writer = getattr(frame, f"write_{format}")
    reader = {"parquet": papq.read_table, "csv": pacsv.read_csv, "json": pajson.read_json}[format]
    with daft.execution_config_ctx(native_parquet_writer=True, local_write_buffer_size_bytes=size):
        writer(str(tmp_path))
    files = list(tmp_path.glob(f"*.{format}"))
    assert files
    result = pa.concat_tables([reader(path) for path in files]).sort_by("id")
    assert result.to_pydict() == data


@pytest.mark.parametrize("format", ["parquet", "csv"])
def test_empty_native_file_write_with_custom_buffer(tmp_path, format):
    frame = daft.from_pydict({"id": [1]}).where(daft.col("id") < 0)
    with daft.execution_config_ctx(native_parquet_writer=True, local_write_buffer_size_bytes=128 * 1024):
        getattr(frame, f"write_{format}")(str(tmp_path))
    files = list(tmp_path.glob(f"*.{format}"))
    assert len(files) == 1
    reader = papq.read_table if format == "parquet" else pacsv.read_csv
    assert reader(files[0]).num_rows == 0
