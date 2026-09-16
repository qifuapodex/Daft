# Configuration

Configure the execution backend, Daft in various ways during execution, and how Daft interacts with storage.

## Setting the Runner

Control the execution backend that Daft will run on by calling these functions once at the start of your application.

::: daft.set_runner_native
    options:
        heading_level: 3

::: daft.set_runner_ray
    options:
        heading_level: 3

::: daft.get_or_create_runner
    options:
        heading_level: 3

## Checking the Runner

Check the execution backend that Daft is currently using.

::: daft.get_or_infer_runner_type
    options:
        heading_level: 3

## Setting Configurations

Configure Daft in various ways during execution.

::: daft.context.set_planning_config
    options:
        heading_level: 3

::: daft.context.planning_config_ctx
    options:
        heading_level: 3

::: daft.context.set_execution_config
    options:
        heading_level: 3

::: daft.context.execution_config_ctx
    options:
        heading_level: 3

## Local file write buffers

Native Parquet, CSV, and JSON writers use a 4 MiB buffer per open file when
writing to local paths, including mounted filesystems such as JuiceFS. Larger
buffers combine small writes before they reach the filesystem. Buffer memory
scales with the number of concurrently open files.

Set a positive byte count before executing the write:

```python
with daft.execution_config_ctx(local_write_buffer_size_bytes=1024 * 1024):
    df.write_parquet("/mnt/juicefs/output")
```

Use `daft.set_execution_config(local_write_buffer_size_bytes=...)` to set the
value globally for subsequent executions. Omitting the option, or passing
`None`, preserves the current setting. Setting `4096` restores the previous
4 KiB capacity. The setting does not change row group or target file sizes,
object storage multipart uploads, PyArrow fallback writers, or shuffle files.

## I/O Configurations

Configure behavior when Daft interacts with storage (e.g. credentials, retry policies and various other knobs to control performance/resource usage)

These configurations are most often used as inputs to Daft when reading I/O functions such as in [I/O](io.md).

::: daft.daft.IOConfig
    options:
        filters: ["!^_"]

::: daft.io.S3Config
    options:
        filters: ["!^_"]

::: daft.io.S3Credentials
    options:
        filters: ["!^_"]

::: daft.io.GCSConfig
    options:
        filters: ["!^_"]

::: daft.io.AzureConfig
    options:
        filters: ["!^_"]

::: daft.io.HTTPConfig
    options:
        filters: ["!^_"]

::: daft.io.UnityConfig
    options:
        filters: ["!^_"]

::: daft.io.HuggingFaceConfig
    options:
        filters: ["!^_"]

::: daft.io.TosConfig
    options:
        filters: ["!^_"]

::: daft.io.CosConfig
    options:
        filters: ["!^_"]
