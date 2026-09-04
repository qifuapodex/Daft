## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.1`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.2`
- **安装版本：** `daft==0.7.24+apodex.2`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

相对 `0.7.24+apodex.1`，本版只新增 [#7459](https://github.com/Eventual-Inc/Daft/pull/7459)；此前版本的修复请查看[上一版 Release Note](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24)，这里不再重复。

---

## 本次更新：`flight_shuffle` 合并分区修复

### #7459 — `into_partitions` 合并分区不再 panic

当 `shuffle_algorithm="flight_shuffle"` 时，`into_partitions(n)` 只要 `n` 小于输入分区数（合并 / coalesce 分支），此前就会在调度器线程中 panic，作业在数据搬运开始前失败：

```text
thread 'Daft-Scheduler' panicked at .../backends/flight.rs:
expected flight partition ref
   flight::read_inputs_from_refs
   IntoPartitionsNode::coalesce_tasks
```

Python 侧只会看到裸的 `RayTaskError(DaftCoreException)`。分区数相等或拆分（`n >=` 输入分区数）不受影响，因此问题通常只会在输入分区较多时暴露。

复现形态：

```python
import tempfile

import daft

daft.set_runner_ray()
with tempfile.TemporaryDirectory() as tmp:
    with daft.execution_config_ctx(
        shuffle_algorithm="flight_shuffle",
        flight_shuffle_dirs=[tmp],
    ):
        df = daft.range(10_000, partitions=8).into_partitions(1)
        print(df.count_rows())
```

### 根因与修复

`ShuffleContext` / `ShuffleBackend::build_refs_task_builder` 以前按节点配置的 backend 选择读路径：节点配置为 Flight 时一律使用 flight reader。但 `IntoPartitionsNode` 的 coalesce 分支拿到的是普通 Ray object refs，并不是 `FlightPartitionRef`，无条件 downcast 因而触发 panic。

本次修复包括：

- 按 refs 的实际类型选择读路径：flight refs 使用 `shuffle_read(Flight)`，普通 refs 使用 `in_memory_scan` + psets；混合类型返回明确的 `DaftError`。
- 将 flight helper 中的 `.expect` 改为错误返回，避免同类问题退化成无上下文的 `RayTaskError`。
- coalesce / equal 分支的本地 concat 始终使用 Ray backend，避免额外的 flight write 以及后续节点收到终端 `FlightPartitionRef`。

回归测试位于 `tests/dataframe/test_into_partitions.py`，覆盖 `flight_shuffle` 下的 coalesce / equal / split（`into_partitions(1/3/7/8/9/16)`），以及 coalesce 后继续执行 `with_column` / `agg` / `groupby`。

---

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.2" "daft==0.7.24+apodex.2"
```

Ray 集群的 head 和每个 worker 都要安装同一版本：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.2" "daft[ray]==0.7.24+apodex.2"
```

相对上一版，发布资产新增 Linux aarch64 wheel。本版资产覆盖：

- Linux x86_64、Linux aarch64
- macOS x86_64、macOS arm64
- Windows x86_64

安装后确认版本：

```python
import daft

print(daft.__version__)
```

应打印 `0.7.24+apodex.2`。
