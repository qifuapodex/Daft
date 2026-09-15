## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.6`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.6)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.7`
- **安装版本：** `daft==0.7.24+apodex.7`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

本版包含上一版之后合入 Apodex fork 的 **#7–#10 共 4 个 PR**；此前版本的修改继续保留，这里仅列出增量。

## 升级注意

本版为新增 EIO 配置更新了 execution config 和嵌入该配置的 distributed plan 的 pickle 版本。**Ray head / driver 和所有 worker 必须安装同一版本并重启相关进程**；旧的相关持久化 config / plan 需要用本版重新创建，不支持旧 pickle 自动迁移或混合版本运行。

Shuffle EIO 本地恢复与受限任务重试**默认启用**。Managed 共享集群调度需要显式配置外部 runtime adapter；已有实验性 AQE 和共享 shuffle 重建仍默认关闭。

## 本次更新

### [#7](https://github.com/qifuapodex/Daft/pull/7) — 可选共享集群调度与 worker drain

- `daft.set_runner_ray(cluster_scheduling=ClusterSchedulingConfig(...))` 可注入共享集群 runtime client，分别报告 execution 总 CPU 需求、worker 准入和节点使用情况；managed execution 不再写入或清空 Ray 的全局扩缩容请求。
- 增加节点 drain 的 prepare / status / cancel / retire 控制，并在最终派发时再次检查状态。Flight 数据依赖一直保留到查询静止且清理得到确认，任务空闲本身不会被当成可退役的依据。
- 完成的 managed execution 会自动退役无依赖的 Daft actors，等待 actor 死亡及 runtime 确认后释放记录；节点仍可被后续查询复用。失败或不确定的清理状态会保留保护，供外部 runtime 对账处理。
- 保留 standalone 路径，改进取消时的任务统计收尾和共享文件清理，包括清扫已死亡 producer actor 留在存活节点上的文件。

这是 Daft 侧的实验性协议，**生产节点缩容仍需要外部容量 runtime、节点保护及部署版本对应的 Ray 验证**。本地测试退役的是 Daft actors，没有删除机器。配置及协议见 [共享集群调度文档](https://github.com/qifuapodex/Daft/blob/apodex-0.7.24.7/docs/architecture/flotilla-shared-cluster-scheduling.md)。

### [#8](https://github.com/qifuapodex/Daft/pull/8) — 修复 cross join 输入带 LIMIT 时静默丢行

- Ray cross join 任一输入分支包含 LIMIT 时，先物化该分支再用于各分区配对，避免重复任务争用同一个 LIMIT 计数器；重试也会复用已选中的输入行。
- 典型复现中，100 行的 LIMIT 结果与 50 行 cross join，此前只返回 2,500 行，本版正确返回 **5,000 行且每个配对恰好出现一次**。
- 覆盖左右两侧及双侧 LIMIT、OFFSET、重复行、UDF pushdown barrier 和 worker crash。没有 LIMIT 的分支继续使用原有融合路径；保守检查可能为部分已有物化边界的分支增加一次物化。

### [#9](https://github.com/qifuapodex/Daft/pull/9) — 修复非整倍数拆分的分区数

- 修复 Ray `into_partitions(n)` 把多个输入分区扩为非整倍数时，worker 错误复用不同拆分因子的 pipeline。例如 **2 → 3** 此前返回 4 个分区，**8 → 9 / 12 / 15** 通常返回 16 个分区，本版严格返回请求的数量。
- 任务 plan fingerprint 纳入实际输出数，区分两种拆分因子；同时覆盖 `map_reduce` / `flight_shuffle`、空分区、链式拆分合并和 transient 重试，验证完整行内容及重复次数。

### [#10](https://github.com/qifuapodex/Daft/pull/10) — Shuffle EIO 本地恢复与有界任务重试

- Flight shuffle 的文件 EIO（`errno=5`）先在当前进程内恢复；读操作保持同一 descriptor 和 producer attempt，从最后成功返回的字节继续，避免重复输出已经交付的批次。
- 写 EIO 按原始字节处理短写，并校验、重写已完成的数据区域，再在**原始 descriptor** 上执行 `sync_data()` 后发布。每个恢复 writer 的缓冲上限为 4 MiB；该恢复要求适用于 local 文件及所有 shared durability 模式，包括 `none` / `background`。校验失败或同步失败会使任务失败，而不是反复同步后宣称修复。
- 默认最多 **6 次本地重试**，等待为 **1、2、4、8、16、32 秒**，单个完整本地预算累计等待 63 秒，不含 I/O 和后续任务重试。本地恢复失败后，符合重启条件的任务使用默认上限为 **3** 的任务重试；先前 transient / worker failure 也消耗同一累计任务失败预算。
- UDF、Python scan 和外部写入不会被自动重放。缺失文件、磁盘写满、只读挂载、权限错误不进入 EIO 专用重试分支；丢失 shuffle 的通用重建仍不属于本次修复。
- EIO 分类跨 IPC、Flight、Python pickle 和 Ray cause 保留。取消会唤醒写入退避等待；失败或取消的 IPC writer 后续不能把不完整输出报告为成功。
- 清理前等待本查询仍在执行的写入退出；worker 确认超时或失败时保留目录并记录错误，避免删除仍被写入的 spill 文件。其他查询的写入不阻塞 standalone 清理。

主要配置及默认值：

```python
import daft

daft.context.set_execution_config(
    flight_shuffle_eio_local_max_retries=6,
    flight_shuffle_eio_local_initial_backoff_ms=1000,
    flight_shuffle_eio_local_max_backoff_ms=32000,
    flight_shuffle_eio_max_retries=3,
    flight_shuffle_eio_initial_backoff_ms=1000,
    flight_shuffle_eio_max_backoff_ms=30000,
)
```

Shuffle 文件布局和 Flight request 格式保持不变。没有 EIO 时不增加额外数据读写或 fsync，但增加游标记录和 CRC32 计算；恢复写入需要完整数据区域读写及同步，活跃的同步重试仍占用 blocking-pool 线程。尚未进行优化构建下的生产吞吐和并发故障负载验证。预算范围、日志与运维限制见 [Shuffle EIO 文档](https://github.com/qifuapodex/Daft/blob/apodex-0.7.24.7/docs/optimization/shuffle.md#retrying-shuffle-eio-failures)。

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.7" "daft==0.7.24+apodex.7"
```

Ray 集群（head / driver 和每个 worker 都要安装同一版本）：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.7" "daft[ray]==0.7.24+apodex.7"
```

发布资产覆盖 Linux x86_64 / aarch64、macOS x86_64 / arm64、Windows x86_64，并提供源码包。

安装后确认 `daft.__version__` 为 `0.7.24+apodex.7`。

**完整差异：** [apodex.6 → apodex.7](https://github.com/qifuapodex/Daft/compare/apodex-0.7.24.6...apodex-0.7.24.7)
