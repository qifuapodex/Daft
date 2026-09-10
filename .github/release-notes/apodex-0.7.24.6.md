## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.5`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.5)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.6`
- **安装版本：** `daft==0.7.24+apodex.6`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

本版包含上一版之后合入 Apodex fork 的 **#2–#6 共 5 个 PR**；此前版本的修改继续保留，这里仅列出增量。

## 升级注意

本版修改了 execution config、distributed plan 和 shuffle input 的 pickle 格式。**Ray head / driver 和所有 worker 必须安装同一版本并重启相关进程**；即使不开启 AQE，也不支持与旧版本混用。持久化的相关 config / plan 需要用本版重新创建，旧 pickle 不会自动迁移。

实验性 shuffle 任务合并和共享 shuffle 重建均**默认关闭**，需要分别显式开启。

## 本次更新

### [#2](https://github.com/qifuapodex/Daft/pull/2) — 正确合并零字节输出分区

- 修复把 `size_bytes() == 0` 当作未知大小的问题，让空输出可以和其他片段合并，减少多空桶聚合中的 Ray objects 和结果获取 RPC。
- 缓冲片段数上限为 1024，同时约束空分区和极小分区；保留固定分区槽位、空结果 schema、输出 metadata 与任务完成统计。

### [#3](https://github.com/qifuapodex/Daft/pull/3) — 修复 native runner 进度条日志死锁

- 统一 Python GIL 与 indicatif 进度条锁的获取顺序，修复 dashboard / 日志并发下反复 `write_parquet()` 可能永久挂起的问题。
- 增加子进程回归测试，覆盖 dashboard 启动后的连续写入场景。

### [#4](https://github.com/qifuapodex/Daft/pull/4) — 保留 generator 异常并正确重试

- `read_generator` 保留原始 Python 异常及 cause，六类 `DaftTransientError` 子类可使用 Flotilla 现有重试预算与退避机制；永久错误仍直接失败。这补齐了 apodex.5 发布说明中明确未覆盖的 generator 路径。
- 将共享 pipeline 的失败传播给所有尚未完成及已排队的输入，避免部分输出被误报为成功。
- 不会自动把任意 SDK 异常识别为 transient，也不回滚 generator / UDF 的外部副作用。

### [#5](https://github.com/qifuapodex/Daft/pull/5) — Shuffle 诊断与可选任务合并

- 增加 Flight shuffle 的结构化 INFO 诊断、读写字节数与耗时统计；未开启对应 INFO 日志或 AQE 时跳过额外统计开销。
- `experimental_shuffle_aqe=True` 可按实际未压缩输出大小合并符合条件的内部 aggregation / distinct exchange 相邻桶；默认建议目标为 256 MiB，每组最多 64 个桶，并保留任务并行度下限。
- 显式用户 repartition 和 join 输入子树不参与自适应合并；此功能不减少 map 文件数量，也不是完整的 Spark AQE。
- 合并桶的 shared reader 复用文件句柄并使用有界预取；大型 RPC 请求按最多 64K refs 分块，避免超过 tonic 的 4 MiB 请求限制，后续分块错误仍会传播。

启用任务合并：

```python
import daft

daft.context.set_execution_config(
    shuffle_algorithm="flight_shuffle",
    experimental_shuffle_aqe=True,
)
```

配置和诊断方式见 [Shuffle 文档](https://github.com/qifuapodex/Daft/blob/apodex-0.7.24.6/docs/optimization/shuffle.md)。尚无多节点 Lustre / NFS 或 1 TiB 性能 A/B 结果；任务合并可能降低 I/O 并发，需结合实际存储测量。

### [#6](https://github.com/qifuapodex/Daft/pull/6) — 可选共享 shuffle 重建

- 已确认成功的共享 map 文件丢失后，可保留并重放符合条件的 producer，再让可安全重启的 consumer 读取替代输出；同时支持 AQE 合并桶读取。
- 跨 Flight、Python / Ray 保留结构化缺失文件信息，并在实际派发任务前重新绑定输入，协调并发重建、消费者等待和重试预算。
- `flight_shuffle_recovery_max_attempts` 默认为 `0`；设为正数才开启，例如 `daft.context.set_execution_config(flight_shuffle_recovery_max_attempts=2)`。可配置重建并发、依赖深度、consumer 失败轮数及保留输入上限。
- **仅覆盖受限、可等价重放的 producer；`read_parquet → repartition` 等外部 scan 链路不在覆盖范围内。** 这不是通用 stage 回滚或任意 worker / 存储故障恢复。
- 开启后可能延长输入 / ObjectRef 的生命周期；保留上限达到后跳过新重建配方。等待、排队和执行没有 recovery deadline，`flight_shuffle_recovery_wait_warn_ms` 只输出等待诊断。

支持范围和限制见 [共享 Shuffle 重建设计](https://github.com/qifuapodex/Daft/blob/apodex-0.7.24.6/docs/architecture/flotilla-shuffle-recovery.md)。合入前验证使用单机本地 Ray，尚未覆盖多节点存储故障和生产规模内存 / 吞吐验证。

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.6" "daft==0.7.24+apodex.6"
```

Ray 集群（head / driver 和每个 worker 都要安装同一版本）：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.6" "daft[ray]==0.7.24+apodex.6"
```

发布资产覆盖 Linux x86_64 / aarch64、macOS x86_64 / arm64、Windows x86_64，并提供源码包。

安装后确认 `daft.__version__` 为 `0.7.24+apodex.6`。

**完整差异：** [apodex.5 → apodex.6](https://github.com/qifuapodex/Daft/compare/apodex-0.7.24.5...apodex-0.7.24.6)
