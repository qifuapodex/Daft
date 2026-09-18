## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.7`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.7)（含 2026-09-16 同版本重发的回归修复）
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** [`apodex-0.7.24.8`](https://github.com/qifuapodex/Daft/tree/apodex-0.7.24.8)
- **安装版本：** `daft==0.7.24+apodex.8`
- **发布渠道：** 仅本 GitHub Release 的 Assets，**不会上传 pypi.org**

本版新增 **#17–#20 共 4 个 PR**。先发布日志，各平台安装包在构建完成并校验后立即上传，Linux 无需等待其他平台。

## 升级注意

**Ray head / driver 和所有 worker 必须统一升级到 apodex.8，并重启相关进程。apodex.7 及更早版本持久化的 execution config、本地及分布式执行计划需要重新创建。** 新版会在解码旧 pickle 前明确报出不兼容；即使继续使用默认 Parquet V1 写入，也需要统一升级。

本版 execution config 使用 `local_write_buffer_v3` 序列化边界，本地及分布式计划使用 `parquet_data_page_v4` 边界。这些变化影响内部 pickle，不要求重写已有 Parquet 文件。Python 端无需因为 Rust Arrow / Parquet 升级到 60 而安装同版本号的 PyArrow。

本地文件写缓冲默认从 **4 KiB 增大到 4 MiB，按每个打开的 writer 分配**；高并发写入时需计入这部分内存。Parquet Data Page V2 **默认关闭**，仍默认写出 V1。现有 EIO 恢复、任务重试及实验性功能的开关保持原有默认值。

## 本次更新

### [#17](https://github.com/qifuapodex/Daft/pull/17) — 可配置的本地文件写缓冲，默认 4 MiB

- 新增 `local_write_buffer_size_bytes`，支持 `daft.set_execution_config(...)` 和 `daft.execution_config_ctx(...)`。作用于 native Parquet、CSV、JSON 的本地及挂载路径写入，并随执行配置传递到分布式 worker；空输出也使用该配置。
- 默认增大缓冲以合并小写入，减少本地和 JuiceFS 等挂载文件系统上的调用开销；设为 `4096` 可恢复原来的容量。参数必须为正数，省略或传 `None` 保留当前配置。
- JSON writer 关闭时显式刷新底层缓冲，避免最终刷新错误被析构过程隐藏。对象存储 multipart、PyArrow fallback writer、shuffle 缓冲、row group 和目标文件大小不受该配置影响。

```python
import daft

with daft.execution_config_ctx(local_write_buffer_size_bytes=1024 * 1024):
    df.write_parquet("/mnt/juicefs/output")
```

### [#18](https://github.com/qifuapodex/Daft/pull/18) — Arrow 60 与 Parquet 元数据复用、按页读取

- Rust Arrow / Parquet 升级至 **60.0.0**。native reader 在当前计划内复用已解析 footer，减少 schema 推断和实际读取之间重复的元数据 I/O；无需新增 Python 参数，也不是跨查询全局缓存。
- 将已有行选择传到实际 I/O：聚集过滤和 `limit` 可利用 offset index 只读取覆盖所需行的页面，保留必要字典页，并合并相邻字节范围；远端选择性读取也按所需范围批量发起请求。
- 没有 offset index、可能跨页的重复嵌套记录或命中过于分散时回退整列读取；全量扫描沿用原有路径。尚未增加基于 column-index min/max 的页面谓词裁剪，极宽表和高延迟存储仍需评估索引开销。

PR 中 native runner、预热缓存下的 30 样本对照显示：聚集过滤中位耗时降低约 **42% / 25%**，`limit(100)` 降低约 **55% / 45%**，宽表单列投影降低约 **15% / 20%**（本地 / JuiceFS）。这些是特定负载的测量，不能外推到冷缓存或 Ray 高并发；**写入尚无稳定提速证据**。依赖升级和性能测量细节见 PR。

### [#19](https://github.com/qifuapodex/Daft/pull/19) — 可选 Parquet Data Page V2 写入

- `write_parquet` 新增 `data_page_version="1.0" | "2.0"`，默认仍为 `"1.0"`。native 和 PyArrow writer 均支持，可用于目录、分区目录及 native runner 的单文件输出，并与压缩级别、列级压缩组合。
- V2 让 native 压缩路径直接读取已编码 values，减少中转复制；保持逻辑数据和现有 fallback 编码策略，文件字节、页面布局及大小可能变化。下游 reader 需要支持 V2 及所选编码、压缩算法。
- **native V2 可能保留明显更大的压缩缓冲**，高可压缩数据、大 row group、多列并发时尤其需要验证内存，必要时减小 `parquet_target_row_group_size` 和写入并发。

```python
df.write_parquet("output_dir", compression="zstd", data_page_version="2.0")
```

此前同一 release 二进制中的 V1/V2 对照原型，在本地字符串负载下测得 ZSTD / Snappy 中位耗时约降低 **10.7% / 5.8%**；正式参数接入已验证正确性，未重新测量最终接口的 release 吞吐。**JuiceFS 未测到稳定的端到端提速**，因此本版保持 V1 默认值。

### [#20](https://github.com/qifuapodex/Daft/pull/20) — 降低 shuffle IPC 与恢复校验的正常路径开销

- 复用 one-shot 数据块 CRC，减少重复校验计算；streaming 路径在支持的 x86_64 CPU 上使用更紧凑的 CRC 状态，其他环境保留 fallback。
- 合并小 IPC 写入及结束操作；单个不超过 8 KiB 的输入 batch 可在关闭时直接完成，省去异步转发任务。大输入和后续输入继续使用带背压的后台写入。
- 较大的 Flight body 使用独立、受限的读取缓冲，避免先清零再覆盖。保留 EIO 恢复校验、原始 descriptor 同步、短写处理、取消及 drain 行为；使用方式不变。

小批次和部分 LZ4 写入场景已有收益，但**尚未达到相对 EIO 引入前基线“所有指标回退不超过 1%”的目标**：本地 Gather 和部分宽表写入指标仍有回退或不确定性。历史对照还包含 Arrow 升级等变化，不能解释为单独的 CRC 收益；本版不承诺所有 shuffle 场景都会变快。可复跑的基准工具及测量限制见 PR。

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --upgrade --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.8" "daft==0.7.24+apodex.8"
```

Ray 集群（head / driver 和所有 worker 都需安装并重启）：

```bash
pip install --upgrade --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.8" "daft[ray]==0.7.24+apodex.8"
```

发布资产覆盖 Linux x86_64 / aarch64、macOS x86_64 / arm64、Windows x86_64，并提供源码包。构建期间尚未完成的平台会陆续补齐。安装后确认 `daft.__version__` 为 `0.7.24+apodex.8`。

**完整差异：** [apodex.7 → apodex.8](https://github.com/qifuapodex/Daft/compare/apodex-0.7.24.7...apodex-0.7.24.8)
