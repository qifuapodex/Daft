## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.3`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.3)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.4`
- **安装版本：** `daft==0.7.24+apodex.4`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

相对上一版，本版只新增 [Eventual-Inc/Daft#7464](https://github.com/Eventual-Inc/Daft/pull/7464) 和 [qifuapodex/Daft#1](https://github.com/qifuapodex/Daft/pull/1)；后者是提交到 Apodex fork 的 PR，并非官方仓库 PR。此前版本的修改这里不再重复。

---

## 本次更新

### Eventual-Inc/Daft#7464 — Flight shuffle 共享文件系统

- `flight_shuffle` 可把 repartition 类 shuffle 输出写到所有节点可见的 POSIX 共享目录，让其他 worker 直接读取，并在写入 worker 丢失后继续访问已发布的数据。
- 针对任务重试和不完整写入增加 attempt 隔离、分区校验、请求完整性检查与跨读路径验证，避免重复读取或静默返回损坏/截断数据。
- `auto` 读路径可在共享目录与 Flight RPC 之间回退；完成或失败的 query 都会清理 shuffle 文件及 worker 注册信息。
- 减少索引预留、目录创建和 `fsync` 带来的写入开销，并限制共享目录并发读取。

启用示例：

```python
import daft

daft.context.set_execution_config(
    shuffle_algorithm="flight_shuffle",
    flight_shuffle_placement="shared_only",
    flight_shuffle_shared_dir="/mnt/shared",
)
```

新增的调节项包括 `flight_shuffle_shared_durability`（`background` / `none` / `sync`）、`flight_shuffle_read_source`（`auto` / `rpc` / `shared`）和 `flight_shuffle_shared_read_concurrency`。Gather 与 `into_partitions` 仍使用 node-local 布局。

### qifuapodex/Daft#1 — Flotilla 调度器可靠性与扩容路径优化

- 对明确归类为 transient 的任务错误进行有限次数、指数退避重试；基础设施故障也使用独立的有限重试预算。
- 重试时优先避开刚刚失败的 worker；硬亲和目标已经消失时退回 spread 调度，避免任务永久等待。
- 首次 Ray worker 刷新后，后续刷新移到后台执行，避免扩容期间阻塞调度循环；同时补充刷新耗时日志。
- 修正 retryable failure 的统计生命周期，避免尚待重试的 operator 被提前标记结束。

默认 transient 重试上限为 3、基础设施重试上限为 10，可分别通过 `DAFT_FLOTILLA_TASK_MAX_TRANSIENT_RETRIES` 和 `DAFT_FLOTILLA_TASK_MAX_INFRA_RETRIES` 调整。

---

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.4" "daft==0.7.24+apodex.4"
```

Ray 集群（head 和每个 worker 都要安装同一版本）：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.4" "daft[ray]==0.7.24+apodex.4"
```

发布资产覆盖：

- Linux x86_64、Linux aarch64
- macOS x86_64、macOS arm64
- Windows x86_64

安装后确认版本：

```python
import daft

print(daft.__version__)
```

应打印 `0.7.24+apodex.4`。
