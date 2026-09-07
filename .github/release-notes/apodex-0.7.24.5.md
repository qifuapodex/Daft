## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.4`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.4)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.5`
- **安装版本：** `daft==0.7.24+apodex.5`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

本版继续跟踪 [Eventual-Inc/Daft#7464](https://github.com/Eventual-Inc/Daft/pull/7464) 和 [qifuapodex/Daft#1](https://github.com/qifuapodex/Daft/pull/1)，且仅列出相对上一版的增量；后者是提交到 Apodex fork 的 PR，并非官方仓库 PR。本版代码增量分别来自 #7464 的 `e454a11f` 和 fork PR #1 的 `cdd77703`。

---

## 本次更新

### Eventual-Inc/Daft#7464 — 20k-core 验证后的 Flight shuffle 修正与优化

- 共享 shuffle 清理现在会独立等待所有 node-local 与 shared 删除任务；节点离线导致本地删除失败时，不再中断对唯一共享副本的清理，并会醒目记录共享目录清理失败。
- Gather 为同一输入的并发 state 统一分配互不重复的 partition ref ID，并共享同一个 attempt token，修复多 state 汇集时可能发生的分区 ID 冲突和数据丢失。
- worker 按 shuffle 缓存已读取的共享文件索引区域，缓存上限为 64 MiB，并在 shuffle 释放时一并清除，减少不同 reduce task 对相同索引的重复读取。
- 当大型集群中最窄 shuffle 的输出分区只能使用不到一半 CPU、且至少会闲置 64 个 CPU 时给出容量提示，避免 reduce 阶段的大量资源闲置。
- 更新共享存储 durability 的实测说明，并扩充写入 durability、稀疏 cell 与 4096 分区生产形状的 benchmark。

### qifuapodex/Daft#1 — 重试 UDF 内部抛出的 transient error

- `UDFException` 序列化时显式保留可 pickle 的 `__cause__`，使 UDF 中的限流、DNS 或 socket 类瞬时错误跨进程和 Ray 传输后仍能保留原始异常类型；不可 pickle 的 cause 会安全丢弃，不会用序列化错误覆盖用户异常。
- `DaftError::is_transient()` 现在以有限深度遍历 Ray 的 `.cause` 与 Python 的 `__cause__` 链，可识别 `RayTaskError → UDFException → DaftTransientError` 的两层包装并触发已有的退避重试。
- 新增 3 个 Ray/Python contract tests，覆盖 cause 的 pickle 往返、两层异常链识别以及不可序列化 cause 的降级行为。

`read_generator` 路径仍会更早丢失异常类型并以 `DaftCoreException` 到达 driver，不在本次修复范围内。

---

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.5" "daft==0.7.24+apodex.5"
```

Ray 集群（head 和每个 worker 都要安装同一版本）：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.5" "daft[ray]==0.7.24+apodex.5"
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

应打印 `0.7.24+apodex.5`。
