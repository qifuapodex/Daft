## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.4`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.4)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.5`
- **安装版本：** `daft==0.7.24+apodex.5`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

本版继续跟踪 [Eventual-Inc/Daft#7464](https://github.com/Eventual-Inc/Daft/pull/7464) 和 [qifuapodex/Daft#1](https://github.com/qifuapodex/Daft/pull/1)，且仅列出相对上一版的增量。发布时，fork PR #1 的 head 仍为 `.4` 已合入的 `c909dd1a`，因此没有重复列出其历史修改；本版代码增量来自 #7464 的新提交 `e454a11f`。

---

## 本次更新

### Eventual-Inc/Daft#7464 — 20k-core 验证后的 Flight shuffle 修正与优化

- 共享 shuffle 清理现在会独立等待所有 node-local 与 shared 删除任务；节点离线导致本地删除失败时，不再中断对唯一共享副本的清理，并会醒目记录共享目录清理失败。
- Gather 为同一输入的并发 state 统一分配互不重复的 partition ref ID，并共享同一个 attempt token，修复多 state 汇集时可能发生的分区 ID 冲突和数据丢失。
- worker 按 shuffle 缓存已读取的共享文件索引区域，缓存上限为 64 MiB，并在 shuffle 释放时一并清除，减少不同 reduce task 对相同索引的重复读取。
- 当大型集群中最窄 shuffle 的输出分区只能使用不到一半 CPU、且至少会闲置 64 个 CPU 时给出容量提示，避免 reduce 阶段的大量资源闲置。
- 更新共享存储 durability 的实测说明，并扩充写入 durability、稀疏 cell 与 4096 分区生产形状的 benchmark。

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
