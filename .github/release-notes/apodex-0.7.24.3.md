## 版本说明

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **相对版本：** [`0.7.24+apodex.2`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24.2)
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.3`
- **安装版本：** `daft==0.7.24+apodex.3`
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

本版只新增 [#7463](https://github.com/Eventual-Inc/Daft/pull/7463)；此前版本的修复继续保留，这里不再重复。

---

## 本次更新：Parquet 压缩级别

`DataFrame.write_parquet()` 新增 `compression_level` 参数，可以显式控制 Parquet 压缩级别：

```python
df.write_parquet(
    "output/",
    compression="zstd",
    compression_level=9,
)
```

- 支持 `zstd`（1–22）、`gzip`（0–9）和 `brotli`（0–11）。
- 同时覆盖 native writer 和 PyArrow fallback。
- `compression_level` 也会应用到 `column_compression` 中支持级别的列；不支持级别的 codec 不受影响。
- 对越界级别，以及只使用 `snappy` 等无级别 codec 却传入级别的情况，统一抛出明确错误。
- 默认值仍为 `None`；不传该参数时保持原有行为。

示例：默认使用 snappy，仅给文本列使用 zstd level 9：

```python
df.write_parquet(
    "output/",
    compression="snappy",
    column_compression={"text": "zstd"},
    compression_level=9,
)
```

---

## 安装

让 pip 从本 Release 的 Assets 中选择当前平台对应的 wheel：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.3" "daft==0.7.24+apodex.3"
```

Ray 集群（head 和每个 worker 都要安装同一版本）：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.3" "daft[ray]==0.7.24+apodex.3"
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

应打印 `0.7.24+apodex.3`。
