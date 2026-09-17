# 延后单个小批次的 shuffle writer 启动

状态：构建、正确性测试及全部预定 ABBA 已完成。结果区分观测 min、median 和统计不确定性；没有删去慢样本。

**单批小文件的调度优化有效，CRC 保持开启；完整的 1% 退化要求尚未达到。**
并发 8 时，独立优化对照的 min/median 改善 13.97%/19.80%；相对 EIO 前旧版，
完整存储对照的小文件 min/median 在本地和 JuiceFS 都改善。但本地 FlightGather
仍有明确回归，其他写入配置也存在超过 1% 的观察差异或较宽的置信区间。
即使把增量矩阵加到每侧每格 256 个样本，也不能宣布全部场景平稳或等价。

## 改动与作用范围

`InProgressShuffleCache` 的首个输入不超过 8 KiB 时，先留在原有有界队列。
如果随后直接关闭，则由关闭 future 消费队列；IPC 编码、写入和关闭仍在
blocking pool 中执行。这样省去一次异步转交，并让单批输入稳定使用已有的
`write_and_close`，避免写入与关闭之间的时序竞争产生额外 blocking 操作。

第二个输入到达或首个输入大于 8 KiB 时立即启动后台 writer，保留流式写入
与生产者的重叠。已启动路径用一次原子读取绕过状态锁。首个小输入仍占原来的
队列容量，没有新增缓存队列或等待计时器；它会等到第二次输入或关闭才开始写入。
共享 active-write 登记同时覆盖待启动状态和后台操作，取消后仍等待 blocking I/O 排空。

此次生产代码只改 `src/daft-shuffles/src/shuffle_cache.rs`，保持当前 CRC 实现、
重试预算、故障恢复及持久性语义。CRC 关闭对照只用于诊断。

## 版本、负载和统计口径

- 历史验收基线：引入 EIO 重试前的 `c6217a8c09cbfe773f88374adf46ff2a7fcb1195`，Arrow-rs 59。
- 独立优化基线：此前的 `3ec21f0ec3a1bbaf622833992ac2da6646f1dca5` 生产源码，Arrow-rs 60。
  `e063e2ab646c22210f3f0f5fe6cd401aac3a3342` 只增加报告，与之生产代码一致。
- 测试版本：`e063e2ab6` 加 [final.patch（gzip）](final.patch.gz)，Arrow-rs 60。
  [builds-final.json](builds-final.json) 固定 release binary、wheel 和实际扩展的哈希。
  [source-final-manifest.json](source-final-manifest.json) 固定 1,233 个源码及配置文件。
- 历史对照包含 EIO、此前优化及 Arrow 版本变化；独立优化对照用于分离本次改动的影响。
- 重点本地单批测试：12 轮 ABBA，每 job 一次 warmup、八次正式样本，每侧每配置 192 个正式样本。
- 完整存储矩阵：八轮对称顺序 old-local、new-local、old-JuiceFS、new-JuiceFS、
  new-JuiceFS、old-JuiceFS、new-local、old-local；每 job 一次 warmup、三次正式样本。
  每侧、每存储、每负载 48 个正式样本。四种投影视图复用样本，不能当作独立实验。
- 独立多批/大批回归：八轮 ABBA，每 job 一次 warmup、三次正式样本，每侧每配置 48 个正式样本。
- 上述增量回归的部分 min/median 观察退化超过 1%，但 median 区间较宽。
  因此预先固定再跑一组十六轮 ABBA，每 job 一次 warmup、八次正式样本，每侧每配置 256 个正式样本。
  复核覆盖原来的全部十二格，代码和 binary 保持不变；两组结果分别保留，不合并或用后者替换前者。
- 同一 binary 的 A/A 噪声对照：固定选择 `stream-tiny/c1-lz4`、`stream-wide/c8-none`、
  `oneshot-small/c8-lz4`，每格十六轮、每 job 八次正式样本，每侧 256 个样本。
  两个标签都加载同一个优化后的 binary，CRC/重试配置相同。这只能检查实验噪声，不能用其结果扣减 A/B 的退化。
- 保留全部正式样本和 warmup，不按快慢删点。min、median、max 分别报告，附样本方差、CV、
  每轮 min 和整轮配对 bootstrap 的 median 95% 区间。min 是观测统计量，不能用 median 区间替代。
  “每轮 min 的中位变化”先分别计算两侧各轮 min 的中位数，再取 B/A−1，不是逐轮百分比的中位数。
- 正式作业串行运行，期间没有重叠构建、测试或 profiler；固定 writer CPU 为 2、3，
  `RAYON_NUM_THREADS=2`、`OMP_NUM_THREADS=1`。完整查询使用同机两个 Ray worker。
- 本地路径为 `/tmp`；JuiceFS 路径为 `/hdd-01/dev/qi.f`，通过 fuse.bindfs 暴露。
  缓存没有清空，结果描述这个挂载路径和当时的系统状态。

| 名称 | 每批逻辑字节 | 每文件/组批数 | 文件/组数 | 总逻辑字节 |
|---|---:|---:|---:|---:|
| stream-single | 4 KiB | 1 | 128 | 512 KiB |
| stream-tiny | 4 KiB | 256 | 8 | 8 MiB |
| stream-wide | 4 MiB | 16 | 2 | 128 MiB |
| oneshot-small | 4 KiB | 128 | 16 | 8 MiB |

4 KiB 是输入大小，不是 IPC 文件大小。writer 的时间覆盖整组文件的创建、编码、写入、
关闭和调度，不是单个文件的耗时；准备、oracle 和清理在计时外。`c8` 是并发上限，
stream-wide 只有两个文件。LZ4 输入是高度可压缩的重复 UInt8 数据。

完整查询实际执行 `FlightGather`：64,000 行、32 个已物化输入分区、全局排序窗口，
检查物理计划以及全部输入值和输出序号。只对 collect 计时，确认扩展哈希一致、无对象存储 spill。
该 PerPartition 后端使用实际 local shuffle 目录；目录按实验条件放到本地或 JuiceFS。
即使配置 shared-only/sync，此路径也不包含 fsync，不能称为同步持久化吞吐测试。

## 已完成的本地重点对照

单位为整组 128 个文件的毫秒；正值代表变慢。

| 对照 / 并发 | A min / median / max | B min / median / max | Δmin | Δmedian | CV A → B |
|---|---:|---:|---:|---:|---:|
| 优化前 → 本次 / 8 | 6.010 / 7.078 / 9.311 | 5.170 / 5.676 / 8.387 | −13.973% | −19.799% | 9.316% → 12.765% |
| EIO 前 → 本次 / 8 | 5.224 / 5.978 / 8.670 | 5.224 / 5.648 / 12.938 | +0.008% | −5.524% | 12.202% → 16.576% |
| EIO 前 → 本次 / 1 | 9.998 / 15.461 / 58.194 | 9.251 / 14.275 / 59.200 | −7.473% | −7.671% | 60.267% → 66.790% |

并发 8 时，本次相对优化前的 median 95% 区间为 [−21.158%, −18.422%]，
相对历史基线为 [−7.296%, −4.078%]。历史对照每轮 min 的中位变化为 −1.247%，
其中 1/12 轮 min 退化超过 1%。历史 max 从 8.670 ms 增至 12.938 ms，仍需保留为尾部波动证据。
不能把 min/median 改善解释为所有延迟指标都已稳定。

同一优化 binary 将 retries 从 6 改为 0 的诊断中，并发 8 的 median 变化为 −1.724%，
区间 [−12.917%, +0.025%]，CV 超过 60%，max 分别为 26.717 / 28.080 ms。
这组数据不足以在 1% 精度上测出 CRC 的独立影响；也同时关闭了恢复策略，不能称为纯 CRC 微基准。

整 job 的上下文切换（包含 warmup、oracle 和清理）也下降：并发 8 的独立优化对照中，
voluntary 中位数 3579.5 → 1293，involuntary 1290.5 → 130.5。
这些计数支持调度开销减少，不能换算成计时段的耗时占比。

## 完整存储与查询结果

A 为 EIO 前旧版，B 为本次优化版；每侧每格 48 个正式样本。正数代表变慢。

### 本地 ext4

| 负载 / 配置 | A min / median / max (ms) | B min / median / max (ms) | Δmin | Δmedian | CV A → B | median 95% 区间 |
|---|---:|---:|---:|---:|---:|---:|
| oneshot-small / c8-none | 3.700 / 4.572 / 5.379 | 3.373 / 4.020 / 5.654 | -8.834% | -12.074% | 8.78% → 10.20% | [-15.01%, -6.99%] |
| stream-single / c8-none | 5.352 / 5.772 / 7.941 | 5.207 / 5.581 / 8.992 | -2.700% | -3.310% | 9.74% → 9.96% | [-5.13%, -1.12%] |
| stream-wide / c8-lz4 | 53.783 / 59.052 / 65.182 | 14.356 / 20.446 / 45.728 | -73.308% | -65.377% | 4.48% → 35.48% | [-68.11%, -53.72%] |
| stream-wide / c8-none | 23.627 / 25.248 / 27.818 | 21.568 / 25.567 / 31.159 | -8.714% | +1.262% | 3.35% → 10.33% | [-2.25%, +4.34%] |
| flight-gather-lz4 / collect | 65.687 / 72.131 / 78.976 | 68.611 / 74.939 / 83.062 | +4.451% | +3.893% | 4.44% → 3.95% | [+2.85%, +6.23%] |
| flight-gather-none / collect | 67.411 / 72.470 / 79.480 | 69.113 / 74.999 / 85.886 | +2.524% | +3.490% | 4.49% → 4.75% | [+1.43%, +6.45%] |

六格中 3 格的观测 min 或 median 超过 +1%；3 格的 median 区间尚不能排除超过 +1% 的退化。
其中两项 FlightGather 的 median 区间下界都超过 +1%，且八轮的 min 都慢超过 1%；
这是本轮明确的整体版本回归，不能归结为一个异常样本，也不能单独归因于本次改动。

- stream-wide / c8-none：每轮 min 的中位变化 -8.685%，1/8 轮 min 退化超过 1%。
- flight-gather-lz4 / collect：每轮 min 的中位变化 +6.094%，8/8 轮 min 退化超过 1%。
- flight-gather-none / collect：每轮 min 的中位变化 +2.903%，8/8 轮 min 退化超过 1%。

### JuiceFS 挂载路径

| 负载 / 配置 | A min / median / max (ms) | B min / median / max (ms) | Δmin | Δmedian | CV A → B | median 95% 区间 |
|---|---:|---:|---:|---:|---:|---:|
| oneshot-small / c8-none | 455.057 / 557.175 / 602.287 | 435.479 / 546.582 / 624.435 | -4.302% | -1.901% | 6.36% → 6.83% | [-3.90%, +1.21%] |
| stream-single / c8-none | 3523.040 / 3771.101 / 3895.570 | 3489.085 / 3707.251 / 3843.399 | -0.964% | -1.693% | 2.74% → 2.53% | [-3.11%, -0.08%] |
| stream-wide / c8-lz4 | 160.970 / 258.001 / 308.945 | 186.685 / 217.349 / 250.567 | +15.975% | -15.756% | 9.63% → 6.18% | [-16.93%, -13.01%] |
| stream-wide / c8-none | 1428.890 / 7917.536 / 9052.732 | 905.577 / 8040.562 / 9372.128 | -36.624% | +1.554% | 23.69% → 31.20% | [-7.27%, +6.37%] |
| flight-gather-lz4 / collect | 1868.110 / 2033.029 / 2313.400 | 1907.416 / 2058.669 / 2301.675 | +2.104% | +1.261% | 4.50% → 4.03% | [-0.43%, +4.48%] |
| flight-gather-none / collect | 1937.212 / 2051.162 / 2170.407 | 1840.249 / 2046.190 / 2314.213 | -5.005% | -0.242% | 3.09% → 4.11% | [-2.53%, +2.81%] |

六格中 3 格的观测 min 或 median 超过 +1%；4 格的 median 区间尚不能排除超过 +1% 的退化。

- stream-wide / c8-lz4：每轮 min 的中位变化 -13.370%，2/8 轮 min 退化超过 1%。
- stream-wide / c8-none：每轮 min 的中位变化 +28.612%，5/8 轮 min 退化超过 1%。
- flight-gather-lz4 / collect：每轮 min 的中位变化 +2.576%，5/8 轮 min 退化超过 1%。

## 独立优化的多批次、大批次回归

A 为此前 Arrow 60/EIO/CRC 版本，B 为本次优化；每侧每格 48 个正式样本，均为本地路径。

| 负载 / 配置 | A min / median / max (ms) | B min / median / max (ms) | Δmin | Δmedian | CV A → B | median 95% 区间 |
|---|---:|---:|---:|---:|---:|---:|
| oneshot-small / c1-lz4 | 7.267 / 9.374 / 13.597 | 7.401 / 8.854 / 13.615 | +1.846% | -5.551% | 17.91% → 19.34% | [-14.90%, +3.84%] |
| oneshot-small / c1-none | 5.369 / 5.787 / 6.737 | 5.269 / 5.748 / 10.570 | -1.853% | -0.672% | 4.41% → 12.33% | [-1.88%, +1.33%] |
| oneshot-small / c8-lz4 | 4.164 / 4.755 / 5.856 | 4.312 / 4.891 / 6.232 | +3.551% | +2.862% | 8.19% → 8.08% | [+0.25%, +5.31%] |
| oneshot-small / c8-none | 3.355 / 4.026 / 4.475 | 3.414 / 3.928 / 4.806 | +1.752% | -2.428% | 7.00% → 7.34% | [-4.45%, +0.00%] |
| stream-tiny / c1-lz4 | 18.748 / 21.631 / 29.028 | 19.778 / 22.763 / 31.102 | +5.495% | +5.231% | 8.84% → 8.90% | [-2.21%, +9.27%] |
| stream-tiny / c1-none | 19.925 / 22.220 / 27.188 | 19.307 / 21.960 / 29.252 | -3.099% | -1.169% | 6.96% → 8.71% | [-4.98%, +0.74%] |
| stream-tiny / c8-lz4 | 7.715 / 8.628 / 11.283 | 7.757 / 8.912 / 12.907 | +0.535% | +3.292% | 9.97% → 9.63% | [-1.15%, +5.17%] |
| stream-tiny / c8-none | 7.928 / 9.111 / 12.121 | 8.140 / 9.147 / 12.647 | +2.673% | +0.403% | 9.17% → 10.73% | [-3.88%, +5.84%] |
| stream-wide / c1-lz4 | 25.530 / 30.457 / 32.978 | 26.781 / 30.035 / 34.236 | +4.900% | -1.387% | 6.26% → 6.11% | [-3.25%, +1.18%] |
| stream-wide / c1-none | 44.040 / 45.795 / 48.108 | 44.424 / 45.957 / 46.933 | +0.873% | +0.355% | 1.77% → 1.37% | [-0.32%, +1.15%] |
| stream-wide / c8-lz4 | 14.960 / 19.789 / 31.976 | 13.770 / 17.984 / 35.963 | -7.959% | -9.120% | 17.90% → 24.45% | [-14.22%, -2.54%] |
| stream-wide / c8-none | 21.899 / 25.338 / 33.770 | 22.301 / 26.485 / 31.827 | +1.838% | +4.528% | 10.15% → 10.07% | [-4.06%, +8.43%] |

### 加大样本后的增量复核

同样的两份 binary、十二种配置；十六轮 ABBA，每侧每格 256 个正式样本。原来的 48 样本结果保留在上表。

| 负载 / 配置 | A min / median / max (ms) | B min / median / max (ms) | Δmin | Δmedian | CV A → B | median 95% 区间 |
|---|---:|---:|---:|---:|---:|---:|
| oneshot-small / c1-lz4 | 7.248 / 9.750 / 14.577 | 7.442 / 9.600 / 14.056 | +2.664% | -1.536% | 19.11% → 18.36% | [-8.44%, +4.56%] |
| oneshot-small / c1-none | 5.340 / 6.045 / 9.364 | 5.365 / 6.043 / 10.557 | +0.477% | -0.034% | 9.25% → 13.49% | [-1.49%, +1.77%] |
| oneshot-small / c8-lz4 | 4.213 / 5.047 / 6.635 | 4.056 / 5.039 / 6.675 | -3.708% | -0.157% | 8.84% → 9.90% | [-3.77%, +2.02%] |
| oneshot-small / c8-none | 3.393 / 4.083 / 6.867 | 3.312 / 4.060 / 5.331 | -2.413% | -0.571% | 9.49% → 8.48% | [-3.50%, +2.54%] |
| stream-tiny / c1-lz4 | 18.293 / 22.780 / 38.843 | 19.155 / 23.223 / 69.690 | +4.714% | +1.944% | 9.78% → 17.48% | [-1.75%, +4.97%] |
| stream-tiny / c1-none | 18.688 / 22.813 / 31.147 | 19.039 / 22.925 / 72.025 | +1.878% | +0.491% | 8.85% → 17.22% | [-2.35%, +3.97%] |
| stream-tiny / c8-lz4 | 7.820 / 9.086 / 15.003 | 7.873 / 9.187 / 15.257 | +0.675% | +1.114% | 10.71% → 13.07% | [-1.22%, +3.34%] |
| stream-tiny / c8-none | 7.973 / 9.296 / 15.179 | 7.913 / 9.185 / 14.937 | -0.752% | -1.201% | 11.70% → 10.45% | [-3.33%, +1.33%] |
| stream-wide / c1-lz4 | 26.268 / 31.527 / 139.710 | 25.889 / 31.537 / 91.614 | -1.442% | +0.032% | 22.16% → 25.63% | [-2.32%, +2.13%] |
| stream-wide / c1-none | 42.849 / 45.268 / 48.757 | 42.951 / 45.412 / 49.141 | +0.238% | +0.318% | 2.47% → 2.37% | [-0.40%, +0.90%] |
| stream-wide / c8-lz4 | 13.961 / 19.599 / 43.511 | 13.859 / 20.209 / 45.621 | -0.734% | +3.116% | 20.54% → 25.62% | [-1.26%, +7.62%] |
| stream-wide / c8-none | 21.160 / 26.618 / 43.036 | 21.269 / 26.254 / 36.345 | +0.512% | -1.367% | 11.86% → 10.59% | [-2.68%, -0.04%] |

加大样本后，十二格仍有五格的观测 min 或 median 超过 +1%。例如小批 c1/LZ4
仍为 min +4.714%、median +1.944%，max 从 38.843 ms 增至 69.690 ms。
其 median 区间 [−1.75%, +4.97%] 包含改善和退化两种可能；不能认定为零开销。
相反，宽文件 c8/未压缩的 median 从首次 +4.528% 变为 −1.367%，
宽文件 c8/LZ4 则从 −9.120% 变为 +3.116%。这些方向变化以及较高 CV
说明目前仍有显著的测量波动。两轮结果都保留，不挑选更有利的一轮作为结论。

### 同一 binary 的 A/A 噪声对照

两侧均为优化后的同一 binary、相同 CRC 和重试配置；每侧每格 256 个正式样本。仅标签不同。

| 负载 / 配置 | A min / median / max (ms) | B min / median / max (ms) | Δmin | Δmedian | CV A → B | median 95% 区间 |
|---|---:|---:|---:|---:|---:|---:|
| stream-tiny / c1-lz4 | 19.031 / 23.262 / 40.311 | 18.375 / 23.414 / 75.456 | -3.448% | +0.656% | 10.48% → 18.64% | [-1.05%, +3.04%] |
| stream-wide / c8-none | 20.973 / 25.496 / 36.868 | 21.164 / 25.908 / 31.478 | +0.910% | +1.616% | 11.00% → 10.22% | [-0.52%, +4.24%] |
| oneshot-small / c8-lz4 | 4.193 / 4.920 / 6.116 | 4.260 / 4.886 / 6.260 | +1.593% | -0.710% | 7.97% → 8.06% | [-2.42%, +1.58%] |

同一 binary 的宽文件 median 也相差 +1.616%，one-shot 的 min 相差 +1.593%，
小批的 min 相差 −3.448%。A/A 表明本机这组实验不足以稳定分辨 1% 左右的差异，
增加样本并没有消除实验波动。它不能证明 A/B 的差异全部来自噪声，
也不能用于扣减 A/B 的退化，更不能覆盖本地完整查询中已有明确区间证据的回归。

这些对照与完整存储矩阵分时运行，不能跨实验混合样本或直接比较极值。全部方差和每轮 min 见 [focus-summary.json](focus-summary.json)，完整存储统计见 [min_median.json](min_median.json)。

## 验证与复现

最终 boxed pending-state 版本通过 `make build`、release wheel 构建、110 项 Rust 测试、
五项取消注入测试，以及 58 项 Python/Ray 测试（22 项未选中）。新增测试覆盖首批延后后的
并发生产者、push/close 竞争，以及关闭期间取消后的 active-write 排空。

全部数据已由 [archive.py](archive.py) 归档；[audit.py](audit.py) 从全部原始样本重算统计，
检查 ABBA 次序、轮次、warmup、样本数及 oracle，不依赖已有汇总判断通过。
共归档 12,288 个正式样本和 2,016 个 warmup；全部源码、patch、binary 和 wheel
哈希在测量结束后复核一致。审计中的 PASS 表示数据和来源校验通过，不表示性能满足 1% 门槛。

```sh
python3 benchmarking/shuffle/normal_path_performance/deferred_first_batch_20260917/archive.py /tmp/daft-task-handle-20260917
python3 benchmarking/shuffle/normal_path_performance/deferred_first_batch_20260917/audit.py
```

重新测量时，先准备各版本的 release test executable 和 wheel，固定编译器、profile
以及相同的 Python 依赖。旧版使用相同的 `write_bench.rs`，仅将不存在的重试
`configure()` 改为 no-op。`builds-final.json` 记录本次使用的二进制身份；其他机器
需使用自己的已核对路径和哈希。构建、功能测试和正式计时分开运行。

```sh
cargo test --release --locked -p daft-io -p daft-writers -p daft-shuffles --no-run

python3 benchmarking/shuffle/write_abba.py \
  --baseline "$PREVIOUS_WRITER" --candidate "$OPTIMIZED_WRITER" \
  --cases stream-single --concurrency 1 8 --compression none \
  --cycles 12 --samples 8 --output /tmp/new-deferred-small

python3 benchmarking/shuffle/write_abba.py \
  --baseline "$PREVIOUS_WRITER" --candidate "$OPTIMIZED_WRITER" \
  --cases stream-tiny stream-wide oneshot-small \
  --concurrency 1 8 --compression none lz4 \
  --cycles 16 --samples 8 --output /tmp/new-deferred-matrix

python3 benchmarking/shuffle/storage_abba.py \
  --builds /path/to/pinned-builds.json --output /tmp/new-deferred-storage \
  --shared-root /hdd-01/dev/qi.f --cycles 8 --samples 3
python3 benchmarking/shuffle/storage_stats.py /tmp/new-deferred-storage --write
```

重点小文件的历史对照使用 EIO 前 writer 作为 baseline。A/A 对照把两侧都指向
`OPTIMIZED_WRITER`，每次仅选择方法部分列出的一个 case/concurrency/compression。
CRC 诊断把两侧都指向该 binary，再指定 `--baseline-retries 6 --retries 0`。
所有输出目录必须是新的目录，不能覆盖或混合先前的样本。
