## 这是什么

这是 **Apodex 内部发行版**，不是官方 Daft 发到 PyPI 的包。

- **基线：** 官方 [`v0.7.24`](https://github.com/Eventual-Inc/Daft/releases/tag/v0.7.24)（2026-08-14）
- **分支：** [`release_apodex_0724`](https://github.com/qifuapodex/Daft/tree/release_apodex_0724)
- **构建 tag：** `apodex-0.7.24.2`
- **wheel 版本：** `daft-0.7.24+apodex.2`（上一版是 [`apodex.1`](https://github.com/qifuapodex/Daft/releases/tag/apodex-0.7.24)）
- **包名：** 仍是 `daft`（不要 `pip install daft` 装官方源）
- **发布渠道：** 只挂在本 GitHub Release 的 Assets 上，**不会上传 pypi.org**

官方 v0.7.24 之后、`main` 上的其它提交**没有**合进来。下面 PR 当时都还开着，按依赖顺序 cherry-pick 到 `v0.7.24` 上。

| PR | 标题 | 解决的问题 |
|---|---|---|
| [#7415](https://github.com/Eventual-Inc/Daft/pull/7415) | release limit's output channel at input exhaustion | Ray 上 `into_partitions` + `LIMIT` **永久挂死** |
| [#7423](https://github.com/Eventual-Inc/Daft/pull/7423) | keep task tokens alive when combining task builders | Ray 上 **cross join + `LIMIT` 永久挂死**（#7415 管不到） |
| [#7424](https://github.com/Eventual-Inc/Daft/pull/7424) | stop upstream work when a distributed LIMIT is satisfied | 分布式 `LIMIT` 在无法 pushdown 时**完全不省计算** |
| [#7451](https://github.com/Eventual-Inc/Daft/pull/7451) | match aliased count aggregations in count pushdown | **SQL `count(*)` 从未走过** parquet count 元数据 pushdown |
| [#7459](https://github.com/Eventual-Inc/Daft/pull/7459) | dispatch flight shuffle reads on ref type, not backend config | `flight_shuffle` 下 `into_partitions` 合并分区时 **panic** |

---

## 安装

从本 Release 的 Assets 里选当前机器对应的 wheel，或让 pip 自己选：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.2" "daft"
```

Ray 集群（**head 和每个 worker 都要装同一份**，不要混用官方 `daft==0.7.24`）：

```bash
pip install --force-reinstall --find-links "https://github.com/qifuapodex/Daft/releases/expanded_assets/apodex-0.7.24.2" "daft[ray]"
```

本 tag 会打这些平台（GitHub 免费 runner，`manylinux_2_24` / macOS / Windows）：

- Linux x86_64、Linux aarch64
- macOS x86_64、macOS aarch64（Apple Silicon）
- Windows x86_64

装完请确认不是官方版本：

```python
import daft
print(daft.__version__)
```

应打印 `0.7.24+apodex.2`，不要装成官方 `0.7.24`。

---

## #7415 — `into_partitions` + `LIMIT` 在 Ray 上挂死

**链接：** https://github.com/Eventual-Inc/Daft/pull/7415

### 现象

Ray runner 上，只要 `into_partitions` 叠在 `LIMIT` 上面，查询会**永远不返回**：没有异常、没有超时，作业一直占着集群，只能外部杀掉。

任意 `num_partitions` 都会复现，包括 `into_partitions(1)`，所以不是并行度问题。`repartition(...)` 和「没有 into_partitions 的普通 scan」都正常。

必须真正物化结果（例如 `to_pydict()`）。`count_rows()` 有时会被优化器把 limit 折掉，看起来像「没挂」。数据源还需要产生多于一个 scan task；单 partition 的 `InMemoryScan` 可能被 `DropRepartition` 把 `into_partitions` 拿掉，也就复现不了。

### 复现

```python
import daft

daft.set_runner_ray()
df = daft.range(0, 10_000, partitions=8).into_partitions(4).limit(10)
print(len(df.to_pydict()["id"]))  # 永远走不到这里
```

真实 parquet 同样会挂：

```python
df = daft.read_parquet(".../*.parquet")  # 8 个文件就够
df = df.into_partitions(8).limit(10)
print(len(df.to_pydict()["some_col"]))
```

SQL 的 `LIMIT` 叠在 `into_partitions` 上也会挂。

### 根因

优化器 `PushDownLimit` 会把计划从：

```text
Limit → IntoPartitions → Scan
```

交换成：

```text
IntoPartitions → Limit → Scan
```

于是 `IntoPartitionsNode` 去消费 `LimitNode` 的 task-builder 流。两边互相等：

1. `IntoPartitions` 必须先数清上游有多少 task，才能决定合并还是拆分，所以会 `input_stream.collect().await`，在提交任何任务之前把 stream **抽干**。
2. `LimitNode` 的循环要等自己已经发出去的 task **真正跑起来**、向 counter actor `claim` 行数之后才结束；并且它把输出 channel 一直握到循环结束才放开。

结果：没有任何 task 被提交 → 没有任何 `claim` 到来 → `_LimitCounterImpl.await_limit_completion` 无限转。这个等待**没有超时**，所以表现为永久挂死而不是失败。

`repartition` 不受影响，是因为它会物化输入流，来一个 task 就提交一个。

### 修复

`LimitNode` 在「已经没有东西可往下游送」时立刻 drop 输出 sender：输入流结束，或下游已经消失。不要等到整个 limit 循环结束。

channel 一关，`IntoPartitions` 就能拿到完整的 builder 列表并开始提交任务；limit 循环随后能观察到 `claim`，正常结束。循环的终止条件、early-stop、取消语义都没改。

### 验证

回归测试在 `tests/dataframe/test_distributed_limit.py`：覆盖 IntoPartitions 三种形态（合并到 1 / 4、拆到 16），以及不走 early-stop、靠把输入抽干收尾的路径（`offset`、`limit > 总行数`、`limit(0)`）。

死锁发生在持有 GIL 的 Rust runtime 里，`@pytest.mark.timeout` 的 SIGALRM 根本跑不进进程，所以测试用独立线程做 watchdog，而不是 pytest-timeout。

本地对照：`into_partitions(1/2/8/16) + limit(10)` 和 SQL `LIMIT 10` 修复前挂死，修复后返回 10 行；`repartition` / 普通 scan 行为不变。

---

## #7423 — cross join + `LIMIT` 在 Ray 上挂死

**链接：** https://github.com/Eventual-Inc/Daft/pull/7423

> 叠在 #7415 上面。只合 #7415 **修不好**这条（独立构建上 3/3 仍挂）。

### 现象

Ray runner 上，cross join **任意一侧**带 `LIMIT`，查询同样永久挂死：没报错、没超时。

limit 在左或右、1 个或 4 个 partition、limit 能不能推进 scan，都会挂。下面这些是好的：不带 limit 的 cross join、带 limit 的 broadcast join、带 limit 的 `concat`。

### 复现

```python
import daft
from daft import col

daft.set_runner_ray()

left = daft.range(0, 400, partitions=4).limit(100)
right = daft.range(0, 5, partitions=1).select(col("id").alias("rid"))

print(len(left.join(right, how="cross").to_pydict()["rid"]))  # 永远走不到这里
```

### 根因

`CrossJoinNode` 会把两侧 builder 当**模板**做笛卡尔展开：每来一个新 builder，就和另一侧已经见过的每一个做 `combine_with`。于是 `LimitNode` 转发下去的**一个** builder，会变成 **n 个真正执行的 task**，而且这 n 个都不是当初注册 token 的那个 builder。

`SwordfishTaskBuilder::combine_with` 合并时把两个字段清成空：

- `notify_tokens: vec![]` — 真正跑起来的 task 不向 `LimitNode` 汇报。`contributors.is_subset(completed_ids)` 永远不成立，limit 循环不退出。
- `cancel_token: None` — `parent_cancel.cancel()` 够不到这些 fused task，early-stop 也取消不了它们。

还有第二条挂死路径。#7415 让 limit 在输入耗尽时放开 channel；若 token 仍被丢掉，循环会在「当前没有 token」时以为没事了，**提前拆掉 counter actor**。后面模板再派生出的 task 打到已经死掉的 actor 上，失败后换 worker 无限重试——看起来还是挂死，只是机制不同。

`broadcast_join`（`map_plan`）和 `hash_join`（1:1 zip）不会把一个 builder 复制成 n 份，所以今天只有 cross join 会爆；`hash_join` 也走 `combine_with`，结构上是暴露的。

actor-pool UDF、vLLM、key-filtering join 用同一套 token 注册方式，在 cross join 下也会过早拆 actor；这次一并修好。

### 修复

- Notify token 从 oneshot 改成可 clone 的 `TaskNotifyToken`，指向同一条 channel，随每次 `combine_with` 进入派生任务。
- Channel 只在「所有派生任务都结束 **并且** 不会再从还活着的 builder 派生新任务」时关闭。
- `LimitNode` 用这条 channel 的关闭作为 drain 条件，counter actor 不会在还有人会跟它说话时被拆掉。
- `combine_with` 从两侧各派生一个 child cancel token：取消某一个派生任务不会误伤兄弟或模板；任意一侧 `cancel()` 仍能打到融进去的那一个 task。

### 验证

回归测试覆盖：limit 在左 / 在右（early-stop 路径），以及 `limit > 总行数`（natural-drain，对应「过早拆 actor」那一半）。同样用线程 watchdog。

### 已知限制（不是这次引入的，测试标了 xfail）

另一侧有 **多于 1 个 partition** 时，`CrossJoinNode` 会把同一份被 limit 过的数据复制进多个 fused task，它们**共用**一个 counter actor。budget 是按「不同输入分区」设计的，不是按「同一分区的副本」，所以每一行往往只在其中一个副本里活下来，笛卡尔积会缺行。

另一侧只有 1 个 partition 时，每个 builder 只变成 1 个 task，计数是对的。native runner 对同一查询会返回完整交叉积。这是 fan-out 点上的独立缺陷，本 PR 只是把它暴露出来，没有假装修好。

---

## #7424 — 分布式 `LIMIT` 满足后仍把上游跑完

**链接：** https://github.com/Eventual-Inc/Daft/pull/7424

### 现象

当 `LIMIT` **不能下推进 scan** 时（前面有行级 UDF、`where` 等），分布式 LIMIT **几乎不省任何计算**。全局行数已经够了，每个 Swordfish task 仍会把整个 partition 读完、每个 morsel 都 `claim` 一次，再把结果切成空的。

在 8×100 万行 parquet、行级 UDF 谓词上，`LIMIT 10` 花 51.9s，同一条查询**不加 limit** 是 51.1s——limit 等于白写。native runner 的 `LimitSink` 会返回 `Finished`，同查询大约 19s。

### 复现形态

```python
daft.set_runner_ray()
df = daft.read_parquet("...").where(keep(col("id"))).limit(10)
df.collect()  # keep 是行级 UDF，挡住 scan pushdown
```

可 pushdown 的 `read_parquet(...).limit(10)` 本来就很快，这条路径不是本 PR 的主目标。

### 根因

`DistributedLimitSink` 丢掉了 counter actor 返回的 `done` 标志，永远回答 `NeedMoreInput`。

不能把 native 的 `StreamingSinkOutput::Finished` 直接拿来用：`Finished` 的意思是「把这个 node 整棵拆掉」。flotilla 会按 plan fingerprint 在**很多个 task 之间复用同一条** local pipeline，短路其中一个 task 会把共享 pipeline 弄死，后续 task 报：

```text
Plan execution task has died; cannot enqueue new input
```

所以需要「只结束这一条 input，而不是拆掉整个 node」的语义。

另外，`SwordfishTaskBuilder::combine_with` 会把 broadcast / cross join 两侧融进**一个** task plan。如果按 `input_id` 整段取消，会把 limit **并不拥有**的另一侧 scan 一并停掉，join 会悄悄少行。取消必须限定在该 sink 自己的子树上。

### 修复

- 新增 `InputSatisfied`：结束当前这条 `input_id`，让喂它的 producer 停下，并丢掉已经交上来的数据；这条 input 仍走普通 `PipelineMessage::Flush` 收尾。下游的 bookkeeping 看不到「取消」，`Finished` 继续只给 native `LimitSink` 用。
- Sink 通过 `cancels_inputs` 选择加入；取消范围覆盖它实际消费的 producer，而不是整个 `input_id`。取消全程是 advisory：producer 不理它，行为与以前一样。
- 子树里即使有 aggregation / sort / top-N 这类必须看完全量输入才吐行的算子也安全：它们都是 `BlockingSink`，在下游被 satisfy 之前，自己已经把输入抽干了。
- 顺带修了同族的潜伏问题：某个 input 的 `Flush` 如果中间算子从来没 buffer 过，会误结束 `process_input`，把复用 pipeline 上其它 input 还需要的 node 杀掉。
- counter actor 用 `asyncio.Event` 叫醒 waiter，不再 10ms 轮询；retry 把 budget refund 回去时会 `clear()`。`Event.clear()` 不能作废一个已经被 `set()` 解析掉的 future，所以 `await_limit_completion` 每次醒来都重新检查 `is_done()`，避免 refund 之后误以为 limit 已满足、把该留下来的行取消掉。
- counter actor 的节点亲和改为 soft：runner 节点满了会降级放置，而不是让整条查询失败。

### 实测

8×100 万行 parquet + 行级 UDF 谓词，`LIMIT 10`：

| | 修复前 | 修复后 |
|---|---|---|
| `where(keep).limit(10)`，8 partitions | 51.9s / 64 claims | **39.2s / 14 claims** |
| 2 partitions | 13.4s / 16 claims | 13.5s / 9 claims |
| 1 partition | 7.1s / 8 claims | 7.1s / 8 claims |
| `read_parquet(8).limit(10)`（可 pushdown） | 0.98s / 8 claims | 0.97s / 2 claims |

1、2 个 partition 几乎持平是预期：单分区往往在自己 scan 末尾才凑满 `LIMIT 10`，能取消的工作很少。8 分区才是这条修复真正干活的地方。

剩余时间主要卡在「取消信号到达时已经 dispatch 出去的 UDF morsel」——Python UDF 不能在 batch 中途打断。更小的 `default_morsel_size` 能再砍一截尾延迟，那是后续工作，不在本 PR。

### 验证

- `test_limit_stops_upstream_work`：用 Ray actor 数喂进 UDF 的行数，断言 limit 真的少干活（不比墙钟，避免机器快慢干扰）。修复前会喂**每一行**。
- `test_limit_larger_than_input_reads_everything`：`limit > 总行数` 时 counter 从不满足，不能误取消，必须读完全量。
- `test_limit_under_broadcast_join_emits_every_limited_row`：fused plan 下，limit 满足后不能把 join probe 侧的行弄丢。
- 若干 actor Event / refund / 唤醒时序的单测，覆盖「`set()` 和 refund 卡在 waiter resume 之前」的交错。

分布式 `LIMIT` 只保证**行数**，不保证**是哪几行**（budget 按 task 先到先得）。测试断言形状和不变量，不断言具体 id 集合。

---

## #7451 — SQL `count(*)` 走不到 parquet count pushdown

**链接：** https://github.com/Eventual-Inc/Daft/pull/7451

### 现象

#5038 给 parquet 加了 count 元数据 pushdown（读 footer 里的行数，不必扫列）。但 `PushDownAggregation` 只匹配**裸**的 `Expr::Agg(AggExpr::Count(..))`。count 一旦包了 alias，就静默 miss，退化成读整列。

SQL 前端会把 `count(*)` 改写成：

```text
count(<表里最窄的那一列>, All).alias("count")
```

所以 **SQL 的 count 实际上从来没有走进过这条 pushdown**。这不是 SQL 独有：DataFrame 里只要聚合带了名字，一样 miss。

```python
src = lambda: daft.read_parquet("<parquet glob>")

src().count("*").explain(show_all=True)
# Pushdowns: {projection: [id], aggregation: count(col(id), All)}   ← 能 pushdown

src().agg(col("id").count("all").alias("n")).explain(show_all=True)
# Pushdowns: {projection: [id]}                                     ← 不能

daft.sql("select count(*) as n from t", t=src()).explain(show_all=True)
# Pushdowns: {projection: [id]}                                     ← 不能
```

`df.count("*")` 能走 pushdown，只因为它把 rename 放在聚合**上面**单独的 `Project` 里，聚合表达式本身是裸的。

### 根因与修复

- 用 `as_count_agg` 替换 `is_count_expr`：通过已有的 `ExprRef::unwrap_alias()` 剥掉 alias，返回 `(裸 count, 别名, count mode)`。
- **推进 scan 的必须是裸 `Count`，不能是带 alias 的表达式。** `scan_task_reader.rs` 按 `Expr::Agg(AggExpr::Count(_, count_mode))` 选元数据路径；把 alias 递进去会落到普通按行读，而 pushdown 之后的 schema 已经收成单个 count 字段——结果会**错**，不只是慢。
- 改写后的 `Sum` 再把名字加回去，保持输出 schema。
- 顺带：规则以前在 `len() == 1` 判断之前就索引 `aggregations[0]`，现在改成靠 `&&` 短路。

### 第二个正确性修复（同一 PR 的 follow-up）

原先改写把 `count(<expr>)` 变成 `sum(<expr>)`，在 **pushdown 之后的 schema** 上求值。那个 schema 里只有一列部分 count、每个 scan task 一行。这只在 `<expr>` 是「名字碰巧等于 scan 输出字段」的裸列时是对的——`count(col(a))` 刚好如此（`Count(col(a), All).to_field().name == "a"`），`sum(col(a))` 读到的是 count 列，纯属巧合。

其它参数都是错的。例如 `count(lit(1))` 变成 `sum(lit(1))`，每个 scan task 一行，返回的是**文件数 / task 数**而不是行数：

```text
read_parquet(4 files, 800k rows).agg(lit(1).count("all"))  →  4
```

裸的这种形式在官方 `main` 上就已经坏了。带 alias 的形式之前因为根本没 pushdown，反而「歪打正着」没踩中。#7451 如果只放宽 matcher、不改求和目标，会把这条回归放进来。

修复：对 **scan 真正吐出的那一列 count** 做 `sum`，不再对原始 count 参数求值。`count(col(a))` 和 `count(lit(1))` 两条都对。

下面这些仍然 **不会** pushdown（和以前一样）：`CountMode::Valid` / `Null`、带 group by 的 count、带 filter / limit 的 count。

### 实测

128 个 S3 parquet（约 32 行 × 43 列 / 文件，约 1.2 MiB/文件），7 次，min / median 秒：

| | 修复前 | 修复后 |
|---|---|---|
| native `SQL count(*)` | 1.41 / 1.48 | **0.77 / 0.87** |
| native `df.count_rows()` | 0.78 / 0.86 | 0.73 / 0.77 |
| ray `SQL count(*)` | 1.49 / 1.63 | **0.94 / 1.00** |
| ray `df.count_rows()` | 0.88 / 0.93 | 0.89 / 0.95 |

`count_rows()` 是对照，基本不动；SQL 现在和它走同一条路径，这就是目的。

### 验证

- `agg_count_all_aliased`（`push_down_aggregation.rs`）：alias 不再挡住 pushdown；交给 scan 的是裸 `Count`；改写后 schema 不变。
- `test_parquet_count_sql_reaches_pushdown`（`tests/io/test_parquet.py`）：端到端覆盖 SQL `count(*)` 和 `count(1)`。

---

## #7459 — `flight_shuffle` 下 `into_partitions` 合并分区会 panic

**链接：** https://github.com/Eventual-Inc/Daft/pull/7459

> 本版相对 `apodex.1` **新增**的修复。

### 现象

`shuffle_algorithm="flight_shuffle"` 时，`into_partitions(n)` 只要 `n` **小于**输入分区数（合并 / coalesce 分支），会在调度器线程里直接 panic，作业失败。数据还没开始搬。

```text
thread 'Daft-Scheduler' panicked at .../backends/flight.rs:
expected flight partition ref
   flight::read_inputs_from_refs
   IntoPartitionsNode::coalesce_tasks
```

Python 侧看到的是裸的 `RayTaskError(DaftCoreException)`，看不出是哪个 node。分区数相等或拆分（`n >=` 输入分区数）不受影响，所以小数据、少 scan task 时往往复现不了，真实规模才会撞上。

### 复现

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
        print(df.count_rows())  # panic
```

### 根因

`ShuffleContext` / `ShuffleBackend::build_refs_task_builder` 以前按**节点配置的 backend**选读路径：节点配了 Flight，就一律走 flight reader。这只在 refs 真的是 flight write 产物时成立。

`IntoPartitionsNode` 的 coalesce 分支会先把 child 的输出物化，拿到的是普通 Ray object refs，不是 `FlightPartitionRef`。无条件 downcast 就把进程打崩了。

还有第二条潜伏问题：coalesce 的读 task 如果再用 Flight-backed 的本地 `into_partitions` 包一层，那个 sink 是终端的——吐出 `FlightPartitionRef` 而不是 morsel。父节点再叠任何算子，都会走到 `unreachable!("... should not receive flight partition refs from child")`。把多分区合成 1 个是本地 concat，不是 shuffle write，应始终走 Ray sink。`enable_scan_task_split_and_merge` 下的 equal 分支有同样的潜伏 bug。

### 修复

- 读路径按 **refs 实际类型** 分发：全是 flight refs 走 `shuffle_read(Flight)`；全是普通 refs 走 `in_memory_scan` + psets；混在一起返回 `DaftError`，不再 panic。
- flight helper 里的 `.expect` 改成返回错误，以后再搞错会在 Python 里看到有意义的错误，而不是裸 `RayTaskError`。
- coalesce / equal 的本地 concat **始终用 Ray backend**，不额外做 flight write（输入已经在 Ray object store 里，再走 flight 只是同等峰值内存下多一轮落盘）。

### 验证

回归测试在 `tests/dataframe/test_into_partitions.py`：`flight_shuffle` 下覆盖 coalesce / equal / split（`into_partitions(1/3/7/8/9/16)`），以及 coalesce 之后叠 `with_column` / `agg` / `groupby`。

---

## 本 Release 还包含的工程改动

- 打包改走 GitHub 托管 runner（`ubuntu-latest` / `ubuntu-24.04-arm` / `macos-latest` / `windows-latest`），不再依赖官方付费的 Blacksmith。
- 官方 `publish-pypi.yml` 在本 fork 上加了 `github.repository == 'Eventual-Inc/Daft'`，即使误推 `v*` tag 也不会往 pypi.org 发。
- 自己发版请继续用 `apodex-*` tag，不要打 `v*`。

## 不要预期的东西

- **没有**官方 0.7.24 之后 `main` 上的其它功能 / 修复。
- 没有 LTS（`daft-lts`、老 CPU 无 AVX）wheel。
- #7423 里「cross join 另一侧多 partition 时 limit 行被复制后丢行」仍然存在，测试是 xfail。
