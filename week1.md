# Week 1：SPANN Multi-Tenant 工作总结

截至 2026-11-05。基于已有的单节点多租户骨架，本周主要补齐在线客户端接口、同租户并发、自动驻留缓存、低开销生命周期管理，以及系统性的正确性和性能实验。

**2026-11-06 更新：本报告以下 §1–9 保留周报时的历史状态。** 按后续确认的 merge 范围，已撤回 heap/hash 和维护线程延迟启动优化，恢复原 SPANN 算法及 posting RocksDB 配置。**+170.2% 和约 15% 差距不能用来代表当前代码**；本轮范围、正确性发现和重测见 §10 及设计文档 §16。

**2026-11-09 更新：** 已去掉强制 BKT，默认继承原 SPANN 的 KDT，并保留显式 BKT/KDT 选择，见 §11。§10 的性能矩阵为此前 BKT 版本结果，不是 KDT 的新测量。

## 汇报要点

- **从内部接口走向在线服务**：复用原有 C++ / Python 客户端，支持创建/删除 tenant、插入/删除向量、查询五种远程操作。
- **实现 tenant 级 buffer pool**：按需加载整个索引实例，使用 LRU 淘汰空闲 tenant；支持容量目标、加载合并、dirty writeback 和安全回收。
- **降低 cached tenant 的访问开销**：驻留查询使用 epoch reclamation，不获取 manager 的全局 cache/lifecycle 锁；workspace 按实例隔离并在线程内复用。
- **恢复同 tenant 并发读写**：不再串行化整个请求；VID/日志只在 tenant 内短暂协调，checkpoint 允许已驻留查询继续。
- **性能有实测收益**：2M/128 tenants、8 个查询线程、全驻留均匀访问，QPS 从 **7,818 提高到 21,126（+170.2%）**，peak RSS 从 **3.38 降至 2.00 GiB**。这是相对本轮开始时的多租户版本，不是相对原始非租户 SPANN。
- **边界明确**：常驻 manager 增量很小，但共享存储仍有成本；混合读写压力下的尾延迟和 checkpoint 等待尚未解决。单 DB 合并完成设计，尚未实现。

## 1. 系统架构与隔离

- **沿用并明确架构基线**：一个 tenant 对应一个独立 SPANN/BKT-head 索引，各自拥有 head、version/deletion map 和 VID 空间；不是把所有租户混入同一个索引后过滤。
- **共享存储而不是每 tenant 开 DB**：节点只有两个 RocksDB 引擎，使用固定数量的 CF，通过 `tenant_id + index_id` namespace 隔离数据。

| 引擎 | 业务 CF | 作用 |
|---|---|---|
| State DB | `postings`、`head_snapshots`、`checkpoint_meta` | posting 数据、内存索引快照和恢复点元数据 |
| Recovery DB | `recovery_log`、`catalog`、`recovery_meta` | 应用 redo、租户清单和恢复点发布信息 |

- 不同 tenant 可以拥有相同数值的 VID；删除后重建同名 tenant 会获得新的内部 `index_id`，避免复用旧 namespace。
- 两个引擎共用默认 **64 MiB RocksDB block cache**；它与下述 tenant 实例缓存是两层不同的缓存。
- 默认最多 64 个 tenant、每 tenant 1M 个已分配 VID；128-tenant 实验显式提高测试限额，没有修改生产默认值。删除向量不回收 VID 配额。
- 当前是可信调用方、单节点、单 owner 的服务模型；数据命名空间隔离不等于账号授权、资源硬隔离或故障隔离。

## 2. 新增设计：Tenant Buffer Pool / 驻留缓存

### 2.1 缓存对象与 admission 策略

- **缓存粒度是整个 tenant 索引实例**：head 图、version/map 等状态和关联 workspace 随实例加载/释放；postings 留在共享 RocksDB。不是数据库的逐页 buffer pool，也不是逐个 head 向量淘汰。
- **Demand admission**：Search/Insert/DeleteVector 访问合法但未驻留的 tenant 时触发加载；没有频率门槛、probation、预取或后台定时扫描。
- 状态机为 `Unloaded -> Loading -> Resident -> Evicting -> Unloaded`。同一 tenant 的并发 miss 合并为一次加载，其余请求等待，不各自构建一份实例。

| 条件 / 配置 | 当前策略 |
|---|---|
| `MaxResidentTenants`，默认 8 | 加载前预留槽位；Resident、Loading 和尚未释放的 Evicting 实例都占用容量 |
| `MaxResidentBytes`，默认 0 | 0 表示关闭字节目标；开启时按最近工作快照组件的字节数计费 |
| 正常 admission | 槽位未满，且关闭字节目标或加入新实例后不超过目标 |
| 单实例大于字节目标 | 允许它在缓存为空时独占，避免该 tenant 永远无法访问；指标报告超目标 |
| 容量不足 | 尝试淘汰空闲 LRU tenant；必要时连续淘汰，直到满足 admission |
| 无可淘汰实例 | 等待容量释放，不抢占在途请求，也不强制释放正在使用的实例 |
| 希望全部驻留 | 配置 `MaxResidentTenants=MaxTenants`、`MaxResidentBytes=0`；仍需访问或显式预热才会加载 |

- **字节目标不是 RSS 硬上限**：不包含共享 DB、线程栈、workspace 高水位、分配器和请求 payload；写入增长在下次保存后更新计费。
- 创建时的私有构建实例、启动恢复时的临时实例不等同于在线缓存槽位；缓存配置不能约束它们的全部峰值内存。
- 这里的 admission 是**缓存容量准入**，不是 tenant 请求速率限制、计算并发配额或公平调度；后者尚未实现。

### 2.2 LRU、eviction 与 dirty writeback

- **LRU 实现**：记录最近访问时间，需要换出时扫描目录，选择最久未访问的空闲 Resident tenant。热查询完成时更新原子时间戳，不在每次命中时修改全局双向链表。
- **victim 条件**：状态必须为 Resident，没有更新/冷路径 lease，且扫描时没有观察到该 tenant 的活动 epoch 读者。选择后仍须执行回收协议，不能假设扫描与新查询之间没有竞争。
- **换出步骤**：标记 Evicting、撤回公开实例指针、等待旧 epoch 读者、排空维护、保存必要工作快照、释放实例及 workspace，最后归还容量。
- **clean/dirty 分离**：未修改 tenant 复用已有快照；dirty tenant 先保存再释放。保存失败不丢弃唯一内存状态，并使节点停止正常服务、等待恢复。
- **eviction 不等于 checkpoint**：换出只更新工作快照，不发布节点恢复点，也不提前删除应用日志。
- 加载、保存、等待 epoch 和实例销毁均不在全局 cache 元数据锁内执行，避免持锁做磁盘 I/O。
- 同一进程内复用已验证的工作文件，避免每次 miss 都把相同 head 从 RocksDB 重新复制到磁盘；重启不信任这些临时文件，仍从已发布恢复点重建。

### 2.3 Epoch reclamation：把安全回收成本移出热路径

- **命中路径**：进入线程 epoch，读取不可变 tenant 目录和原子发布的实例指针，执行查询，再退出 epoch。首次线程注册需要协调，稳态命中不获取 manager 全局 cache/lifecycle 锁，也不递增全局 epoch。
- **回收路径**：先撤回指针，再推进 epoch，等待旧读者退出，之后才允许销毁。新 epoch 的读者不会持续阻止旧代回收。
- **miss 必须先退出 epoch**，再等待加载、容量或生命周期屏障，避免请求一边等待容量、一边阻止自己的 victim 回收。
- 更新/冷加载保留 lease 和必要屏障；这不是“整个系统无锁”，RocksDB 和索引内部仍有自己的同步。
- 当前使用节点级 epoch：一个 tenant 的长查询可能延迟其他 tenant 的回收；容量/epoch 等待使用 50 微秒定时检查，不保证固定 eviction latency。
- 管理员可通过内部 `GetStats` 和离线 `tenanttool stats` 观察驻留状态、计费字节、hit/miss、load/eviction 次数和耗时、dirty、failed/closed。`activeRequests` 统计 lease，不包括 epoch 热查询；没有新增普通用户的 List/Unload 权限。

## 3. 新增设计与实现：在线 Client API

- **复用现有客户端**：C++ `AnnClient` 和 Python `SPTAGClient.AnnClient`。Python 是 C++ 的 SWIG 绑定，不是新写的一套纯 Python 网络客户端。
- 请求经版本化 `TenantRequest/TenantResponse` socket 协议进入服务端共享的 `TenantIndexManager`；所有新方法显式携带 tenant，不依赖旧 `SetSearchParam` 状态。

| Python API（C++ 方法同名） | 功能与主要契约 |
|---|---|
| `CreateTenant(tenant, vectors, count, dimension, value_type, metric)` | 创建唯一索引；至少 64 条种子向量；类型、维度、距离创建后固定 |
| `DeleteTenant(tenant)` | 删除整个 tenant 索引，不是卸载 |
| `InsertTenant(tenant, vectors, count, value_type)` | 单条或批量插入；成功返回 `first_id` 和 `count`，对应连续 VID 区间 |
| `DeleteTenantVector(tenant, vector_id)` | 删除该 tenant 内的一个 VID |
| `SearchTenant(tenant, query, k, value_type)` | 单条近似 top-k 查询，返回 VID 和距离 |

- **统一返回对象**：Python dict 含 `status`、`status_name`、`first_id`、`count`、`ids`、`distances`；C++ 返回 `shared_ptr<RemoteTenantResult>`。失败时只使用状态字段；Search 中 `id=-1` 表示无有效候选槽位。
- **数据格式**：连续 bytes 或连续 NumPy 数组，只有向量 payload、没有文件 header；不自动转换 dtype。支持 `Int8/UInt8/Int16/Float`、L2/Cosine，服务端校验类型、维度和长度。
- **并发调用**：五个 Python API 在同步网络等待期间释放 GIL；同一个 client 可以由多个线程调用。socket 增加完整帧发送队列，避免并发 `async_write` 交叉。
- **兼容性**：旧 `Search` 的签名和四元组返回值不变，保留 `SetSearchParam("tenantid", ...)` 的旧查询方式；新协议不改旧 wire format。
- **错误与超时**：返回明确的服务端/网络/超时错误；超时不取消服务端执行，不自动重发写入。没有 request ID 去重或 exactly-once，结果未知的 Insert 不能盲目重试。
- **范围限制**：单条新协议请求/响应 body 上限 64 MiB；大批 Insert 应拆批。客户端不提供在线 List/Describe/Unload/Checkpoint，也不提供认证授权、metadata、iterator、原地更新或业务 document-ID 映射。
- **管理接口单独保留**：内部 Manager 提供 Open/Checkpoint/Close、List/Describe/Unload/GetStats；`tenanttool` 用于离线管理，不能与 server 同时打开同一个 root。

## 4. 同 Tenant 并发与恢复协议

- **撤掉整个 tenant 请求串行化**：同 tenant 支持多查询、多插入和读写重叠；每个 tenant 的提交短临界区只做容量检查、实际 VID 预留、日志追加及必要删除协调，posting 更新和派生维护在其外执行。
- **VID 与 redo 一致**：日志记录实际分配的 VID 范围和向量 payload；在线执行和恢复都使用这些 VID，而不是预测 VID 后再次调用会重新分配的插入入口。不同 tenant 不共享 VID 分配锁。
- **请求级维护完成确认**：一次 Insert 等待自己及其派生任务完成，不等待其他请求不断新增的所有后台任务；卸载、checkpoint、关闭仍排空整个相关索引。
- **checkpoint 分离读写保护**：暂停新更新、冷加载及淘汰，等待在途更新完成；已驻留查询可以继续。提交序号不等于完成水位，必须排空后才能发布连续日志进度。
- **两阶段持久化**：先保存一致的 State checkpoint，再在 Recovery DB 原子发布 current/catalog，最后裁剪已覆盖日志；dirty 工作快照不能冒充已发布恢复点。
- **恢复不污染唯一恢复点**：从已发布 checkpoint 建立新的工作副本，逐 tenant 加载、重放、验证、保存并释放，全部成功才开始服务；重放中再次崩溃仍可重新开始。
- **生命周期不拖住其他热查询**：创建先私有构建，再在不包含构建的独占阶段发布；删除/卸载先撤回目标实例，排空后释放。管理提交仍可能保存其他 dirty tenant，不承诺固定暂停时间。
- **一致性边界**：查询与写入重叠时可以看见部分进行中变化，不提供批次事务快照；成功写入后的后续查询应看到其效果，受 ANN 近似搜索语义限制。
- **持久性边界**：承诺目标是进程崩溃恢复，不是掉电/OS 崩溃持久性。已执行但未收到 ACK 的操作可能恢复出来；不确定状态修改失败会使节点 fail-closed，而不是继续服务。

## 5. 已实施的性能优化与修复

| 改动 | 策略 / 解决的问题 |
|---|---|
| 实例隔离 TLS workspace | 按运行时实例 ID 隔离，长驻 worker 复用本线程 workspace；避免跨 tenant 复用旧 I/O ID/callback，同时避免共享 workspace 池的热路径锁 |
| Workspace 回收 | 每线程、每类型最多缓存 64 个实例槽位；实例卸载和线程退出均能释放空闲 workspace，不能依赖服务线程结束才回收 |
| 恢复原 `VersionLabel` 策略 | 撤回多租户改造额外加入的逐候选锁，沿用原 SPANN 的访问和写侧增长协调；不声称解决了原实现全部内存模型风险 |
| Posting 直接拷贝 | `PinnableSlice` 直接复制到 PageBuffer，去掉中间 `vector<string>`；仍是一次拷贝，不是零拷贝，独立吞吐收益未稳定显现 |
| Managed heap/hash 初始化优化 | 保持搜索预算和 heap 容量，延迟构造 heap 元素，visited table 只清理实际触及的 slot；只启用在对应 managed 路径 |
| 维护线程延迟启动 | managed load 不立即创建 head/SPANN maintenance workers，首次提交任务才启动；legacy/build/refill 保持 eager，首次写入可能承担启动成本 |
| 增量快照导入 | 未修改 tenant 不重新保存；dirty tenant 仍完整序列化，但按 1 MiB chunk 做字节比较，只写变化块、清理过期块，最后写 manifest |
| ThreadPool 关闭修复 | 实际复现 abort 谓词在 mutex 外更新导致 lost wakeup；改为持 queue mutex 更新后 notify/join，修复快速换入换出可能触发的关闭挂起 |

- 优化以 profile 和对照实验为依据，没有通过缩小搜索预算、更改搜索结果或无限增大 TLS cache 获得收益。
- 不把所有改动都称为独立加速：例如 epoch 的主要价值是安全回收与减少生命周期阻塞，直接 PageBuffer 拷贝在单项对照中近似持平。
- 没有强制切换 blob 布局、提高默认 block cache 或合入 lazy posting-buffer 原型；这些候选收益依赖负载，有的还增加 RSS。

## 6. 正确性与验证工作

- 最终 Release 和 ASAN/LSAN 均通过 **30 个用例、17,101 个断言**；保留原 BKT/KDT 和 BKT iterator 的兼容验证。
- 六组数据完成默认/高预算 tenant/direct 输出等价检查；1M/1 的高预算完整 10K 查询还与同索引 private RocksDB 对照匹配。
- 全量扫描 **25M case-vectors** 的 posting，核对 tenant/VID 范围、版本、live-vector 覆盖和原始 payload。六组复用了前 10M 数据源，不能说成 25M 个独立原始向量。
- 覆盖 LRU 顺序、dirty eviction、并发加载合并、超大 tenant、保存失败、同 tenant 并发写、重复删除、跨 tenant 相同 VID、checkpoint 与查询重叠、全部向量删除后重开等场景。
- 8/128 tenants 的并发与 SIGKILL 恢复实验通过；最终还实际覆盖了“Insert 已执行、ACK 记账前被杀”的窗口，恢复出 4,096 个已 ACK 插入和 1 个未 ACK 插入，逐向量检查通过。
- 真实 Python/TCP smoke 覆盖五种 API、同连接并发、GIL 释放、旧 Search 兼容、错误响应、超时/迟到响应及在线写入后进程崩溃恢复。
- ThreadPool 修复后完成 **100K 轮快速启动/关闭**；另有独立 epoch/workspace TSAN 压测。不能据此宣称整个 SPANN 并发实现均已通过 TSAN，也不是所有故障边界的穷举证明。

## 7. 性能实验与结果

### 7.1 方法：分清数据规模、召回率与对照基线

- 数据为 SIFT、UInt8、128D、平方 L2、top-5；覆盖 **1M/1、1M/8、1M/32、2M/128、10M/32、10M/128**，tenant 大小不等，每个约 10K–1M 向量。
- uniform 为均匀 tenant 访问；hot70 为 70% 请求访问最大 tenant，其余均分。分别测试全部驻留和约四分之一驻留槽位。
- 最终受控读实验每轮预热 3 秒、测量 15 秒、每场景三轮，交错/轮换实现顺序，报告中位数；C8 固定 8 个 CPU，最终 C16 使用 16 个 CPU。
- 测量原生 C++ API、warm OS cache，不包含 Python/TCP 端到端开销；throughput 使用完整 10K 查询，recall 用前 200 条的逐 tenant exact truth 和公开报告的距离容差。
- 默认 `MaxCheck/SearchInternalResultNum=256/32`；高预算 `8192/512` **仅为隔离诊断配置**，当前管理/远程 API 未开放这两个参数。
- tenant 分区会改变每次查询的搜索空间，跨 tenant 数的 QPS 不能直接作为 tenancy tax；不同阶段的优化百分比也不能相加。

### 7.2 本周优化前后

| 同组对照场景 | 优化前 | 优化后 | 变化 |
|---|---:|---:|---:|
| 早期 1M/1、C8，四项读路径改动，QPS | 17,232 | 24,390 | +41.5% |
| 2M/128、C8、全驻留 uniform，QPS | 7,818 | 21,126 | +170.2% |
| 2M/128、C8、全驻留 hot70，QPS | 16,667 | 28,917 | +73.5% |
| 990K 初始数据加十批 1K 插入，10 次 checkpoint 总耗时，0 查询线程 | 16.654 s | 5.241 s | 缩短 68.5% |

- 第一行是早期多租户版本到首批读路径优化；中间两行是已发布 `b1818eec` 到最终本地版本。最后一行固定 4 个写线程、只比较 snapshot chunk 复用，不是把不同实验的最佳值拼起来。
- 2M/128 uniform 的 **p99 从 1.751 降到 0.564 ms，peak RSS 从 3.384 降到 1.998 GiB**；高搜索预算没有同等幅度的 workspace 收益。
- checkpoint 对照中，应用层 snapshot Put payload 从约 **444 MB 降到 19.9 MB（减少 95.5%）**；这不是物理 SSD 写入量，且不代表持续混合写负载下的查询尾延迟也改善。

### 7.3 最终多规模、全驻留、C8 uniform

| 数据量 / tenant 数 | 默认 QPS | 默认 peak RSS GiB | 默认 macro Recall@5 | 高预算 QPS | 高预算 macro Recall@5 |
|---|---:|---:|---:|---:|---:|
| 1M / 1 | 25,403 | 0.179 | 76.20% | 1,593 | 98.80% |
| 1M / 8 | 27,920 | 0.355 | 84.98% | 1,957 | 99.61% |
| 1M / 32 | 29,033 | 0.955 | 89.51% | 2,228 | 99.95% |
| 2M / 128 | 21,630 | 1.999 | 91.93% | 1,953 | 99.99% |
| 10M / 32 | 19,617 | 1.344 | 81.67% | 1,577 | 99.38% |
| 10M / 128 | 18,229 | 2.327 | 87.74% | 1,675 | 99.84% |

- 表中 RSS **只对应默认预算**，不含 OS page cache。高预算更耗内存，例如 10M/128 达到 5.106 GiB。
- 这是宏平均 recall，不是每个 tenant 的保证；10M/128 高预算最低 tenant 为 97.9%。不能把默认 25.4K QPS 和高预算 98.8% recall 描述成同一配置。
- 同规模在不同表中有独立重复批次，因此 2M/128 的 21,630 与最终优化复核的 21,126 不矛盾；优化百分比只使用同组配对数据。

### 7.4 换入换出压力：延迟启动维护线程的收益

- 对照组已经包含 workspace 优化和 ThreadPool 关闭修复；实验组仅增加 deferred workers。五种多 tenant 规模、C1/C8、uniform/hot70，共 **120 轮**，QPS 中位数改善 **1.5%–31.8%**。

| C8 场景 / resident slots | eager QPS | deferred QPS | 改善 |
|---|---:|---:|---:|
| 2M/128 / 32，uniform | 1,578 | 1,967 | +24.6% |
| 2M/128 / 32，hot70 | 4,696 | 6,190 | +31.8% |
| 10M/128 / 32，uniform | 998 | 1,222 | +22.4% |
| 10M/128 / 32，hot70 | 3,074 | 3,839 | +24.9% |

- **容量压力仍然昂贵**：2M/128 uniform 从全驻留约 21.6K QPS 降至 32 slots 的约 2.0K；不能用“热路径便宜”掩盖反复换入的成本。
- profiling 指向实例加载、workspace 初始化、线程创建和 dirty writeback；四 tenant 的阶段计时没有发现 manager LRU mutex 饱和，128-tenant 栈采样也不支持把主要问题简单归结为这把锁。不能外推为任何规模都无锁竞争。

### 7.5 多租户增加了多少开销

- **共同设置**：SIFT1B 前缀数据，UInt8、128 维、平方 L2、top-5；`1M` 表示总计 100 万条向量，不是每个 tenant 都有 100 万条。吞吐查询来自完整 10K query 集，recall 用前 200 条的逐 tenant exact truth。
- **测量方式**：同一台 AMD EPYC 7V12 主机，原生 C++ API、只读、warm OS cache，不包含 Python/TCP；每轮预热 3 秒、测量 15 秒，各实现三轮交错/轮换运行，取中位数。C1/C8 绑定 CPU 0–7，C16 绑定 CPU 0–15；C 表示查询线程数。

| 对照实验 | 总数据量 / tenant 数 | 查询并发 | 驻留与访问模式 | Block-cache 上限 |
|---|---|---|---|---|
| Manager 增量：tenant vs direct | 1M / 1、8、32；2M / 128；10M / 32、128，共六组 | C1、C8 | 全部 tenant 驻留，测量阶段无 miss/load/eviction；多 tenant 分别测 uniform、hot70 | 节点共享 64 MiB |
| 非租户端到端：tenant vs private RocksDB（下表六行） | **1M / 1：总计 100 万向量，只有 1 个 tenant** | C1、C8、C16 | 单索引全驻留，所有查询访问同一索引 | 双方各 64 MiB |
| 更大 cache 的端到端对照 | **1M / 1：同一份 100 万向量、1 个 tenant** | C8 | 同上，仅调整 cache 配置 | 双方各 256 MiB |

- 多 tenant 数据按不等大小分区，单 tenant 约 10K–1M 向量；uniform 对 tenant 均匀分配请求，hot70 将 70% 请求分配给最大 tenant。cache 为节点级预算，不是给每个 tenant 各配 64 MiB。
- Manager 增量使用默认搜索预算 `MaxCheck/SearchInternalResultNum=256/32`；两个 private 对照均分别测默认 `256/32` 和高预算 `8192/512`，同组两边使用相同预算。高预算仍是诊断配置，不是远程 API 默认值。
- **Manager 增量**：相同共享存储、索引、cache 和 workspace，仅绕过 manager 的 direct 对照中，默认全驻留 22 个场景的 QPS 差异为 **-1.07% 至 +1.67%**。差异接近运行波动，不是严格上界，也不是全部 tenancy 成本。
- **同机非租户端到端结果**：以下六行均为上述 **1M 向量、1 个 tenant、64 MiB cache** 的实验，不是 128-tenant 对照。两边使用相同 head/graph/version/posting 内容；private 使用原实现及 blob 布局，tenant 使用共享 SST 路径。

| 预算 / Recall@5 | 并发 | Tenant QPS | Private RocksDB QPS | Tenant QPS 差异 |
|---|---:|---:|---:|---:|
| 默认 / 76.2% | 1 | 3,605 | 4,241 | -15.01% |
| 默认 / 76.2% | 8 | 24,993 | 29,731 | -15.94% |
| 默认 / 76.2% | 16 | 46,599 | 55,259 | -15.67% |
| 高预算 / 98.8% | 1 | 255 | 316 | -19.37% |
| 高预算 / 98.8% | 8 | 1,599 | 1,809 | -11.58% |
| 高预算 / 98.8% | 16 | 2,854 | 3,262 | -12.52% |

- 差距包含共享存储、namespace、SST/blob、压缩/缓存行为和实现差异，不能全部归因于路由或 key 前缀。没有测“128 个独立 private DB、相同总资源”的对照。
- 固定查询的 RocksDB 计数器显示，高预算下 shared/private 返回相同 posting payload，但读取字节为 **8.296/3.575 GB**。这是包含 OS cache 命中的软件读取量，不是实际 SSD I/O。
- **增大 cache 可以反转差距，但有内存代价**：双方配置为 256 MiB、C8 时，默认 tenant/private 为 **36,761/29,680 QPS（+23.86%）**，高预算为 **2,246/1,803 QPS（+24.55%）**。
- 对应 tenant/private peak RSS：默认 **0.394/0.125 GiB**，高预算 **0.435/0.181 GiB**。相同 cache 上限不等于相同实际内存使用；private blob 还依赖 OS cache。生产默认仍为 64 MiB。

### 7.6 与 Microsoft 公开 Benchmarking.md 并列参考

| 系统 / 场景 | Recall@5 | QPS | p95 ms |
|---|---:|---:|---:|
| 本机最终 tenant，1M/1、C16、高预算 | 98.8% | 2,854 | 6.029 |
| 公开 FILEIO，最终 1M、16 search threads | 98.6% | 1,412–1,459 | 15.57–15.65 |
| 公开 TiKV，最终 1M、16 search threads | 98.2% | 1,082–1,086 | 16.46–17.00 |

- 公开报告使用 990K 初始数据加十轮 1K 插入、200 queries，插入时没有查询；与本机的硬件、图、revision、参数、cache、查询工作集和计时方式均不同。
- 因此只作背景比较，**不能据此宣称 multi-tenant 比原系统快两倍，或由比值计算 tenancy overhead**。可信的开销估计仍来自上一节同机 private 对照。
- 参考：[固定 revision 的原始报告](https://github.com/microsoft/SPTAG/blob/0d69bb53d3e441679781c7ccee489409f0483a00/docs/Benchmarking.md)。

### 7.7 混合读写：仍有尾延迟和 checkpoint 等待问题

- 4 writers + 8 readers，CPU 0–7，默认预算、64 MiB cache；eager/deferred 各三轮。插入 ACK/s 包含测试记账落盘、删除及周期 checkpoint 干扰，不是纯 Insert 微基准。

| 数据 / slots | eager / deferred Insert ACK/s | eager / deferred read p99 ms |
|---|---:|---:|
| 1M/8 / 8 | 2,950 / 3,048 | 3.471 / 3.494 |
| 2M/128 / 32 | 157.2 / 155.7 | 347.229 / 419.040 |

- 全驻留写入的跨轮波动较大，不能宣称稳定写加速；128-tenant 压力下读 p99 回退，说明只读收益不等于混合负载全面改善。
- 128-tenant 每轮一次后台 checkpoint 调用约 **53 秒**；40 次栈采样中 checkpoint 线程均在等更新屏障独占锁。**这是调用总耗时，不是 53 秒 stop-the-world。**
- dirty eviction 中的 snapshot 写回触发 RocksDB write-thread 等待/write stall，导致需要换入的查询也受写入 backpressure 影响；未证明 p99 差异全部由延迟启动引起。
- 单独实验将 `head_snapshots` write buffer 从 4 改为 16 MiB：压力场景 ACK/s 提升 17.2%、p99 约 399 -> 170 ms，但全驻留写入下降 **33.7%**，因此**不合入默认配置**。仅计入实际 OPTIONS 已验证生效的实验。
- 当前没有相同 durability 语义的非租户写入对照，不能从上述数字计算写入 tenancy tax。

## 8. 新设计提案：将 Log DB 与 Posting DB 合并

- **已完成设计，未实施**：候选为一个 RocksDB 引擎、State/Recovery 两组独立 CF；不是取消应用 redo，也不是把所有数据放进一个 CF。
- **不能直接整库回滚**：若 checkpoint 后的操作已 ACK，合并 DB 后回滚整库会连它的 redo 一起丢掉。
- **也不能保留最新 postings 再盲目重放旧 head 后的日志**：posting append merge 不幂等，可能重复追加，head/postings 状态也不匹配。
- **候选协议**：冻结全部 State 修改，导出一致的 State CF 组及 manifest/catalog/每 tenant 水位；发布恢复指针成功后才能裁剪日志。
- **恢复策略**：保留最新 Recovery CF，只重建 State CF；记录 restore intent，崩溃后从同一已发布 State checkpoint 重新开始，再重放新日志。
- RocksDB 有 CF export/import API，但恢复耗时、空间成本、失败中断和全部 crash boundary 尚未验证；两个现有引擎已经共享 cache，合并不等于 cache 减半。
- **当前建议保留双引擎交付**：单引擎可能增加 snapshot/posting 写入与 redo append 的 write-stall 耦合，需要独立原型和故障/性能矩阵后再决定。

## 9. 交付状态与下一阶段重点

- **已交付代码与工具**：客户端五种 API、并发读写协议、tenant 实例缓存、epoch 生命周期、workspace/快照优化、管理员内部指标，以及 smoke、规模 benchmark 和 profiling 工具。
- **发布状态**：前一批优化已推送为 `b1818eec`；最新 workspace/worker 优化、关闭修复及多规模实验文档仍在本地，未新增 commit/push。
- **优先待解决**：checkpoint 有界进入/公平性、dirty writeback backpressure、混合读写 p99；明确生产环境的 recall 目标及搜索预算配置方式。
- **按原范围延期**：自动 checkpoint 调度、tenant 计算准入与公平调度、分布式/TiKV。当前持续写入需上层显式 checkpoint。
- **尚无结论的范围**：cold SSD、TCP/开放环路排队、长期 QoS、掉电恢复、全部故障边界。原公共 socket 的已知断连健壮性问题也未整体加固，不能宣称已达到完整生产就绪状态。

完整设计、参数、逐轮数据与局限见 [docs/MultiTenantDesign.md](docs/MultiTenantDesign.md)：缓存 §1.4，接口 §12，首批优化 §13.4，单 DB 设计 §14，多规模实验 §15。

## 10. 对齐原 SPANN 的收敛版本（2026-11-06）

以下为 11 月 6 日 BKT 版本的交付快照；后续 KDT 适配见 §11。

- **已撤回** sparse visited clear、lazy/vector heap、延迟启动 maintenance workers、未使用的 KDT hook；严格 legacy query parsing 缩小到 tenant 模式。`Heap.h` 和 KDT 头文件与原始版本一致。
- **配置对齐**：posting 使用原 RocksDB integrated blob、Bloom、compaction/write-buffer 配置，节点共享 cache 默认恢复 **3 GiB**；算法默认恢复 **MaxCheck=4096、SearchInternalResultNum=64、ReplicaCount=8、ExcludeHead=true** 等，不再强制低预算 256/32。
- **保留必要改动**：共享 namespace/ownership、实例隔离 workspace、epoch/lease、VID/日志与 checkpoint 协议。小型公共 shutdown/socket/client 正确性修复按独立 prerequisite 审查，而非和可选算法优化混合提交。
- **发现并修复实际 bug**：恢复 head-only 存储后，原路径会返回已删除的 head；仅对 managed 查询补全 deletion 检查，未改普通 legacy 行为。Release/ASAN/LSAN 28 用例、2,434 断言及真实 TCP/Python smoke、legacy 算法兼容通过。
- **性能重测已完成**：六个规模全部重建，114 个受控读计时运行、25M case-vectors 全量检查以及混合更新/两类 SIGKILL 恢复通过。新输出位于 `/mnt/sptag-tenant-alignment-7de262b3-20261106`，完整设置、必要参数例外和原始证据入口见 [设计文档 §16](docs/MultiTenantDesign.md#16-对齐原-spann收敛-multi-tenant-补丁2026-11-06)。

**该批 BKT 同配置 private 对照：** 总计 1M 向量、1 tenant、全驻留，4096/64 搜索预算、双方各 3 GiB 配置 cache、相同索引内容和文件系统，三轮中位数。C8 的 **tenant 8,037.6 QPS / private 8,100.1 QPS，吞吐差异 −0.77%**；C1 为 −0.67%。C8 p99 为 1.166 / 1.156 ms，peak RSS 为 0.211 / 0.155 GiB。三轮 QPS 范围重叠，因此不把约 0.8% 宣称为精确固定开销；旧约 15% 差距不再复现，但也不能把旧差距全部归因于 blob 单项。

下表为另一组全驻留 uniform、C8 的 tenant/direct 矩阵批次；Recall 用每 tenant 前 200 条 query 的 exact truth，QPS 用完整 10K 查询：

| 总向量 / tenants | tenant QPS | direct QPS | macro Recall@5 |
|---|---:|---:|---:|
| 1M / 1 | 8,046 | 7,940 | 97.700% |
| 1M / 8 | 7,453 | 7,385 | 99.450% |
| 1M / 32 | 8,186 | 8,112 | 99.863% |
| 2M / 128 | 4,877 | 4,870 | 99.917% |
| 10M / 32 | 6,402 | 6,376 | 99.006% |
| 10M / 128 | 4,655 | 4,662 | 99.634% |

- 全驻留 uniform/hot70 的 11 个场景中，tenant/direct 吞吐差异为 **−1.14% 至 +1.34%**。direct 同样使用共享存储，不是 128 个 private DB 的对照。
- 四分之一驻留槽位下，uniform QPS 为 **303–1,195**、p99 为 **17–100 ms**；输出与全驻留一致，但冷加载开销仍明显。
- 2M/128、4 writers + 8 readers，单次验收样本中，全驻留/32 slots 的插入 ACK/s 为 **534 / 316**，read p99 为 **6.35 / 41.85 ms**；两组各完成 16,384 个 ACK 插入和 128 个删除。压力组仍出现 **52.43 s checkpoint 调用**，不能宣称公平进入问题已解决，也不是 52 秒停止所有查询。
- 当前约 8K QPS 对应 **97.7% recall**，历史约 25K 对应 **76.2% recall**；算法参数、公共优化和存储设置均有变化，不能直接把 QPS 降幅当作 tenancy overhead。当前结果不替代 §1–9 的历史记录。
- 所有新改动和文档仍在本地，未新增 commit/push；单 DB 合并和原先延期的功能没有顺带实施。

## 11. 移除强制 BKT，恢复原 head 选择（2026-11-09）

- TenantManager 不再指定 BKT 默认值；省略 head 直接继承原 SPANN（当前 KDT），显式 BKT/KDT 委托原配置及工厂实现。
- KDT 只新增 managed 实例 workspace 和在线保存配置保护，不改树/图算法；旧 BKT 索引按原配置恢复，不自动迁移。
- 内部 Create、C++/Python CreateTenant 和 CLI 都支持可选 head；原调用方式继续可用。显式 head 使用 v2 Create 请求，默认请求与响应保持 v1。
- 本次不重跑六规模矩阵；§10 约 0.77% 差异仍为此前 BKT/BKT 对照。详情和产物路径见 [设计文档 §17](docs/MultiTenantDesign.md#17-复用原-spann-的-head-选择2026-11-09)。
