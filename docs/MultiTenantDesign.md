# SPANN Multi-Tenancy: Initial System Design

状态：已实现单节点多租户的共享存储与恢复、同租户并发读写、增量工作快照、LRU 驻留缓存、管理员内部状态接口，以及基于现有 C++ `AnnClient` / SWIG Python `SPTAGClient` 的五种在线操作。**仅面向可信调用方，不提供认证授权、请求去重或完整资源治理。** 第 11 节记录实现范围，第 12 节记录接口契约；缓存设计见 §1.4。原有 BT comments 保留。

接口阅读入口：**[第 12 节：用户接口契约](#12-用户接口契约)**，首先说明在线客户端 API，再说明内部 C++ Manager、旧 socket 查询和离线 CLI；用户不需要直接操作 Manager。

**2026-11-06 范围收敛：** 该轮恢复了原 SPANN 的 posting RocksDB 配置及搜索等算法默认值，撤回通用 heap/hash 和维护线程延迟启动优化。§13、§15 是此前版本的历史测量，不能据此描述当前版本；对齐范围、当时的配置差异与重测记录见 §16。

**2026-11-09 head 选择修复：** 新建 tenant 不再强制 BKT，默认沿用原 SPANN（当前为 KDT），也可显式选择 BKT/KDT；已有索引按保存的配置加载，不自动切换。实现和接口见 §17。§16 的性能结果双方均使用 BKT，不能作为新默认 KDT 的性能结果。

**Benchmarking 分支集成：** `tenant-benchmark` 基于上游 `539f6a6`，复用其多层 SPANN，见 §18。该分支使用新的节点格式；§17 对旧 BKT fixture 的兼容结论仅适用于原 `tenant` 分支，不代表跨基线数据格式兼容。本文历史性能数字未在新基线上重测。

## 并发读写改造设计

本轮实现目标是恢复原动态 SPANN 在同一个索引上的并发读写能力，不通过整个 tenant 串行化实现多租户。以下契约取代本文此前的“同 tenant 一次一个请求”简化；不引入普通用户管理 API、自动 checkpoint 调度或无关的公共组件加固。

### VID 和日志

- **仅同一 tenant 内的写线程协调 VID。** 每个 SPANN 实例保留自己的 `m_dataAddLock` 和 VID 空间；不同 tenant 可以分配相同数值的 VID，没有节点级 VID 分配锁。
- 把插入拆为“实际预留 VID”和“使用已预留 VID 插入”。Manager 不再根据当前向量数预测 VID，再调用会重新分配 VID 的普通 `AddIndex`。
- 每 tenant 的提交临界区只包含容量检查、VID 预留及应用日志追加。不同 tenant 不共享提交锁；同 tenant 的 posting 更新、head 搜索和派生维护在该临界区之外并发执行。索引原有分配锁仍负责实际分配，提交锁额外协调该 tenant 的日志顺序。
- 日志记录明确的 VID 范围和向量 payload；执行和恢复均使用记录的 VID。恢复仍可顺序执行；旧版本日志保留可读性，不因并发实现而丢弃已有恢复材料。
- 日志序号表示已记录操作，不等于最大已完成操作。不同写入允许乱序完成，checkpoint 必须排空更新与维护后才能发布对应日志水位。
- 预留或日志追加后的不确定失败使节点停止接受新操作，保留原 checkpoint 和日志以便重开恢复；不继续发布带有未记录预留或部分更新的快照，不随意回收 VID。
- 对尚在插入中的 VID 的删除返回 busy，而不是越过插入；重复删除的存在性检查、日志追加和删除协调执行，避免记录不能重放的重复删除。

### 操作与生命周期保护

- 已驻留查询通过节点级 epoch 保护目录和实例，不获取节点生命周期屏障或全局缓存锁；元数据 `Describe` 也走 epoch 目录。更新和冷加载仍使用生命周期屏障及内部 lease。创建的构建阶段使用共享保护，发布、删除和关闭仍获得节点独占保护，但不会因此停止其他已驻留 tenant 的新查询。
- 缓存状态和条件变量合并同 tenant 的并发加载，不覆盖整个查询/更新。已加载查询不持有提交锁。
- 单次插入等待本次操作所派生维护任务完成，不等待其他请求持续产生的全部任务；维护任务必须传递完成上下文。checkpoint、卸载和关闭仍排空整个索引。
- 查询不提供整个批次的事务快照：与更新重叠时可以看到部分进行中的变化；成功更新返回后的后续查询应看到其效果，受原有近似搜索语义限制。

### Checkpoint 和增量保存

- 独立的更新屏障使 checkpoint 暂停新更新，等待在途更新及维护结束；生命周期屏障固定 tenant 集合。
- 已加载 tenant 的只读查询不获取更新屏障，可以与 checkpoint 并行；按需加载会等待更新屏障，避免恢复文件与保存过程竞争。
- 保存路径必须只读地序列化在线索引，不做在线 refine、参数改写或查询触发的 merge。必要的底层并发修正仅限该路径的直接依赖。
- version-map 沿用原 SPANN 的访问和写侧增长协调，不增加逐候选读锁；继承的待讨论风险见 §1.1。managed 维护统计保留实例内保护；SPANN/BKT/KDT 保存配置时不改写在线查询使用的 hash 参数。
- 每 tenant 记录当前日志序号、工作快照序号及快照存在性，仅重新保存 dirty tenant；未修改 tenant 的快照保留在共享 State DB 中，节点恢复点仍包含完整清单。
- dirty tenant 仍调用原 SPANN 的完整文件序列化，但导入 `head_snapshots` 时逐个比较 1 MiB chunk，只写变化或新增的块，删除缩短文件的尾块及已消失文件的旧块，最后写 manifest。比较读取不填充 block cache，避免挤占 posting 查询缓存；不改变节点 checkpoint 的发布、日志清理和失败恢复协议。
- 工作快照保存成功与节点恢复点发布成功是两个阶段；只有后者允许回收对应应用日志。新 tenant、恢复重放、卸载和重新加载均维护上述状态。

### 在线调用与验收

- 新增的五个 Python tenant API 在同步网络等待期间释放 GIL；旧 `Search` 调用契约保持不变。
- 同一连接的并发 tenant RPC 需要有序发送完整帧，不能重叠执行 `async_write`。发送队列是支撑本轮并发接口的直接依赖；不顺带改造无关 Socket 行为。
- 验收覆盖同 tenant 多写入、读写重叠、独立 tenant 重复 VID、重复删除、按需加载、checkpoint 时查询继续、未修改快照复用，以及并发写入后崩溃重放和再次恢复。

## 1. 目标与假设

目标是在同一个系统中支持多个用户，每个用户拥有自己的 SPANN 索引，能够独立查询、更新、删除和恢复。

已确认要求：

- 一个 user 对应一个 tenant，一个 tenant 同时只拥有一个索引。
- 第一版只接受可信调用方，以 tenant_id 路由并检查索引状态；不实现账号、token、角色、ACL 或租户授权，不直接对不可信终端用户开放。
- 第一阶段面向单节点，使用两个节点级共享 RocksDB：State DB 保存索引状态，Recovery DB 保存应用恢复日志。不是每个 tenant 创建两个 DB。
  - BT comments: 我觉得单机上使用RocksDB吧，然后分布式的情况下我们使用Distributed TiKV；目前我们可以关注在单机场景。之所以使用RocksDB是因为我觉得上生产环境需要一个成熟的DB。
- 少量按用途划分的 Column Families（CF），通过 tenant/index 前缀隔离数据，不为每个 tenant 创建独立 CF。
- 尽量沿用现有 checkpoint + 应用日志重放机制，采用节点级一致 checkpoint 和节点级恢复，允许 checkpoint 期间暂停所有 tenant 的更新。
- 目标是进程崩溃后的正确恢复；暂不保证机器掉电或操作系统崩溃后，已成功确认的近期写入不丢失，不要求每次写入同步落盘。
- 不支持历史回滚或在线全量重建，不引入 generation 概念及新旧索引切换机制。
- 不支持普通用户跨 tenant 搜索。
- 不在第一阶段解决单个 tenant 的跨节点分片。

核心建议：**每个 tenant 一个独立 SPANN 索引，共享 serving 进程和受控执行资源；在 SPANN 上增加 Tenant/Index Manager，而不是将所有 tenant 数据混入一个索引后再过滤。**
  - BT comments: 我同意你的看法，但是我认为不同的SPANN的postings需要共享一个storage engine，比如大家共同用RocksDB来进行管理，否则每个SPANN index创建一个RocksDB这样开销就会很大？

上述 BT comments 已纳入设计：所有索引共享 postings 存储；为减少对现有 recovery 模型的修改，应用日志仍放在另一个共享 DB。TiKV 是未来方向，不在本阶段实现。

### 1.1 开发范围与优先级约束

本次工作的目标是为 SPANN 增加 multi-tenancy，而不是顺带重构或全面加固原有系统。后续实现和 review 遵循以下边界：

- **优先完成 multi-tenant 功能及其直接相关的正确性与 robustness。** 包括租户路由与数据隔离、共享存储的命名空间和所有权、租户生命周期并发、节点 checkpoint 与恢复一致性，以及新增 tenant API 的输入校验、错误处理和对象生命周期。
- **修复本 feature 引入的问题，以及实现该 feature 必须解决的直接依赖问题。** 必要时可以修改公共代码，但应说明与 multi-tenancy 的直接关系，保持最小改动，不扩展为通用改造。
- **无直接关系的既有 robustness、性能优化或代码清理只记录，不默认实施，也不列为本 feature 的最高优先级。** 是否开展由用户与 manager 商量后另行决定。
- 区分“multi-tenant 必须完成的工作”和“公共组件的后续维护建议”；不能仅因为某个公共组件被多租户路径复用，就把其所有历史问题纳入本次范围。

公共网络层事项的范围记录：

| 事项 | 当前依据与处理方式 |
|---|---|
| 连接提前断开时，`Connection::Start/Stop` 中 endpoint 查询可能抛异常 | 文档已有历史观察记录；作为公共 Socket robustness 事项保留，待用户与 manager 确认后安排 |
| `Connection::AsyncSend` 的异步写入可能重叠 | 本轮同一客户端并发 tenant RPC 直接依赖完整帧发送，因此增加 strand 上的发送队列；不扩展为其他 Socket 加固 |

若后续证据表明上述问题直接阻碍 multi-tenant 的必要行为，应明确具体场景和依赖关系，再重新评估最小修复范围，而不是自动扩大任务。

#### 2026-11-04 性能优化范围决议

以下是已确认的实施边界；当前生命周期和 workspace 实现分别见 §1.4、§6.1，性能对照需区分改造前快照与优化后版本：

- **恢复原有 SPANN 的轻量读写策略，不借 multi-tenant 改造额外加固原实现。** `VersionLabel` 已恢复为原始 `cafdd0ab` 的实现，撤回 managed 路径新增的逐候选 shared-lock/统一读写锁保护，沿用原版的状态访问、原子删除/CAS 和写侧增长协调。不另行增加原子分块存储、通用 `Dataset` 重构或更强的查询一致性协议。
- 保留 multi-tenant 必需的租户隔离、tenant 内 VID/日志提交协调、显式 VID 重放、请求维护完成确认、lease 和 checkpoint/lifecycle 屏障；恢复 SPANN 读写策略不等于回退这些多租户协议。
- 已批准的 workspace 优化是恢复线程局部复用的性能优势，同时补齐索引实例隔离和淘汰回收；继续使用节点级长驻服务线程池，不按请求或 tenant 创建线程池。
- 按较大 refine 预算分配 workspace、每次查询清零 visited 表，是原始 `cafdd0ab` 已有的实现，不作为本轮 multi-tenant 回退修复的必做项。
- 原实现的潜在 concurrency 风险先记录，后续与 manager 讨论，或在测试复现实际问题后再评估处理；不得将未证实的风险当作扩大本轮改造范围的理由。

原始并发实现的待讨论项（静态代码观察，不是已经复现的故障）：

| 观察 | 已核对的原始实现 | 本轮处理 |
|---|---|---|
| 版本/删除字节的普通读取与原子写入混用 | `cafdd0ab` 的 `VersionLabel::Deleted/GetVersion` 直接读取，`Delete/IncVersion` 使用原子 exchange/CAS；完整内存模型保证需进一步核对 | 记录，不在本轮引入新的状态同步机制 |
| 版本表增长的读侧可见性 | 原有 `Dataset` 已分块并 reserve 块目录；`incRows` 为普通变量，写侧在 `AddIndex` 的 `m_dataAddLock` 内增长，查询不获取该锁 | 记录，不因风险推断而重写分块结构或增长发布协议 |

### 1.2 延后实现的 feature

- **tenant 级请求准入、并发限额和公平调度延期到分布式阶段。** 当前不实现；届时结合租户负载及 compute nodes 自动扩缩容设计，不作为本轮单节点 multi-tenant 完成的前置条件。
- **自动 checkpoint 调度：列入未来 feature，当前不实现。** 先完成 multi-tenant 相关功能及其直接依赖，再安排该功能。未来可考虑按时间或未纳入 checkpoint 的日志量触发，具体策略和阈值届时确定。
- 原有 SPANN 已有显式 checkpoint 执行入口，并由 SPFresh 更新流程在每轮更新后调用；缺少的是常驻服务的自动调度，不是本次 multi-tenant 改造造成的功能退化，也不作为当前 multi-tenant 功能完成的前置条件。
- 自动触发与增量保存是不同能力：当前已实现未修改 tenant 的快照复用、dirty tenant 的未变 chunk 复用，以及节点 checkpoint/恢复一致性保护。延期的只是自动调度；持续写入期间仍需上层显式调用 `Checkpoint()`，以控制日志增长和恢复重放量。

### 1.3 下一阶段单节点工作

- **自动驻留缓存：已实现。** LRU、容量目标、加载合并、lease 和 dirty eviction 见 §1.4；不向普通客户端开放 Unload。
- **生命周期并发：已实现驻留查询的 epoch 快路径。** 创建发布、删除和关闭仍需全局提交协调，但不再通过该屏障暂停其他已驻留 tenant 的查询；更新和冷加载仍可能等待，不能称为所有操作无锁。
- **管理员状态与指标：已实现内部 `GetStats` 和离线 `tenanttool stats`。** 面向系统管理员，不扩展普通客户端的租户枚举权限；尚无独立在线管理员网络端点。
- **四租户 serving 性能实验：已完成，见 §13。** 保留改造前基线、改造后全驻留和缓存压力的独立结果，以及数据、配置与复现脚本。

### 1.4 单节点驻留缓存与生命周期实现设计

- 缓存单位是 **tenant 的整个 SPANN 实例**：head 图、version/map 等辅助状态和实例 workspace 一起加载/释放；postings 继续存储在共享 RocksDB，RocksDB block cache 独立配置。不是把所有 tenant 的 head 合并成一张图，也不是逐个 head 向量淘汰。
- 使用 LRU 淘汰空闲实例。`MaxResidentTenants` 默认 8，约束在线缓存中驻留及正在加载的实例数；`MaxResidentBytes` 默认 0（关闭字节目标），启用时按最近工作快照组件的总字节数计费。该计费可比较不同大小索引，但**不是 RSS 硬上限**：不包含线程栈、workspace 高水位、分配器、共享 DB、请求 payload 等；写入增长在下次保存后刷新计费。
- 若希望全部 tenant 在访问后保持驻留，可设置 `MaxResidentTenants=MaxTenants` 且 `MaxResidentBytes=0`；创建/恢复后仍按需加载，不做全量预热。没有单独的后台定时扫描或 RSS 压力探测器。
- 驻留查询先进入节点 epoch，再从不可变目录读取稳定 tenant entry 和原子发布的实例指针；查询完成后退出 epoch。更新和冷加载继续使用内部 lease，在途维护包含在更新请求的 lease 内。LRU 跳过观察到有活动读者或 lease 的实例；选中后撤回指针并等待旧 epoch 读者，防止选择与新读者进入之间的竞争。容量不足且没有空闲候选时等待，不把缓存容量等待当成 tenant 计算准入或租户配额。
- 单个索引超过字节目标时允许它独占缓存，避免该 tenant 永远不可访问；统计明确报告超目标。加载/淘汰操作不持有全局缓存元数据锁进行磁盘 I/O；其他已驻留 tenant 的请求继续执行。相同 tenant 的并发加载合并，状态为 `Unloaded/Loading/Resident/Evicting`。
- 淘汰 dirty 实例先排空维护并保存工作快照，再释放内存；不会因此发布节点 checkpoint 或提前清理日志。启动时逐 tenant 验证、重放并保存必要工作快照，再释放该实例，避免所有 tenant 同时驻留；启动完成后按需加载。恢复仍是节点级，首次启动验证不是后台 lazy recovery。
- 本进程已恢复或保存成功的工作目录文件可直接用于再次加载，避免每次 cache miss 都把相同 head 从 RocksDB 重新复制到磁盘。该就绪标记不持久化；重启仍从已发布 checkpoint 建立新工作副本，不信任上次进程的临时文件。
- 慢路径锁顺序为管理操作协调锁（仅管理路径）→节点生命周期屏障→更新屏障→缓存元数据锁。驻留查询不获取这些管理锁，每线程首次进入节点时注册 epoch record，命中时不递增全局 epoch。miss 必须退出 epoch 后才能等待加载、容量或屏障，避免等待自身回收。冷加载、淘汰和更新仍参与更新屏障；checkpoint 排除它们，但允许已驻留查询。
- 目录通过不可变快照发布，实例通过原子指针发布；生命周期慢路径先撤回指针，再推进 epoch 并等待旧读者，最后排空维护、保存和释放。在线淘汰在实际释放前仍占用容量。节点级 epoch 的代价是一个 tenant 的长查询可能延迟其他 tenant 的回收；新 epoch 的读者不会阻止旧代回收。读者退出不唤醒全局条件变量，容量等待和 epoch 等待使用 50 微秒定时检查；不保证固定换出延迟。
- 创建仍串行化管理操作，但在节点共享保护下构建私有实例，其他 tenant 的读写可继续；构建结束后保存并释放私有实例，在不包含构建的全局独占提交阶段加入 Catalog 并发布节点 checkpoint。提交仍可能保存其他 dirty tenant，不承诺固定暂停时长。构建阶段可临时额外驻留一个实例，不计入在线缓存槽位；输入/构建峰值不是缓存预算的保证范围。
- 显式卸载撤回目标实例，等待其更新 lease 和节点旧 epoch 读者；其他 tenant 的新驻留查询可继续。创建、删除、checkpoint、关闭串行协调，避免未提交构建状态进入其他 checkpoint。关闭撤回整个目录并等待旧读者；删除只停止目标 tenant 的新索引访问，随后更新目录。
- 管理员通过内部 C++ 状态快照读取节点 failed/closed、缓存容量、驻留数/计费字节，以及每 tenant 的状态、lease 数、命中/缺失、加载/淘汰次数与耗时。计数为进程内诊断，不持久化；不增加普通客户端 List/Unload 权限。

| 路径 | 主要保护与锁顺序 |
|---|---|
| 驻留查询 / Describe | 进入线程 epoch → 读取不可变目录；Search 再读取已发布实例，退出前更新 tenant 的最近访问时间 |
| 更新 | lifecycle shared → updates shared → 取得 lease；随后 tenant submit 临界区，索引更新在其外执行 |
| 冷加载/淘汰/卸载 | 先退出查询 epoch；lifecycle shared → updates shared → cache 状态转换；撤回实例后在 cache 锁外等待旧读者并执行 I/O |
| 创建 | management → lifecycle shared → updates shared 私有构建；释放 shared 锁后 lifecycle exclusive 发布 |
| Checkpoint | management → lifecycle shared → updates exclusive |
| 删除/关闭 | management → lifecycle exclusive |
| 管理员统计 | lifecycle shared；每 tenant 先 submit 再 cache，不持有 cache 锁等待日志提交 |

## 2. 当前代码基础

当前项目并非完全没有多索引能力：通用 Server 已经可以加载多个命名索引，但这不等于具备多租户隔离和资源管理。

BT comments: 原来的[Index] List的作用是什么？会支持同时serve多个SPANN index吗？还是有其它的目的呢？

| 代码位置 | 现有能力 | 对设计的影响 |
|---|---|---|
| [`ServiceContext.cpp`](../AnnService/src/Server/ServiceContext.cpp) | 从 `[Index] List` 加载多个索引，保存为 `map<string, shared_ptr<VectorIndex>>` | 可以复用注册和加载思路，但需要动态生命周期管理 |
| [`SearchExecutor.cpp`](../AnnService/src/Server/SearchExecutor.cpp) | 根据请求中的 `indexname` 选择一个或多个索引 | 改为按可信调用方提供的 tenant_id 路由到唯一索引，并检查索引状态 |
| [`SPANN/Index.h`](../AnnService/inc/Core/SPANN/Index.h) | head index、version map、extra searcher、options 基本属于实例状态 | 独立 SPANN 实例适合作为数据隔离单位 |
| [`ExtraDynamicSearcher.h`](../AnnService/inc/Core/SPANN/ExtraDynamicSearcher.h) | 每个实例维护后台任务、workspace ID 和存储对象 | 需要控制每个索引带来的线程、内存和 I/O 成本 |
| [`KeyValueIO.h`](../AnnService/inc/Helper/KeyValueIO.h) | 抽象 Get、MultiGet、Put、Merge、Delete、扫描、checkpoint 和关闭操作 | 保留抽象，区分索引级存储视图与节点级 engine 所有权 |
| [`ExtraRocksDBController.h`](../AnnService/inc/Core/SPANN/ExtraRocksDBController.h) | 每个对象自行打开、关闭 DB，整数 key 未加索引命名空间 | 改为注入共享 engine 和限定命名空间的视图 |
| [`PersistentBuffer.h`](../AnnService/inc/Core/SPANN/PersistentBuffer.h) | 用独立 RocksDB 保存插入/删除日志并扫描重放 | 沿用其 checkpoint + 逻辑重放模型；新 manager 接管命名空间和提交顺序，关闭其管理索引的旧 WAL，旧单索引入口保持不变 |
| [`MultiIndexScan.h`](../AnnService/inc/Core/MultiIndexScan.h) | 跨多个索引联合检索和排序 | 不是 tenant manager，不作为本设计核心 |

需要分别解决两个问题：

1. 多个 SPANN 实例可以同时正确运行。
2. 多个 tenant 的请求限定在各自选中的索引内，并在受控资源预算下执行；调用者是否有权选择该 tenant 由可信调用方负责。

## 3. 总体架构

```text
Trusted Caller
  |
  v
API
  |  请求携带 tenant_id
  v
Tenant Router + Index State Check
  |  检查 tenant 是否存在、索引是否可用
  v
Admission Control
  |  配额、并发与有界排队（资源控制，不是权限校验）
  v
Index Manager
  |  获取目标 tenant 的可用索引及生命周期 lease
  +--------------------+--------------------+
  v                    v                    v
Tenant A Context       Tenant B Context     Tenant C Context
  SPANN index            SPANN index          SPANN index
  head / versions        head / versions      head / versions
  workspace pool         workspace pool       workspace pool
  namespaced store       namespaced store     namespaced store
          \                    |                    /
           +-------------------+-------------------+
                               |
                    Storage Engine Manager
                       /               \
                Shared State DB     Shared Recovery DB
                postings CF         recovery_log CF
                head_snapshots CF   catalog CF
                checkpoint_meta CF  recovery_meta CF

Catalog Manager <----> Index Manager
Node Checkpoint / Recovery Coordinator
  协调所有索引、两个 DB 和恢复进度

Shared CPU / Memory / SSD
  由节点预算和租户配额共同约束
```

隔离原则：

- 索引内存状态独立，持久化数据通过命名空间逻辑隔离；底层 DB、CF 和执行资源共享。
- 执行资源尽量共享，但必须有节点级预算和 tenant 级配额。
- 不为每个用户默认启动独立服务进程。
- Index 只持有存储视图，不拥有共享 DB，卸载单个索引不能关闭节点的数据库。

共享进程和共享 DB 不提供租户级崩溃隔离或硬内存隔离。数据库故障可能影响整个节点；不能承诺每个 tenant 都能独立恢复。强隔离部署留待后续设计。

## 4. 核心组件

### 4.1 Tenant Catalog

Catalog 是逻辑上的 metadata service（索引登记册），持久化索引归属、配置和期望状态。第一版实现为进程内 Catalog Manager，不单独部署服务，也不额外创建第三个 DB。

建议记录：

```text
TenantRecord
  tenant_id
  index_id
  dimension
  value_type
  distance_metric
  storage_namespace
  desired_state
  resource_quota
```

Catalog 保存在 Recovery DB 的 `catalog` CF，并在 State checkpoint 的 `checkpoint_meta` 中保存相同清单及每索引日志进度。**第一版创建/删除都必须完成节点 checkpoint 才返回成功**；Recovery DB 中的 Catalog 和当前 checkpoint 指针通过同一个 WriteBatch 发布，因此不存在“已经确认但尚未进入 checkpoint”的生命周期变化。

这是实现中采用的简化：不增加生命周期日志和初始构建输入的独立保留协议。重启使用已发布 checkpoint 中的清单，校验它与 Recovery Catalog 一致，再重放向量更新。加载状态由 manager 在内存管理；上面的 `desired_state/resource_quota` 是后续扩展，不是当前持久化字段。

`index_id` 是不复用的内部索引实例 ID，不表示一个 tenant 同时拥有多个索引。例如，删除后重新创建仍属于同一 tenant，但会分配新 ID，避免旧任务或延迟清理误操作新数据。普通插入、删除、checkpoint 和重启不改变这个 ID。

### 4.2 TenantContext

每个 tenant 的运行时上下文建议持有：

```text
TenantContext
  tenant_id / index_id
  SPANN index instance
  runtime state
  instance-local workspace pools
  resource quota / usage
  in-flight requests / iterators / background jobs
  lifecycle synchronization
```

每个 tenant 只有一个当前有效索引。Checkpoint 是该索引的恢复材料，不是另一份在线索引。

### 4.3 Index Manager

Index Manager 负责完整生命周期，而不只是提供 map 查询。

当前 C++ 接口位于 `SPANN/TenantIndexManager.h`：

```text
Open(root, limits, manager)
Create(tenant_id, value_type, dimension, metric, seed_vectors, count, headIndexAlgo = Undefined, postingLayers = 0, configuration = nullptr)
Insert(tenant_id, vectors, count, first_id)
DeleteVector(tenant_id, vector_id)
Search(tenant_id, query, k, results, expected_type)
Unload(tenant_id)
DeleteTenant(...)
Describe(tenant_id, value_type, dimension)
List()
GetStats(node_stats, tenant_stats)
Checkpoint()
Close()
```

当前缓存的运行时状态转换：

```text
Unloaded -> Loading -> Resident -> Evicting -> Unloaded
```

创建未发布的实例不对查询可见；删除提交后从 Catalog 移除。加载或保存失败采用**节点 failed**，而不是允许其他 tenant 继续服务的 tenant 级 FAILED 状态。

关键约束：

- 同一 tenant 的并发加载合并为一次操作。
- 第一版不暴露原始 index 指针或 `AcquireIndex`；manager 的整个操作作用域就是内部 lease，通过节点 shared/exclusive 屏障和 tenant 锁保证对象存活。
- 第一版不提供 iterator。未来增加时必须让 iterator 在整个使用周期内持有 lease，而不是仅在创建时持有。
- 卸载或删除先停止接收新请求，再等待查询、iterator 和后台任务结束，最后释放该实例及存储视图，不关闭共享 DB。
- 后台任务也必须参与生命周期计数或持有等价的执行租约。
- 普通请求持有节点生命周期共享屏障；同 tenant 查询和更新可并发，只有 VID 预留、日志提交和删除协调使用 tenant 内短临界区。删除尚在插入中的 VID 返回 `ExternalAbort`；普通读写竞争不再返回 busy。
- 创建的构建阶段允许其他 tenant 读写；卸载只等待目标 tenant 的 lease。创建发布、删除和关闭仍持有节点生命周期独占屏障，可能阻塞其他 tenant。独立 Checkpoint 使用更新独占屏障，不阻塞已加载 tenant 的只读查询；按需加载和淘汰等待该屏障。
- 加载、恢复和关闭失败必须显式报告，不能伪装为成功。
- 创建、删除和卸载必须与节点 checkpoint 屏障协调；已卸载索引的恢复材料也不能遗漏。

仅有 `shared_ptr` 不够：它可以保持对象存活，但不能表达禁止新写入、后台任务排空和存储关闭顺序。

### 4.4 Tenant Router 与 API

第一版面向可信环境，不在服务内部实现认证和授权。请求路径：

```text
可信调用方提供 tenant_id
  -> Tenant Router 查找对应索引
  -> 检查 tenant 是否存在及索引状态
  -> 配额与准入检查
  -> manager 内部获取受生命周期保护的索引
  -> Search / Insert / Delete
```

tenant_id 是路由标识，不是访问凭证。可信调用方负责传入允许访问的 tenant_id；若面向不可信用户，需要由上游业务服务或网关提供认证和授权。本服务不能因此被描述为具备用户访问权限隔离，数据 API 和管理 API 均不得直接暴露给不可信调用方。

Router 不实现 owner/ACL 校验，但必须处理不存在、未就绪、正在排空或删除的索引；状态检查与操作的内部 lease 获取必须协调，不能因检查后的并发删除访问失效实例。`Describe` 只是元数据查询，不是 lease；服务端把解析使用的数据类型传入 `Search` 再次校验，Search 也重新检查存在性及输入大小。不得回退到其他 tenant 的索引，也不接受任意文件路径或裸存储 key 来绕过 namespace。

不同 tenant 可以使用不同的维度、数据类型和距离函数。输入必须针对目标索引校验，不依赖节点级默认参数。

现有 socket Server 增加可选 `[MultiTenant] Root` 模式。旧 Search 请求用 `$tenantid:<id>`；要求一个显式 tenant，拒绝 `indexname`、跨 tenant 搜索和未实现的 metadata 提取。新增 TenantRequest/TenantResponse 协议支持在线创建、删除、插入和查询，每次调用显式指定一个 tenant，并复用服务端同一个 manager。未配置 Root 时保留旧 server 模式，新 tenant RPC 返回 `Socket_UnsupportedRequest`。

### 4.5 Storage Engine Manager

节点统一拥有两个 RocksDB 实例和 CF handles，管理数据库生命周期、共享资源及 DB 级 checkpoint。TenantContext 通过绑定 `tenant_id/index_id` 的 `KeyValueIO` 视图操作数据，不能取得任意租户的裸 key 访问能力。

继续演进已有 `KeyValueIO`，不另起一套平行 backend 框架。需要区分视图释放与 engine 关闭、索引状态保存与节点 DB checkpoint，并补充恢复协议实际需要的 batch、扫描和错误语义。同一 DB 内可以跨 CF 原子 batch；两个 DB 之间没有这种原子性。

## 5. 存储与恢复

### 5.1 两个共享 DB 与少量 CF

下面是建议布局，CF 数量固定，不随 tenant 数增长：

| DB | CF | 用途 |
|---|---|---|
| State DB | `postings` | 所有索引的 posting lists |
| State DB | `head_snapshots` | head 及匹配的 version/deletion、posting size、checksum、必要映射等序列化状态 |
| State DB | `checkpoint_meta` | 节点恢复点的索引清单、snapshot 引用和日志进度 |
| Recovery DB | `recovery_log` | checkpoint 后的应用恢复记录 |
| Recovery DB | `catalog` | 与当前 checkpoint 原子发布的租户清单和配置 |
| Recovery DB | `recovery_meta` | 有效节点 checkpoint 的发布信息和必要恢复控制信息 |

CF 可以理解为同一 DB 内独立组织的 LSM：有自己的 memtable 和 SST 状态，但共享 DB 的内部 WAL，cache 和后台资源可以配置为共享。因此不为每个索引创建一个 CF，避免固定成本随索引数增长。代价是租户共享 compaction、缓存和 write stall 等影响，CF 不是权限或故障隔离边界。

两个 DB 分离的原因不是 RocksDB 内部 WAL 需要另一个 DB，而是应用恢复需要同时保留“旧的完整索引状态”和“该状态之后的操作日志”。不再采用此前讨论的“单 DB 中最新 postings + 较旧 head snapshot”的恢复路线，避免为此重写完整的状态对齐协议。

建议 key 结构（斜线仅用于展示，实际使用无歧义的版本化编码）：

```text
postings:
  tenant_id / index_id / posting_id

head_snapshots:
  tenant_id / index_id / component / chunk
  tenant_id / index_id / manifest

recovery_log:
  tenant_id / index_id / operation_sequence
```

实际 namespace 是格式版本字节、8 字节 tenant 长度、tenant 原始字节、8 字节 index_id；所有整数顺序字段采用 8 字节 big-endian 编码。Snapshot key 不重复包含 checkpoint_id：一致性边界由整个不可变 State DB checkpoint 提供。在线工作 DB 中可覆盖该索引的 snapshot；旧的完整版本由已发布 checkpoint 保持。

所有 Get、MultiGet、Merge、扫描和范围删除都必须限定命名空间。MultiGet 使用 RocksDB 批量接口；扫描游标属于具体操作，不共享可变 iterator。默认 CF 只因 RocksDB 要求而存在，业务使用上述六个固定 CF。

数据库目录、checkpoint 输出目录和临时文件路径由服务端管理。临时构建文件仍需按索引隔离，但它们不是每个 tenant 独立的数据库。

### 5.2 Head snapshot

Head 仍驻留内存用于查询；RocksDB 保存其可恢复的序列化表示，不把内存图的每次修改都改造成 KV 操作。

现有 BKT/KDT head 的序列化包含 vectors、tree、graph 和 head 删除标记。完整 SPANN 恢复还需要匹配的 version/deletion map、posting sizes、checksums、映射和配置等状态。复用现有文件序列化：写入 index_id 隔离的工作目录，再按 1 MiB 分块保存到 `head_snapshots` CF；恢复时从 CF 还原文件再调用现有 loader。这些临时文件不是权威恢复材料。

`checkpoint_id` 标识恢复点，不表示新的在线索引。分块写入未完成的 snapshot 不得被发布或加载。跨多个 chunk 的完整性通过 manifest/完成协议表达，不要求一个无限大的 WriteBatch。

### 5.3 现有 recovery 基线

当前动态更新路径已经有 checkpoint + 应用日志重放：

- `PersistentBuffer` 用 `PersistentBufferPath/WAL` 下的独立 RocksDB 保存操作。插入记录包含 VID、vector version 和向量，删除记录包含 VID。
- 使用 RocksDB postings 时，在线 DB 位于 `IndexDirectory/KVFile`，恢复模式打开 `PersistentBufferPath/KVFile` 下的 DB checkpoint。应用日志 DB 与 postings DB 不同。
- Head checkpoint 位于 `PersistentBufferPath/HeadIndex`；辅助状态也保存为文件。
- 恢复加载 checkpoint 的 head、postings 和辅助状态，再扫描应用日志，插入重新调用 AddIndex，删除更新 version map，最后等待后台维护完成。
- Postings 可以使用其他 backend，但当前 PersistentBuffer 的日志实现依赖 RocksDB。

实际 checkpoint 调用先执行 extra searcher checkpoint（保存辅助状态、backend checkpoint、清理旧日志），再保存 head。该顺序存在日志过早清理窗口，不能直接作为安全发布协议。

当前 `Checkpoint()` 获取 `m_checkPointLock` 独占锁，而 `AddIndexSPFresh()` 获取共享锁，所以该入口会被阻塞到 checkpoint 完成。通用 AddIndex 和按 VID 删除没有统一使用这把锁；第一次等待后台任务还发生在获取独占锁之前。现有测试流程中也有先等待一批插入/删除完成，再调用 checkpoint 的用法。

因此，现有模型可以复用，但不能据此宣称已有严格恢复保证，或把暂停时间描述为固定的“短暂”。

### 5.4 新设计的恢复契约

已确认：以较小改动沿用“从一致 checkpoint 出发，再重放后续操作”，而不是加载旧 head 后对齐在线 DB 中最新 postings。

故障范围：

- 目标覆盖服务进程异常退出后的恢复，操作系统和底层存储仍正常。
- 已确认成功的更新不能因服务进程崩溃而丢失；对应应用日志写入必须成功，并满足该故障模型下的可恢复要求。
- 不保证机器掉电、操作系统崩溃或存储介质损坏后的数据保留，不额外要求每次写入 `sync=true`。
- 不保证请求未返回成功就一定未执行；超时/重试的去重契约需在 API 设计中明确。
- 不提供用户可选的历史回滚，也不提供单 tenant 独立时间点恢复。

不承诺掉电持久化，不意味着可以忽略日志写入错误、关闭内部 WAL，或提前清理恢复材料。插入/删除先成功写 Recovery DB 应用日志，再同步执行并排空派生维护任务，随后确认成功；之后同一 tenant 的查询可见该更新。未确认操作可能在重启后出现，当前没有请求 ID 去重，调用方不得把未知结果的插入盲目重试理解为 exactly-once。

### 5.5 节点级一致 checkpoint

当前实现按以下顺序执行：

1. 获取节点生命周期共享屏障和更新独占屏障，等待在途更新完成，并阻止新更新与按需加载进入；tenant 集合保持不变，已加载 tenant 的查询可继续。
2. 等待在途更新和其衍生的后台任务完成，再禁止新的维护任务改变状态。不能先关闭任务执行能力而导致排空死锁。查询若会触发维护，也必须受屏障约束。
3. 确定每个索引已纳入该恢复点的连续日志进度，仅为未保存或日志序号变化的 tenant 序列化 head/辅助状态。导入 State DB 时按字节比较并复用未变 chunk，更新变化块、清理过期块并最后写 manifest；未修改和已卸载 tenant 直接复用有效快照，仍记录完整索引清单。这里不是 SPANN 序列化本身的增量重构。
4. 在状态保持一致的情况下创建新的 State DB checkpoint；检查所有序列化、数据库和文件操作结果。
5. 只有整个 checkpoint 完整后，才能在 Recovery DB 用同一 WriteBatch 更新 `recovery_meta/current` 和 `catalog/tenants`；调用 `FlushWAL(false)`，不要求 fsync。
6. 发布成功后按每 tenant 连续进度清理旧日志，然后清理显式登记的旧 checkpoint/工作副本；最后释放屏障。未发布或失败的 checkpoint 不得取代原有效恢复点。

清理阶段仍在更新屏障内，不预先保证更新暂停时长或 shared_mutex 的调度公平性。Create/DeleteTenant 所触发的 checkpoint 持有生命周期独占屏障，会暂停更新及冷加载，但其他已驻留 tenant 的 epoch 查询可继续；Close 则先关闭整个查询入口。暂未增加定时 checkpoint，嵌入调用方应按日志量或周期调用 `Checkpoint()`。析构只排空并释放，不隐式发布 checkpoint，保留异常退出/重放路径。

State DB checkpoint 是节点级，不是每 tenant 一个 DB checkpoint。内部保留旧恢复材料只是为了安全完成协议，不表示支持历史回滚。

### 5.6 进程崩溃恢复

```text
打开 Recovery DB
  -> 选择最近完整发布的节点 checkpoint
  -> 从它建立可写 State DB 工作副本
  -> 逐 tenant 加载 head/辅助状态，重放 checkpoint 之后的应用记录
  -> 排空维护、验证并保存必要的工作快照，然后释放该实例
  -> 所有 tenant 验证通过后恢复节点服务，按请求加载到驻留缓存
```

不要直接修改唯一的已发布 checkpoint。恢复期间再次崩溃时，必须能够重新从完整恢复点开始，不能在部分重放的 postings 上再次盲目追加。恢复结束前保留原 checkpoint 和所需日志；重放不能把相同操作重新追加成新的应用日志。

### 5.7 Recovery log 与生命周期

日志沿用现有 PersistentBuffer 的逻辑操作重放模型，不新增记录每个图节点变化的物理日志系统。实现没有把旧 `PersistentBuffer` 直接套进共享 DB：旧格式、裸整数排序、忽略错误和重放再记日志的行为不适合直接复用。新 manager 使用同一共享存储封装写应用日志，并为它管理的 SPANN 实例关闭旧 WAL；没有同时运行两套应用日志。

Key 包含 tenant/index 身份和连续 operation_sequence。Value 是版本化、长度定界的二进制记录：版本、operation、起始 VID；Insert 再包含 count 和原始向量字节，Delete 不带向量。重放校验序号连续、插入 VID 与当前尾部一致、payload 大小有效；已进入 checkpoint 水位的旧日志跳过。使用原始向量与固定索引配置重做归一化和 AddIndex，不记录物理图结构变化。

从匹配的 head/postings checkpoint 开始，可以沿用重新执行插入并触发维护的思路。仍需验证 split/merge/reassign 的所有持久化影响能够通过该恢复过程重建，不承诺恢复后的图结构逐字节相同。

创建/删除以同步节点 checkpoint 作为提交点，不写额外生命周期日志。成功创建的种子数据、postings、head 和 Catalog 已进入同一 checkpoint；成功删除的 tenant 不在发布清单中。发布前崩溃仍恢复旧清单，发布后崩溃使用新清单。失败/未确认的创建或删除可能已经提交，需在重启后查询状态，而不是假定未执行。暂不提供在线修改数据类型、维度或距离函数。

每索引序号的分配、并发执行与提交顺序必须协调；仅用原子计数器生成 ID 不等于按序提交。Checkpoint 只能清理已被完整覆盖的连续进度，不能按最大见过的序号直接删除。

### 5.8 必要的正确性加固

最小修改不意味着原样保留已知风险，至少需要：

- 新路径传播日志 Put、checkpoint、扫描和清理失败，区分日志末尾与读取错误；不宣称旧单索引 PersistentBuffer 的历史问题已全部修复。
- 使用正确的日志排序及范围删除编码，所有操作限定索引前缀。
- 完整 checkpoint 发布后才清理日志，失败时保留旧恢复路径。
- 覆盖所有更新入口和相关后台任务的节点屏障。
- 处理重放期间再次崩溃、重放重复记日志，以及创建/删除与 checkpoint 的竞态。

两个 DB 不能跨库原子提交，必须明确先可靠记录应用日志、再执行状态变更的顺序及失败处理。以上保证都针对已选择的进程崩溃模型，不扩展为掉电持久化承诺。

## 6. 多实例运行前的关键改造

### 6.1 Workspace 按实例隔离

[`WorkSpace.h`](../AnnService/inc/Core/Common/WorkSpace.h) 的默认 factory 使用：

```cpp
static thread_local std::unique_ptr<WorkSpaceType> m_workspace;
```

这是同一类型、同一线程共享的 workspace，不是某个 index 实例独占的 workspace。

[`ExtraDynamicSearcher.h`](../AnnService/inc/Core/SPANN/ExtraDynamicSearcher.h) 的 `InitWorkSpace` 在首次初始化时会绑定实例分配的 I/O workspace ID 和回收 callback；复用时使用 `Clear` 路径。因此，不能未经处理就假设同一 worker 在线程上交替执行 tenant A、B 是安全的。

managed 路径当前使用 `InstanceThreadLocalWorkSpaceFactory`，不改动 legacy 默认 factory：

- SPANN 及 BKT/KDT head workspace 按运行时 factory 的单调实例 ID 在 TLS 中隔离，不使用可能复用的对象地址或持久化 index_id。
- 同一长驻 worker 复用本线程 workspace，Get/Return 命中不获取共享池 mutex；嵌套借用时保留最多一个空闲 workspace。
- 每线程、每 workspace 类型最多缓存 64 个实例槽位；淘汰旧槽位只丢弃空闲 workspace，不影响已借出的对象。
- factory 只在首次注册槽位时使用锁并保留弱引用。实例卸载先排空请求和关联维护，再销毁 factory，立即释放仍由长驻线程持有的空闲缓冲区；线程退出也会回收自己的槽位。
- 新实例即使来自相同 tenant/index_id，也不会继承旧实例的 I/O ID 或回收 callback。继续复用节点服务线程，不按 tenant 创建请求线程池。

### 6.2 显式关闭与后台任务排空

动态索引存在 split、merge 等后台任务，不能依赖从 registry 删除对象就完成安全卸载。

需要定义显式的关闭契约：

```text
停止接收新请求
  -> 排空在途请求及其衍生任务，阻止新的独立维护任务
  -> 完成所需持久化
  -> 释放索引存储视图与实例资源
  -> 释放实例
```

具体排空顺序需要兼顾请求可能产生后台任务的情况。任务必须持有正确实例的上下文，不能访问已释放的状态。

三种生命周期操作分开处理：

- Unload：释放内存，保留存储数据和有效恢复材料，不关闭共享 DB。
- Delete：独占屏障下排空任务、删除工作 DB 中该索引范围，再用节点 checkpoint 发布删除；发布前旧 checkpoint 和日志保留。发布后才能清理旧恢复材料。范围删除不等于立即回收 SST 物理空间。
- Shutdown node：排空节点所有相关任务，按协议保存必要状态，最后关闭两个共享 DB。

### 6.3 Backend 兼容边界

保留现有 FILEIO/SPDK backend，不把它们的多租户支持纳入本次实现。未来 TiKV 后端继续通过存储抽象接入，但必须重新兑现 batch、追加、重试和恢复语义，不能假设 RocksDB Merge 可以直接替换。Head 序列化存储也不因替换 postings backend 而自动完成适配。

## 7. 资源管理与公平性

下表是后续完整资源治理目标，不是首版已经全部实现的能力。首版实际限额和缺口见第 11.3 节。

当前优先级以第 1.2–1.3 节为准：单节点先做驻留缓存及生命周期并发；tenant 级准入和计算资源调度留到分布式与 compute 扩缩容阶段。

| 资源 | 初步机制 |
|---|---|
| 查询 CPU | 共享 worker pool，tenant 级并发上限、有界队列和公平调度 |
| 内存 | 节点总预算、两个 DB 的共享 cache/memtable 预算，以及 tenant 的 head、辅助状态、workspace 预算 |
| SSD 容量 | tenant 数据用量与节点物理空间预算，计入日志、snapshot、checkpoint 文件保留和临时空间 |
| SSD I/O | 首先限制 tenant I/O 并发；后续增加字节数/IOPS 调度 |
| 更新与维护 | 限制 insert、split/merge/compaction 并发，为查询保留资源 |
| 加载与构建 | 独立限制并发，避免多 tenant 同时操作造成资源峰值 |
| Checkpoint | 控制频率，观测全节点更新暂停时间和日志积压 |

当前 RocksDB backend 为普通 postings DB 配置了 3 GiB 容量的 block cache，不表示启动即占满，但不能原样为每个 tenant 创建一份。改为节点级配置两个 DB 的 cache、write buffers、后台线程及 I/O 预算。SPANN 的百万级 DataBlockSize 等实例固定成本也需要重新评估。

粗略内存模型：

```text
节点内存 =
  所有驻留索引的 head、version、mapping 等状态
  + 两个 DB 的 cache、memtable 和管理开销
  + 活跃 workspace
  + 后台维护和加载峰值
  + 服务自身开销及安全余量
```

容量不能只按 tenant 数估算，也不能只用索引文件大小代替内存使用。

共享 CF 中磁盘、cache 和 compaction 成本不容易精确归属每个 tenant，逻辑字节配额不等于物理占用硬隔离。需要节点总预算和入口限流兜底；一个 tenant 的写入可通过 compaction、WAL 保留或 write stall 影响其他 tenant。

当前先限制在线驻留槽位和每索引线程配置，并提供按工作快照字节计费的缓存目标；不承诺节点 RSS 硬上限。后续再逐步将后台执行改为带 tenant 上下文的共享调度，避免简单合并线程池破坏 workspace 绑定。

当前已实现按需加载和缓存压力驱动的 LRU 淘汰；没有空闲超时扫描。所有候选都有在途请求时，缓存等待 lease 释放；当前没有等待超时、公平性或请求准入保证，也不回退到其他索引。

## 8. 可观测性与验收重点

应能按 tenant 定位：

- 请求量、错误、限流、队列等待、查询延迟。
- 索引状态、加载耗时、恢复失败。
- 驻留资源、cache、workspace、磁盘用量。
- 更新积压、后台维护任务、节点 checkpoint 暂停时长、日志保留量和恢复耗时。

tenant 数量很大时，指标标签需要控制基数；详细租户诊断可以放入日志或按需查询接口。

重点验收场景：

- 两个 tenant 使用重叠的向量 ID，数据仍完全独立。
- 同一 worker 交替查询不同配置的索引，不复用错误的 workspace。
- 并发加载同一 tenant 只创建一个可用实例。
- 查询、iterator、更新和后台任务执行期间的卸载或删除行为符合契约。
- 卸载单个索引不关闭共享 DB，删除只影响指定前缀，重新创建后旧任务不影响新 index_id。
- 正常服务期间单个 tenant 过载不应无限占用资源；首版生命周期操作是全局协调阶段，加载或持久化异常采用节点 fail-closed，而不是承诺 tenant 级故障隔离。
- 在日志写入、状态修改、snapshot 写入、checkpoint 创建、完成标记发布和日志清理各阶段注入进程崩溃，恢复符合已确认写入的契约。
- 恢复中再次崩溃不会导致重复追加；重放不生成重复应用日志。
- 创建、删除及已卸载索引跨 checkpoint 的恢复结果正确，已删除索引不会重新暴露。
- 读取错误和写入失败被显式报告，不作为成功或正常日志末尾处理。

机器掉电和介质损坏不属于本版持久化保证；通过进程崩溃验证不能宣称已经保证掉电恢复。

## 9. 分阶段实施

### 阶段一：共享存储与多实例正确性

- 引入节点级 Storage Engine Manager，复用 KeyValueIO 接入两个共享 RocksDB 和固定 CF。
- 完成命名空间编码、读写、扫描、删除隔离及实例生命周期分离。
- 解决 workspace 的实例隔离。
- 隔离所有存储与临时路径。
- 定义后台任务排空和显式关闭契约。
- 确认多个 SPANN 实例能够通过共享 RocksDB 同时查询和更新。

### 阶段二：节点 checkpoint 与 recovery

- 复用 head/辅助状态序列化，接入 head_snapshots CF。
- 将 PersistentBuffer 演进为共享 Recovery DB 中的应用日志。
- 实现统一更新屏障、checkpoint 完成发布和安全日志清理。
- 完成节点恢复、重复恢复以及生命周期 checkpoint 提交协议；逐步补齐各阶段故障注入验证。
- 未完成正确性加固前，不把原有 recovery 接口当成可交付保证。

### 阶段三：多租户管理与资源治理

- 引入持久化 Catalog、TenantContext 和 Index Manager。
- 增加 tenant_id 路由与索引状态检查，遵循仅接受可信调用方的部署边界。
- 提供必要的租户管理和查询、增删接口。
- 将静态启动配置加载演进为动态生命周期管理，与恢复协议保持一致。
- 加入驻留缓存容量控制；tenant 级有界队列、并发准入与公平调度按 §1.2 延期。
- 限制加载、构建和后台任务资源。
- 增加租户级可观测性。
- 根据实际需求引入按需加载和空闲卸载。

### 后续：多节点与 TiKV

- 增加 `tenant -> worker` placement。
- 首先让一个 tenant 完整落在一个 worker。
- 单租户分片、副本、迁移和高可用另行设计。
- 评估 TiKV backend 的实际操作与恢复语义，不预先实现分布式事务、placement 或故障转移。

现有 Aggregator 偏向分布式检索，不能直接等同于 tenant placement/router。

## 10. 主要取舍与待讨论问题

独立内存索引符合“每个用户自己的索引”，共享 DB/CF 降低存储固定成本，但 head、workspace 和维护资源仍随活跃索引数增长。本版选择节点级 checkpoint/recovery，不以 tenant 独立恢复为目标。

| 选择 | 收益 | 代价 |
|---|---|---|
| 两个共享 DB，而非每 index 两个 DB | DB 数量固定，保留状态与日志分离的恢复模型 | 两库之间需要明确提交顺序 |
| 少量 CF + 前缀，而非每 index 一个 CF | 避免每 CF 的 memtable/LSM 管理成本随租户数增长 | 配置、compaction 和资源干扰更共享 |
| 节点级一致 checkpoint | 接近现有恢复模型，不设计最新 postings 对齐旧 head 的协议 | 全节点更新暂停、节点整体恢复 |
| 不保证掉电持久化 | 暂不增加每次写入同步落盘要求 | 机器掉电或 OS 崩溃可能丢失近期成功写入 |

如果目标是几十到几百个中大型活跃索引，这可以作为合理起点；如果是数十万个、每个用户仅有几百条向量，则需要重新评估 SPANN 的固定成本，考虑小租户使用其他索引类型或不同物理布局。上述数量只是设计场景，不是已测得的容量承诺。

日志格式、生命周期提交、查询暂停和接口选择已有首版实现，见第 11 节。以下问题仍需根据实际工作负载确定：

1. 预计 tenant 总数、同时活跃数，以及单 tenant 数据量分布是什么？
2. Checkpoint 周期、日志量阈值、可接受的全节点暂停和恢复时间目标是什么？
3. 是否需要请求去重、在线管理/写入协议，以及更细粒度的忙/失败响应？
4. 哪些 head/SSD 参数需要开放，是否需要空索引和少于 64 条种子向量的创建方式？
5. 何时引入驻留内存/磁盘预算、公平调度、自动卸载和租户指标？

## 11. 第一版实现与使用

### 11.1 代码入口

| 入口 | 作用 |
|---|---|
| [`SharedRocksDB.h`](../AnnService/inc/Core/SPANN/SharedRocksDB.h)、对应 `.cpp` | 节点级 DB/CF 所有权、namespaced posting view、批量读取、顺序扫描、原子发布和 checkpoint |
| [`TenantIndexManager.h`](../AnnService/inc/Core/SPANN/TenantIndexManager.h)、对应 `.cpp` | Catalog、路由、准入、生命周期、应用日志、节点恢复和清理 |
| [`ServiceContext.cpp`](../AnnService/src/Server/ServiceContext.cpp)、[`SearchService.cpp`](../AnnService/src/Server/SearchService.cpp)、[`SearchExecutor.cpp`](../AnnService/src/Server/SearchExecutor.cpp) | 多租户在线 RPC 和兼容旧 Search 的查询模式 |
| [`ClientInterface.h`](../Wrappers/inc/ClientInterface.h)、[`PythonClient.i`](../Wrappers/inc/PythonClient.i) | 现有 AnnClient 的在线 tenant API 和 Python 结构化结果绑定 |
| [`RemoteSearchQuery.h`](../AnnService/inc/Socket/RemoteSearchQuery.h)、对应 `.cpp` | 版本化 tenant 请求/响应及有界解析；不改变旧 Search 格式 |
| [`TenantTool/main.cpp`](../AnnService/src/TenantTool/main.cpp) | 可信环境的离线创建、插入、查询、删除、列举和 checkpoint 工具 |
| [`MultiTenantTest.cpp`](../Test/src/MultiTenantTest.cpp) | 独立于外部数据集的集成与恢复测试 |
| [`tenant_smoke.py`](../Test/tenant_smoke.py) | 离线 CLI、Python 在线 CRUD、旧 Search 兼容、真实 TCP 错误响应及进程崩溃恢复 |

SPANN 增加共享 posting store 注入、实例 workspace 和维护任务排空接口。现有 `VectorIndex::LoadIndex` 默认行为不变，新重载允许注入 store；保存接口增加显式允许“所有向量已删除”的快照选项，避免正常删除后不能 checkpoint。ThreadPool 排空检查统一观察队列与运行任务，后台异常会被新路径观察到并报告失败。

### 11.2 为减少复杂度采用的具体取舍

| 实现决定 | 原因与影响 |
|---|---|
| 同 tenant 并发读写；VID 预留和日志提交使用 tenant 内短临界区 | 没有跨 tenant VID 协调；维护任务通过请求上下文完成确认，不等待其他请求的全部维护 |
| 私有构建、目标租户卸载与全局提交分离 | 创建构建和卸载不暂停其他 tenant；创建发布、删除及关闭仍全局协调；独立 checkpoint 允许驻留查询 |
| LRU 实例缓存与 lease | 压力下自动淘汰空闲实例，dirty eviction 保留日志；恢复逐个验证后释放，不一次性驻留所有 tenant |
| 仅重写 dirty tenant 的工作快照 | 用 sequence/savedSequence/hasSnapshot 判断，不扫描所有向量；节点 checkpoint 仍包含完整恢复状态 |
| 创建/删除必须同步发布 checkpoint | 免去生命周期 WAL 和首次构建输入保留协议；代价是管理操作较重 |
| Catalog 与 current 指针一起发布 | State checkpoint 清单与 Recovery Catalog 必须一致；Catalog 不领先于已确认 checkpoint |
| Head 通过既有文件格式分块进入 CF | 避免重写图索引序列化；额外占用临时磁盘及复制时间 |
| 恢复创建新的可写 State 副本 | 不修改唯一恢复点；重复崩溃后可以从原 checkpoint 重新重放 |
| 仅在发布/恢复成功后清理登记的旧目录 | 正常情况下保留一个发布 checkpoint 和一个工作副本；失败遗留目录在下次成功恢复后回收 |
| 发布失败或状态修改失败使节点 fail-closed | 不继续在不确定状态上服务，需要销毁 manager 后重新 Open；不承诺租户级故障隔离 |

Snapshot 旧 chunk 会在写入新快照前从**工作 DB**清除，不破坏已发布 checkpoint。删除租户的遗留应用日志会在成功恢复/发布后清理。目录清理只处理 Recovery DB 显式登记的 artifact，不按名字扫描删除 root 下的任意内容。首次 Open 要求空的专用目录；不要把已有单索引目录作为这个 root。

### 11.3 支持范围与已知限制

- 一个 tenant 一个 SPANN 索引；head 沿用原默认值，也可显式选择 BKT/KDT。每次删除后重建分配新 index_id，没有 generation。
- 创建需要至少 **64 条种子向量**，不能创建空索引。沿用 `ExcludeHead=true`：部分原始向量仅保存在 head 中，其余保存在 postings；可以把已有索引内全部向量删除并正常 checkpoint/reload。
- 数据类型支持现有 `Int8/UInt8/Int16/Float`，距离支持 L2/Cosine。维度、类型、距离在创建后固定。Cosine 查询在内部副本归一化，不修改调用方输入。
- 默认最多 64 个 tenant、每 tenant 1,000,000 个已分配 VID、维度 4096、top-k 1000。VID 不回收，删除不会腾出这个计数配额；这些是容量边界，不是性能承诺。
- 两 DB 共用默认 **3 GiB** block cache，与原 private posting DB 的容量一致，但不是每个 tenant 各配 3 GiB，也不是节点 RSS 上限。posting CF 的 blob、Bloom、compaction 和 write-buffer 配置对齐原 `RocksDBIO`；Recovery 使用原 WAL-DB 默认配置，新 snapshot/checkpoint CF 保留 4 MiB buffer，详见 §16。
- 除调用方显式选择 head、容量与恢复协议必要设置外，继承原 SPANN 及相应 head 的构建、查询和维护线程默认值，维护线程仍立即启动。关闭 query-triggered merge、旧应用 WAL 和独立异步 append queue，写入在确认前等待本请求派生的维护；不关闭 RocksDB 自身 WAL。没有公平调度器或硬磁盘/内存配额。
- 默认最多 8 个在线驻留/加载槽位，支持可选快照字节缓存目标；自动淘汰空闲 LRU 实例，并支持内部显式 `Unload`。重启逐 tenant 验证恢复，服务开始时实例均未驻留。没有空闲超时扫描、iterator 或跨租户搜索。
- 没有定时 checkpoint。应用持续写入时必须主动 checkpoint，否则 Recovery DB 日志持续增长。
- 没有 online rebuild、跨节点 placement、TiKV、权限系统或请求去重。
- 现有 C++ / Python 客户端支持五种在线操作：创建 tenant、删除 tenant、插入向量、删除向量、查询。在线列举、Describe、Unload 和 checkpoint RPC 不在此次接口范围；这些操作仍使用内部 Manager 或离线工具。离线 CLI 与 server 不能同时打开同一 root，Recovery DB 的 LOCK 排除第二个 owner。
- 新 tenant 协议的单条请求/响应 body 上限为 64 MiB（包含头部、tenant ID 和向量 payload）；超过上限的客户端输入被拒绝，收到超限包头时连接会关闭。大批量 Insert 应拆批；Create 先用不超过上限且至少 64 条种子向量建立索引。该上限不是节点总内存或队列预算。
- 既有 socket 连接层仍有历史限制：验收时观察到客户端在 accept 完成前立即断开，会触发 `Connection::Start` 的 `remote_endpoint` 异常并退出进程。本次没有扩展修改这部分通用连接代码；上线前应另行修复，不能把当前 server 当作已完成网络健壮性加固。
- 进程崩溃恢复以 OS/存储正常为前提；不保证掉电、OS 崩溃和介质故障。当前实现验证环境是 Linux。

### 11.4 构建与测试

使用项目现有 RocksDB、Boost、TBB、zstd 等依赖：

```bash
cmake -S . -B build-multitenant \
  -DROCKSDB=ON -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON
cmake --build build-multitenant --target SPTAGTenantTest tenanttool server _SPTAGClient -j 2
OMP_NUM_THREADS=2 ctest --test-dir build-multitenant -R '^MultiTenant$' --output-on-failure
python Test/tenant_smoke.py --bin-dir Release
```

运行 Python smoke 的解释器须与 CMake 找到的 Python 开发库版本一致。测试使用 bytes，不要求 NumPy。

若本机 Conda 被 CMake 自动选中，需确保其 libstdc++ 与系统 RocksDB 兼容；也可显式设置匹配的系统 `Boost_DIR`。不要全局替换机器的运行库。

本次验证结果：Release 的 22 个 MultiTenant 用例通过；Debug AddressSanitizer（`detect_leaks=1`）测试通过；ROCKSDB=OFF 的 server/client 与测试目标构建通过，既有 `AlgoTest` 的 BKT/KDT/SPANN 三个用例通过；CLI + Python 真实 TCP CRUD、同客户端并发/GIL、错误响应和崩溃恢复 smoke test 通过。测试时只对进程设置 `LD_PRELOAD` 解决本机 Conda/系统 libstdc++ 混用，ASAN 进程同时预加载 libasan 并设置 8 MiB stack；未修改机器全局配置。

在线 API 的 smoke 还覆盖 Python 客户端五种操作、旧 Search 兼容、错误类型/维度拒绝、超时后迟到响应、断连与不支持协议的响应，以及未 checkpoint 的在线写入在进程被杀后恢复。若 Python 动态加载系统 RocksDB 时报告 jemalloc 的 static TLS 错误，可仅对测试进程预加载与链接环境匹配的 `libjemalloc.so.2`，不要更换全局库。

测试覆盖 namespace/range/MultiGet 隔离、同线程跨租户查询、不同维度/类型、并发插入触发维护、卸载重载、删除重建、全部向量删除、容量检查、目录保留数量、服务端路由拒绝路径、未 checkpoint 的插入/删除后子进程 `_exit`、重复恢复、已发布水位跳过旧日志以及损坏日志拒绝启动。所有数据使用测试独占的临时目录，不读写真实索引。

新增缓存验收覆盖 LRU 顺序、dirty eviction 与重放、单索引超字节目标、并发冷加载合并、单槽位下跨 tenant 并发更新与 checkpoint、工作文件复用、加载失败回收容量并唤醒等待者、淘汰保存失败后日志恢复，以及阻塞创建/卸载保存时其他 tenant 的查询和插入继续。缓存字节计费与驻留数量的这些验证不等于验证了 RSS 硬隔离或调度公平性。

**验证范围不等于穷尽故障证明**：尚未覆盖每一条 RocksDB/文件系统调用之间的故障注入、长时间多租户压测、磁盘满、所有平台，以及掉电测试。生产容量、tail latency、checkpoint 暂停和大规模恢复性能仍需专门评估。

### 11.5 离线管理与服务端配置

`tenanttool` 接受现有 packed-vector 文件布局：两个 native int32（count、dimension），随后是对应类型的连续向量。查询文件 count 必须为 1，文件大小必须精确匹配 header。

```bash
# ROOT 必须是新的空目录，或本工具创建的节点目录。
Release/tenanttool /path/to/tenant-node create alice Float L2 /path/to/seeds.bin
Release/tenanttool /path/to/tenant-node insert alice /path/to/additions.bin
Release/tenanttool /path/to/tenant-node search alice /path/to/query.bin 10
Release/tenanttool /path/to/tenant-node delete-vector alice 128
Release/tenanttool /path/to/tenant-node list
Release/tenanttool /path/to/tenant-node checkpoint
# 删除整个租户是显式管理操作：
# Release/tenanttool /path/to/tenant-node delete-tenant alice
```

离线命令成功结束前调用 `Close()` 发布 checkpoint。持续在线写入应嵌入同一个长生命周期 `TenantIndexManager`，而不是每条写入启动一次 CLI。

示例 server 配置（只对可信本机调用方开放）：

```ini
[Service]
ListenAddr=127.0.0.1
ListenPort=8000
ThreadNumber=8
SocketThreadNumber=2

[MultiTenant]
Root=/path/to/tenant-node
MaxTenants=64
MaxVectorsPerTenant=1000000
MaxDimension=4096
MaxResults=1000
CacheBytes=3221225472
MaxResidentTenants=8
MaxResidentBytes=0
```

启动：`Release/server -m socket -c /path/to/server.ini`。

通过现有 socket RemoteQuery 的 query string 查询，例如四维 Float tenant：

```text
$tenantid:alice $resultnum:10 0.1|0.2|0.3|0.4
```

返回的 indexName 是 tenant ID。缺少 tenant、未知 tenant、维度不匹配、超出 top-k、混用 indexname、重复 tenantid 或要求 metadata 时明确失败，不回退到默认索引。此模式没有认证/授权，不应直接暴露给不可信用户。

## 12. 用户接口契约

`TenantIndexManager` 成员函数是服务端内部接口。用户通过现有 `AnnClient` 的新增方法远程调用，不获取或直接操作 Manager。系统仍然是一个 user 对应一个 tenant、一个 tenant 只有一个索引，不涉及账号注册。

### 12.0 在线用户访问层：现有 AnnClient 扩展

目标调用关系是：

```text
用户应用
  -> SPTAGClient.AnnClient / C++ AnnClient
  -> TenantRequest / TenantResponse socket 协议
  -> 服务端共享的 TenantIndexManager
  -> tenant_id 对应的唯一 SPANN 索引
```

Python 绑定仍由 SWIG 从 C++ 客户端生成，不是另一套纯 Python 网络实现。新方法如下；C++ 方法同名，返回 `std::shared_ptr<RemoteTenantResult>`，Python 返回 dict：

| Python 方法 | 行为 |
|---|---|
| `CreateTenant(tenant, vectors, count, dimension, value_type, metric, headIndexAlgo=None)` | 在线创建唯一索引；种子数至少 64；类型如 `"Float"`，距离如 `"L2"` / `"Cosine"`；最后一项可选 `"BKT"` / `"KDT"`，省略、None 或空字符串沿用原 SPANN 默认值 |
| `DeleteTenant(tenant)` | 在线删除整个 tenant 索引 |
| `InsertTenant(tenant, vectors, count, value_type)` | 插入一批向量；根据 payload 字节数、count 和类型推导维度，服务端在生命周期屏障内检查类型和维度 |
| `DeleteTenantVector(tenant, vector_id)` | 删除该 tenant 内的一个 VID |
| `SearchTenant(tenant, query, k, value_type)` | 单条查询；根据 payload 和类型推导维度；返回 k 个结果槽位 |

`vectors/query` 使用连续的 packed payload（bytes 或连续 NumPy 数组），不带文件 header；客户端不会自动转换 dtype，必须与 `value_type` 一致。所有新方法显式携带 tenant，不读取旧 `SetSearchParam` 状态；tenant 使用长度编码，可包含旧查询字符串不能表示的分隔符。客户端连接后再发请求。

所有新方法的 Python 结果具有相同字段：

```python
{
    "status": 0,                 # SPTAG ErrorCode 数值，0 表示成功
    "status_name": "Success",    # ErrorCode 名称
    "first_id": -1,              # 仅成功 Insert 返回首个新 VID
    "count": 0,                  # Insert 的新增数量，或 Search 的结果槽位数
    "ids": [],                  # 仅 Search 填充；-1 表示无有效候选
    "distances": [],
}
```

失败时仅使用 `status/status_name`。服务端直接返回 Manager 的错误，如 `Key_NotFound`、`VectorNotFound`、`FailedParseValue`、`ExternalAbort`。客户端还可返回 `Socket_FailedNetwork`、`Socket_Timeout`、`Socket_InvalidMessage`、`Socket_UnsupportedRequest`。旧 server 不支持 tenant RPC 时不会回退到其他索引。

超时由既有 `SetTimeoutMilliseconds` 控制；新 RPC 在非正超时配置下使用 9000 ms。超时不取消服务端执行，客户端不自动重发写入。断连/超时的写入结果可能未知，尤其不能盲目重试 Insert；没有 exactly-once 或 request_id 去重。首次建索引可能较慢，应显式增加超时。

```python
import time
import numpy as np
import SPTAGClient

client = SPTAGClient.AnnClient("127.0.0.1", "8000")
deadline = time.monotonic() + 10
while not client.IsConnected():
    if time.monotonic() >= deadline:
        raise RuntimeError("Connection timed out")
    time.sleep(0.05)
client.SetTimeoutMilliseconds(60000)

def checked(result):
    if result["status"] != 0:
        raise RuntimeError(result["status_name"])
    return result

seed = np.arange(128 * 4, dtype=np.float32).reshape(128, 4)
checked(client.CreateTenant("alice", seed, 128, 4, "Float", "L2"))
added = np.array([[0.1, 0.2, 0.3, 0.4]], dtype=np.float32)
inserted = checked(client.InsertTenant("alice", added, 1, "Float"))
found = checked(client.SearchTenant("alice", added[0], 10, "Float"))
checked(client.DeleteTenantVector("alice", inserted["first_id"]))
checked(client.DeleteTenant("alice"))
```

旧 `Search` 的签名和四元组返回值不变，仍可使用 `SetSearchParam("tenantid", "alice")` 查询。新方法不提供 metadata、iterator、原地更新或业务 document ID 映射。`Open`、`Checkpoint`、`Close` 仍属于服务端节点管理，不向普通用户暴露。

协议使用新 packet type `0x04/0x84`，body 带独立版本号 1，并严格检查字段长度、维度/类型与总长度。和现有 socket 协议一样使用 native-endian 标量与向量 payload，要求两端兼容；不改变旧 Search 的 wire format。新 RPC 响应回到请求实际来源连接，不采用请求指定的转发连接。

### 12.1 三种接入方式与能力边界

| 接入方式 | 适用调用方 | 当前提供的能力 | 不提供的能力 |
|---|---|---|---|
| C++ `SPTAG::SPANN::TenantIndexManager` | 嵌入 SPTAG 的业务服务 | 创建/删除 tenant 索引、增删向量、查询、列举、读取类型/维度、卸载、checkpoint、关闭 | 账号/权限、iterator、按 VID 读取向量、原地更新向量、请求去重 |
| `AnnClient` / `SPTAGClient.AnnClient` + socket Server | 可信远程调用方 | 创建/删除 tenant、插入/删除向量、近似 top-k 查询 | 远程 List/Describe/Unload/checkpoint、账号/权限、请求去重 |
| `tenanttool` CLI | 离线运维和功能验证 | 创建、插入、查询、删除向量、删除 tenant、列举、checkpoint | 长驻在线管理、与 server 同时操作相同 root、单独的 unload/describe 命令 |

同一个 root 同时只允许一个 manager owner。在线查询和更新都由持有该 manager 的 server 处理，不能为每个请求重新 Open，也不能通过另一个 CLI 进程修改正在服务的节点。

### 12.2 标识和数据约定

| 字段 | 调用方契约 |
|---|---|
| `tenant_id` / API 中的 `tenant` | 调用方提供的、区分大小写的字符串；C++ 创建接口接受 1–1024 字节。为兼容 socket 文本选项和 CLI，建议统一使用字母、数字、`-`、`_` 组成的 ID，不使用空白或分隔符 |
| `index_id` | 系统内部分配且不复用；调用方不传入，也不依赖它进行路由。删除重建同名 tenant 后，其 index_id 改变 |
| `VID` / `id` | 索引内的向量 ID，类型为 `SizeType`。创建的种子向量按输入顺序分配 `0..count-1`；插入返回新分配的连续区间。不同 tenant 可以有相同 VID，因此业务引用必须包含 tenant_id |
| `type` | `VectorValueType::Int8/UInt8/Int16/Float`，创建后不可修改 |
| `dimension` | 每条向量元素数，必须为正且不超过节点 `maxDimension`，创建后不可修改 |
| `metric` | `DistCalcMethod::L2` 或 `Cosine`，创建后不可修改 |
| C++ `ByteArray` | **只有向量 payload，不含文件 header**；按向量连续排列，长度精确等于 `count * dimension * GetValueTypeSize(type)`。Float 元素必须是有限值 |
| `count` | Create 的种子数至少 64；Insert 至少 1，且不得超过已分配 VID 容量上限 |
| `k` | `1..maxResults`；不是跨 tenant 的总结果数 |

输入缓冲区在同步调用返回之前必须有效，且不能被其他线程修改。manager 不接管调用方缓冲区，也不会为了 Cosine 归一化而修改它。Insert/Search 的 payload 必须按该 tenant 的类型编码；它们不是自动类型转换接口。

VID 删除后不回收、不复用；删除整个 tenant 后重新创建则从新索引的 VID 0 开始。系统不存储业务自定义 document ID，也没有业务 ID 到 VID 的映射接口，应用需要自行维护。

### 12.3 服务端内部 C++ API：节点管理

头文件：[`inc/Core/SPANN/TenantIndexManager.h`](../AnnService/inc/Core/SPANN/TenantIndexManager.h)，需要 ROCKSDB 构建。

```cpp
static ErrorCode Open(const std::string& root, const Limits& limits,
                      std::unique_ptr<TenantIndexManager>& result);
ErrorCode Checkpoint();
ErrorCode Close();
```

| 接口 | 输入与成功结果 | 持久化/生命周期语义 |
|---|---|---|
| `Open` | `root` 为专用节点目录；`limits` 为节点限额；成功后 `result` 持有可用 manager，失败时为空 | 新节点要求空目录；已有节点逐 tenant 验证 checkpoint、重放日志、保存必要工作快照并释放实例，全部成功后返回，随后按需加载。不能把旧单索引目录当作此格式打开 |
| `Checkpoint` | 无 tenant 参数，返回 `ErrorCode` | 覆盖整个节点；排除更新和按需加载、排空维护，仅保存 dirty tenant；已加载查询可继续，随后发布完整恢复点及清理旧材料 |
| `Close` | 返回关闭过程的 `ErrorCode` | 尝试 checkpoint，随后释放索引和 DB；即使返回失败也会关闭该 manager。重复 Close 返回 Success，但不能据此抹去首次关闭失败 |

`Limits` 当前是**节点统一配置**，不是每个 tenant 可单独指定的 quota：

| 成员 | 默认值 | 含义 |
|---|---|---|
| `maxTenants` | 64 | 节点中登记的 tenant 数量上限，包括已卸载的 tenant |
| `maxVectorsPerTenant` | 1,000,000 | 每个 tenant 已分配 VID 数量上限；删除向量不释放此额度 |
| `maxDimension` | 4096 | 创建时允许的最大维度 |
| `maxResults` | 1000 | 单次查询的最大 k |
| `cacheBytes` | 3,221,225,472 | 两个 DB 共用的 block cache 容量，与原 private posting DB 默认容量一致；不是进程总内存上限 |
| `maxResidentTenants` | 8 | 在线驻留及加载/淘汰保留的槽位数；须大于 0。恢复或私有构建的临时实例见 §1.4 |
| `maxResidentBytes` | 0 | 缓存工作快照字节计费目标，0 表示关闭；不是 RSS 上限，允许单个超大 tenant 独占 |

Open 不修改已有索引的数据配置；不要把更换 limits 当成索引缩容或重建。析构会排空并释放资源，但**不等同于成功 Close**，不会隐式发布新 checkpoint。调用方应显式检查 Close 的返回值。

### 12.4 服务端内部 C++ API：tenant 生命周期与数据操作

下表参数顺序与当前头文件一致；除 `List` 外，接口返回 `ErrorCode`。

| 接口 | 输入 | 成功输出及效果 |
|---|---|---|
| `Create(tenant, type, dimension, metric, vectors, count, headIndexAlgo=IndexAlgoType::Undefined, postingLayers=0)` | tenant ID、固定索引配置和至少 64 条种子向量；可选 head 为 BKT/KDT；head Undefined、postingLayers 0 表示不覆盖原默认值 | 构建索引并完成节点 checkpoint 后返回 Success；已存在的 tenant 不覆盖、不重建，head 类型与层数随原配置持久化 |
| `Insert(tenant, vectors, count, firstID, expectedType, expectedDimension)` | 目标 tenant、按其类型/维度排列的新向量；后两项可选 | `firstID` 输出首个 VID；本批 VID 为 `[firstID, firstID + count)`。日志写入和实际更新及派生维护完成后返回 Success |
| `DeleteVector(tenant, id)` | 目标 tenant 内有效 VID | 逻辑删除该向量；后续查询不再返回它。不删除整个 tenant，也不保证立即回收磁盘空间 |
| `Search(tenant, query, k, results, expectedType, expectedDimension)` | 单条向量 payload、k；后两项可选 | `results` 被替换为 k 个 `BasicResult` 槽位，每个含 `VID` 和 `Dist`，按距离排序、越小越近。无有效候选的槽位 VID 为 -1，应跳过；近似搜索不保证精确最近邻 |
| `DeleteTenant(tenant)` | 目标 tenant | 删除其索引并完成节点 checkpoint 后返回 Success。其他 tenant 不被删除，但操作期间会等待节点屏障 |
| `Unload(tenant)` | 目标 tenant | 保存当前可恢复状态并释放该索引的内存；保留 Catalog 和数据。后续 Search/Insert/DeleteVector 会按需加载 |
| `Describe(tenant, type, dimension)` | 目标 tenant | 输出该 tenant 的数据类型和维度；不返回 index 指针、metric、运行状态或资源使用量 |
| `List()` | 无参数 | 返回已登记的 tenant ID 列表，包括已卸载索引；不是状态/健康检查接口 |
| `GetStats(node, tenants)` | `NodeStats` 与 `vector<TenantStats>` 输出引用；返回 void | 管理员内部诊断，关闭或 failed 后仍可读取；不触发加载，不发布 checkpoint |

补充约束：

- `firstID`、Search 的 `results`、Describe 的输出参数只在返回 Success 后使用；失败时不能把残留输出当作本次结果。
- `expectedType` 默认 Undefined，`expectedDimension` 默认 0，表示不额外检查。在线 Insert/Search 显式传入两者，并在节点生命周期共享屏障内检查，避免 Describe 与执行之间重建索引导致类型/维度误解码。它们不提供 index_id 版本绑定，同配置删除重建仍按当前 tenant 路由。
- `Unload` 不单独发布节点 checkpoint，异常退出仍从最近发布的恢复点加日志恢复；卸载状态不持久化为重启后的驻留策略。
- 重复 DeleteVector 返回 `VectorNotFound`；对不存在的 tenant 执行 DeleteTenant 返回 `Key_NotFound`，当前没有“存在或不存在都成功”的删除契约。
- List 没有 ErrorCode，也不检查节点是否 failed；Close 后列表会为空。不能用“能 List”推断节点可以接受数据操作。
- `GetStats` 包含节点 failed/closed、槽位及计费容量、是否超字节目标，以及每 tenant 的 `Unloaded/Loading/Resident/Evicting`、activeRequests、cacheHits/cacheMisses、loads/evictions、最近加载/淘汰耗时和 dirty。dirty 指工作快照是否落后于日志，不表示已发布 checkpoint 是否覆盖全部写入。指标重启清零；并发变化时节点与各 tenant 数据不是同一时刻的全局事务快照。没有在线管理员 RPC；嵌入服务可调用此接口接入其管理员观测系统。
- `activeRequests` 是已取得 lease 的请求数，不包含共享服务队列或等待缓存空间的请求；loads/evictions 计在线缓存操作，启动验证加载不计入 loads。`residentBytes`/`snapshotBytes` 是工作快照计费字节，不是 RSS 测量。

### 12.5 并发、完成确认与错误处理

**当前实现**支持同一 tenant 的并发查询、并发插入及读写重叠。每 tenant 的 submitMutex 只协调 VID 预留、应用日志追加和删除；posting 更新、head 搜索和派生维护在锁外执行。不同 tenant 的 VID 和提交锁独立。重复删除串行检查，恰有一次成功，之后返回 `VectorNotFound`；删除尚在插入中的 VID 返回 `ExternalAbort`。

Create 的构建阶段允许其他 tenant 读写，发布阶段才持有节点生命周期独占屏障；DeleteTenant/Close 仍使用独占屏障。Unload 阻止目标实例的新 lease 并等待已有 lease，不暂停其他 tenant。Checkpoint 排除更新、按需加载和淘汰，但不排除已驻留 tenant 的查询。Manager 没有请求级超时或取消参数；客户端超时不取消服务端执行。五个 Python tenant API 释放 GIL，允许同一 AnnClient 的多个线程并发调用；调用期间不可修改传入的向量缓冲区。

并发查询没有批次事务快照保证，可以看到正在进行的部分插入/删除。`sequence` 仍是 tenant 内普通计数器，由提交锁保护，不是全局原子计数器。提交顺序和完成顺序可以不同；只有排空更新与维护后，checkpoint 才发布完整水位。

成功插入/删除向量在返回前已执行完成，之后的查询可见其效果，且可通过应用日志应对进程崩溃；这不要求每次更新都 checkpoint，也不表示保证掉电持久化。失败、断连或超时的操作可能已部分执行，或已提交但未送达响应。当前没有 request_id 或 exactly-once 去重，不应盲目重试结果未知的 Insert。

| C++ 返回值 | 常见含义 | 调用方处理 |
|---|---|---|
| `Success` | 本次操作成功 | 使用本次输出 |
| `ExternalAbort` | 待删除 VID 所属插入尚未完成，本次未删除 | 等对应插入结果后有界重试；不是普通并发查询/插入竞争 |
| `Key_NotFound` | 请求指向的 tenant 不存在；底层必需数据缺失也可能传播此码 | 结合操作和日志判断，不能自动创建或回退到其他 tenant |
| `VectorNotFound` | 待删除 VID 不存在或已删除 | 检查 tenant/VID 或由业务自行定义幂等删除处理 |
| `FailedParseValue` | 输入类型、维度/长度、k 等非法；Create 重名或超过 tenant 数也使用此码 | 修正请求，不把它当成临时 busy |
| `MemoryOverFlow` | Insert 超过已分配 VID 容量 | 不立即重试；删除向量不会释放该配额 |
| `DiskIOFail`、文件错误或其他非 Success | 存储、加载、checkpoint 等失败 | 记录错误并检查节点状态；状态修改/恢复材料保存失败可能已将节点置为 failed |
| `Fail` | 包括节点已关闭/failed，以及部分恢复一致性检查失败 | 查日志；failed manager 需销毁后重新 Open 恢复，不能继续发送写入 |

这些是当前 ErrorCode，不是完善的业务错误分类；并非所有内部错误都可由单个返回码精确区分。对已进入 failed 状态的节点，应停止正常服务并由节点管理层恢复，不能自动重放调用方所有未确认请求。

### 12.6 旧 Socket Search 查询接口（兼容保留）

启用方式和配置见第 11.5 节。接口沿用现有 **SPTAG socket 二进制协议**：完成连接注册后发送 `RemoteQuery`，其 `m_queryString` 包含查询文本。下面的字符串不是 HTTP URL，也不能直接用裸 TCP 文本替代协议封包。

```text
$tenantid:alice $resultnum:3 0.1|0.2|0.3|0.4
```

| 请求项 | 约定 |
|---|---|
| `$tenantid` | 必须显式指定一个 tenant，不能重复指定；没有默认 tenant 或跨 tenant fan-out |
| `$resultnum` | 可选，默认使用 `[QueryConfig] DefaultMaxResultNumber`，未配置时为 10；仍必须满足节点 MaxResults |
| 向量文本 | 元素按目标 tenant 的类型解析；默认以 `|` 分隔，元素数必须匹配维度 |
| Base64 向量 | 也可使用既有 `#<base64>` 形式；解码结果直接按 tenant 类型解释，必须是单条正确长度的 payload |
| `$datatype` | 不是修改 tenant 类型的接口；当前向量解析以 Catalog 中的目标类型为准，不应依赖此选项进行转换 |
| `$indexname` / `$extractmetadata:true` | 不支持与此模式组合 |

成功响应为 `SearchResponse`：packet 的 `PacketProcessStatus::Ok`，body 是 `RemoteSearchResult`，其状态为 Success，包含一个 `IndexSearchResult`；`m_indexName` 等于 tenant ID，`m_results` 包含 VID/Dist。无有效候选的槽位 VID 为 -1。

旧 Search 查询执行失败时返回 packet `PacketProcessStatus::Failed`、空 body，详细原因记录在服务端日志中；不会逐项传递 C++ ErrorCode。需要区分 busy、参数错误和未知 tenant 的客户端应使用 §12.0 的 `SearchTenant`，它返回结构化错误；旧 Search 行为保持不变。

### 12.7 离线 CLI 接口

统一形式：`tenanttool ROOT COMMAND ...`。CLI 使用默认 Limits，没有逐条命令设置 quota 的选项。

| 命令及位置参数 | 输出/效果 |
|---|---|
| `create TENANT TYPE METRIC VECTORS.bin` | 创建并 checkpoint；TYPE 如 Float，METRIC 如 L2 |
| `insert TENANT VECTORS.bin` | 插入后输出 `first_id=<首个VID>` |
| `search TENANT QUERY.bin K` | 每个结果槽位输出一行 `<VID> <Dist>`；可能有 VID=-1 的槽位 |
| `delete-vector TENANT VID` | 删除一条向量 |
| `delete-tenant TENANT` | 删除该 tenant 的整个索引 |
| `list` | 每行一个 tenant ID |
| `checkpoint` | 发布节点 checkpoint |
| `stats` | 离线节点/租户状态；该命令新开 manager，不能查看正在运行的 server 的实时计数 |

文件格式为 native int32 的 `count`、`dimension` 加连续向量 payload，区别于 C++ ByteArray 不带 header 的约定。种子文件 count 至少 64，查询文件 count 必须为 1；文件大小必须精确匹配 header 和数据类型。

退出码：`0` 表示命令及最后的 Close 成功；`1` 表示节点打开、操作或关闭失败；`2` 表示命令/参数解析或输入文件校验失败。不能用是否打印 `first_id` 判断整个命令成功，因为它之后的 Close 仍可能失败。CLI 输出还可能混有库日志，目前不是稳定的 JSON/机器可读输出协议。

### 12.8 服务端内部集成流程

嵌入业务服务：

```text
进程启动：Open(root, limits)
  -> 管理请求：Create(tenant, type, dim, metric, seeds, count)
  -> 数据请求：Insert / Search / DeleteVector，始终传入 tenant
  -> 按周期或日志量：Checkpoint()
  -> 按需释放内存：Unload(tenant)
  -> 进程正常退出：检查 Close() 返回值
```

最小 C++ 示例：在专用节点中创建一个新的四维 Float tenant，然后查询。`seeds` 必须恰好包含 64 条向量，`query` 包含一条；已有 tenant 的业务路径不应重复调用 Create。

```cpp
#include "inc/Core/SPANN/TenantIndexManager.h"

SPTAG::ErrorCode CreateAndQuery(
    const std::string& root,
    const SPTAG::ByteArray& seeds,
    const SPTAG::ByteArray& query,
    std::vector<SPTAG::BasicResult>& results)
{
    using namespace SPTAG;
    std::unique_ptr<SPANN::TenantIndexManager> node;
    auto status = SPANN::TenantIndexManager::Open(root, {}, node);
    if (status != ErrorCode::Success) return status;

    status = node->Create("alice", VectorValueType::Float, 4,
                          DistCalcMethod::L2, seeds, 64);
    if (status != ErrorCode::Success) return status;

    status = node->Search("alice", query, 3, results, VectorValueType::Float);
    if (status != ErrorCode::Success) return status;
    return node->Close();
}
```

使用现有 server 时，可直接配置空的专用 Root 并启动，通过 §12.0 的客户端 API 在线创建、导入、查询和删除。离线 CLI 仅在停服时使用，不得绕过 owner 锁打开正在服务的 RocksDB。

## 13. 四租户 serving 性能实验

脚本：[`Test/tenant_benchmark.py`](../Test/tenant_benchmark.py)。本轮使用用户确认的合成数据基线，不替代真实业务数据上的性能/质量验收。

### 13.1 可复现方法

- 四 tenant 分别 10,000 / 50,000 / 100,000 / 200,000 条 128D Float，L2、top-k=10。各 tenant 使用独立标准正态向量和独立查询，不用库内向量做 self-query。固定 seed=20261102；每 tenant 1,024 条查询，分块 float64 brute-force 计算精确 top-10。
- 保持当前固定索引参数：SelectHead.Ratio=0.2、ReplicaCount=2、SearchInternalResultNum=32、MaxCheck=256；不为得到更高 QPS 而单独降低某个版本的搜索质量。
- localhost 真实 TCP 与现有 Python AnnClient；每个客户端 worker 使用独立持久连接、同步 closed-loop 请求。各 worker 轮流访问四 tenant，查询随机有放回抽样。因此各 tenant 请求量接近相等，**不是分别测得的单 tenant 饱和 QPS**；不同大小的代价体现于分租户延迟及整体吞吐。
- 默认总并发 1/4/8/16，每组预热 5 秒、测量 30 秒，重复 3 次，并交替并发度顺序。报告成功完成 QPS、p50/p95/p99、错误率、每 tenant recall，以及 server/client CPU 使用与 server RSS。QPS 只计时间窗内成功完成的请求；延迟含少量窗口结束后完成的请求、排除失败请求，没有做 coordinated-omission 校正。
- client/server 共用最多 8 个 CPU 的亲和性集合，服务 worker=8、socket worker=1；不是为 server 单独保留 8 核。运行于共享机器，不宣称独占硬件、冷磁盘或生产容量保证。OS page cache 不清空。
- 基线与新版本复用同一已构建的数据和索引，排除重新构图的随机性。全驻留（8 槽位）与缓存压力（2 槽位）分开报告；后者是四 tenant 轮转造成的容量不足场景，不是全驻留搜索性能回退。
- 创建由离线 tenanttool 完成并 checkpoint，不计入 serving QPS。测量阶段没有更新或定时 checkpoint。原 Socket 正常退出路径存在已记录的 endpoint 异常，脚本在只读测量完成后主动终止测试 server，不以该实验验证 graceful shutdown，也没有修改公共连接代码。

示例：

```bash
# 首次生成数据、计算 ground truth 并创建索引；目录必须不存在。
python Test/tenant_benchmark.py --bin-dir Release --output /dedicated/tenant-benchmark

# 复用完全相同的数据/索引；分别保留运行结果。
python Test/tenant_benchmark.py --bin-dir Release --output /dedicated/tenant-benchmark \
  --reuse --run-name resident --resident-tenants 8
python Test/tenant_benchmark.py --bin-dir Release --output /dedicated/tenant-benchmark \
  --reuse --run-name pressure --resident-tenants 2 --concurrency 1 4 \
  --seconds 15 --repeats 2
```

输出包含 `dataset.json`、查询/ground-truth 数组、构建日志、独立命名的结果 JSON 与 server 日志。结果 JSON 保留每次测量而非只输出一个最佳 QPS；新脚本还记录 server/client 二进制 SHA-256。索引与源向量保留供后续对照，不能让两个实验进程同时打开相同 node 目录。本机 Conda 环境运行时额外按进程预加载系统 libstdc++ 和 jemalloc，不修改全局库配置。

### 13.2 结果与解释

2026-11-02 实测，AMD EPYC 7V12，client/server 共用 CPU 0–7。这里的“基线”是**本轮驻留缓存改造前、已经支持同 tenant 并发的多租户版本**，不是原始单索引 SPANN。全驻留每个配置 3 轮，表中为各轮指标的中位数；p95/p99 不是把所有轮样本合并后的分位数。

| 总客户端并发 | 基线总 QPS | 新版本总 QPS | 新版本 p50 / p95 / p99（ms） |
|---|---:|---:|---|
| 1 | 1,018 | 923 | 1.078 / 1.638 / 1.768 |
| 4 | 2,968 | 2,963 | 1.344 / 2.004 / 2.255 |
| 8 | 3,949 | 3,940 | 1.973 / 3.254 / 3.939 |
| 16 | 4,784 | 4,781 | 3.242 / 5.163 / 6.640 |

并发 4/8/16 的总吞吐差异约 0.1%–0.2%，本次测量中基本接近。主实验的并发 1 中位数下降约 9.3%；基线三轮为 922 / 1,018 / 1,018，新版本为 923 / 918 / 935，存在明显轮次/状态差异，不能据此宣称所有并发度均无回退，也没有进行统计显著性或 CPU/profile 归因。

针对这一差异另做 A/B/A/B 复核：每次独立启动 server、并发 1、预热 5 秒、测量 15 秒，基线分别为 **927.7 / 922.6 QPS**，新版本为 **911.2 / 939.6 QPS**，均无错误。独立启动的交错复核没有复现稳定的 9.3% 差距；保留主实验数字，不用复核结果替换或挑选最佳值。启动/运行顺序、workspace/cache 状态及共享环境的影响仍需更严格的性能归因。

新版本并发 16 的分租户结果：

| tenant 向量数 | QPS | p50 / p95 / p99（ms） | recall@10 |
|---|---:|---|---:|
| 10,000 | 1,195 | 2.488 / 4.111 / 5.437 | 60.85% |
| 50,000 | 1,195 | 3.115 / 4.819 / 6.330 | 41.70% |
| 100,000 | 1,195 | 3.346 / 5.117 / 6.598 | 32.77% |
| 200,000 | 1,195 | 3.943 / 5.833 / 7.231 | 26.75% |

分租户 QPS 接近是均匀请求配比的结果，不代表大小租户的独立容量相同。三种场景的 recall 相同，缓存没有改变索引内容；但**当前固定参数在该合成数据上的 recall 偏低，4,781 QPS 不是高召回生产服务能力的承诺**。需要在真实数据及明确 recall 目标下再做参数与性能评估，本轮不扩展修改 ANN 算法或隐藏低召回结果。

缓存压力场景：四 tenant 轮转、仅 2 个槽位，每组 2 轮 × 15 秒：

| 总客户端并发 | 总 QPS | p50 / p95 / p99（ms） | 各轮结束时 RSS 最大值 |
|---|---:|---|---:|
| 1 | 52.1 | 17.986 / 39.071 / 39.806 | 253.5 MiB |
| 4 | 212.9 | 16.354 / 39.890 / 41.051 | 284.0 MiB |

这是容量不足导致持续加载/淘汰的压力场景，吞吐明显低于全驻留；不能把 head 缓存设计理解为“淘汰没有成本”。同并发 4 的全驻留各轮结束 RSS 最大值约 435.2 MiB，压力场景约 284.0 MiB；这里只采样每轮结束 RSS，不是过程峰值或硬内存上限。全驻留并发 16 时 server/client 分别约使用 6.68 / 0.98 个 CPU 核，server 各轮结束 RSS 最大值约 438.5 MiB。

三组主实验共完成 2,284,065 次成功请求（含时间窗结束后的 drain），错误数均为 0。原始输出保留在本次 session 的 `files/benchmark-baseline/`：`results.json` 为改造前基线，`resident.json` 为新版本全驻留，`pressure.json` 为缓存压力；同目录有数据清单、query/ground-truth 和各组 server 日志。改造前后可执行文件分别保存在 `files/baseline-binaries/`、`files/resident-binaries/`，脚本可重建数据和复现实验。

本次 artifact 根目录为 `~/.copilot/session-state/7de262b3-9792-4fd2-9b05-38956827dc65/files/`；额外交错复核结果为 `benchmark-baseline/{baseline,resident}-{a,b}-c1.json`。基线和新版本结果不能解释为相对原始单租户 SPANN 的额外成本测量，也未覆盖写入吞吐、读写混合或生产 SLA。

### 13.3 SIFT 受控对照与瓶颈实验

2026-11-04 开始补齐真正的原始 SPANN 对照。参考
[`Benchmarking.md`](https://github.com/microsoft/SPTAG/blob/196d1e581489785ba23f88a760a6d6e09b5f909c/docs/Benchmarking.md)
的数据定义和报告规范，不将其 FILEIO/TiKV 历史 QPS 当作当前实现的基线。

- 数据：SIFT1B 前 1,000,000 条 UInt8/128D，平方 L2、top-5；重写 DEFAULT 文件行数头，使用完整公共 10K 查询集，并单独汇报前 200 条查询的召回率。基础数据 SHA-256 为 `7cdd76b29c338b78bf17c2ad5060b383110b0bf6ba914e8a77e7e642ba7a6111`，查询为 `eca755831fc9a8004e14886df48b81109a8ed3bfc6b632509c93c0460a30d552`。
- 第一组：A 为真正未引入 tenant 的 `cafdd0ab`；B 为当前代码的 legacy 路径；C 为当前代码的单 tenant 全驻留路径。A/B 读取同一份原始版本构建的索引；C 独立构建，因此必须报告召回与构图差异，不能把全部差异解释为租户路由成本。
- 首轮测量不修改产品参数，legacy 构建显式对齐当前 tenant 的参数（包括 `SearchInternalResultNum=32`、`MaxCheck=256` 和单线程构建），不是参考文档的 `4/4/4` 和 `SearchInternalResultNum=64` 配置。legacy 查询关闭异步搜索触发的 merge，保证只读测量。后续参数扫描需单独标注，不能混入原始版本结果。
- 两条路径均使用 RocksDB，但旧 `RocksDBIO` 与 `SharedRocksDB` 的存储配置并不相同：旧路径启用 blob、3 GiB block cache，新路径默认共享 64 MiB block cache。第一轮是实现整体成本对照；存储布局、缓存、workspace 和逐向量并发保护都是待分离的因素，不预先断言瓶颈。
- 查询驱动为 `Test/tenant_perf.cpp`，同源编译到两个版本；数据和精确 truth 为 `Test/sift_benchmark.py`，实验顺序由 `Test/sift_runs.py` 记录。先测进程内核心路径，不与 Python/TCP serving QPS 混比。每组并发 1/4/8/16，预热 5 秒、测量 30 秒，3 轮旋转 A/B/C 顺序，所有进程限制在 CPU 0-7。
- QPS 只包含窗口内成功完成请求；延迟含窗口末尾 drain、排除失败；错误单独计数。CPU 为进程 CPU 时间除以含 drain 的 wall time；RSS 为进程历史峰值，包含加载恢复阶段，不称作测量窗口内峰值。缓存指标为进程累计值，可用预热/测量前后差分。
- 多租户计划：固定 seed 20261104 对百万向量互斥分区，大小 100K/200K/300K/400K，各自精确 truth；比较随机均匀与 `70/10/10/10` 热点请求分布、全驻留与两个槽位。不同于 §13.2 强制轮转的最坏缓存访问。
- 更新计划：990K 初始 + 10 批各 1K，先无并发查询，再混合读写；分开记录插入及显式 checkpoint 时间。并发插入记录输入行到实际分配 VID 的映射，按阶段语料计算 truth，不能假定输入提交顺序等于 VID 顺序。
- 当前宿主 `perf_event_paranoid=4`，无权限使用 perf 事件；未修改机器全局配置。性能归因需使用可运行的对照或用户态证据，不将代码猜测当作 profile 结论。

本组 artifact 在本 session 的 `files/sift-study/`：独立原始源码/构建、两版固定二进制、数据清单、逐次 CSV、完整日志与 `manifest.json`。本轮主实验及两个单变量诊断对照已完成，诊断修改均只在 session 副本内；未修改产品核心实现，也未实现自动 checkpoint、租户准入或 TiKV。

#### 首轮单租户结果（2026-11-04）

A/B/C 的 36 个计时轮次已全部完成，查询错误为零。下表为各配置 3 轮 QPS 的中位数；均为进程内查询，不是 TCP serving。

| 并发 | A：原始 SPANN | B：当前 legacy | C：当前单 tenant | C 相对 A |
|---|---:|---:|---:|---:|
| 1 | 4,247.3 | 4,241.9 | 3,307.2 | -22.1% |
| 4 | 15,622.0 | 15,544.8 | 9,864.2 | -36.9% |
| 8 | 29,380.3 | 29,428.6 | 16,973.3 | -42.2% |
| 16 | 27,019.1 | 27,007.1 | 16,393.8 | -39.3% |

完整 10K 查询的 strict ID recall@5：A/B 为 **76.246%**，C 为 **76.244%**；距离容差 recall 分别为 76.268% / 76.266%。前 200 条均为 76.2%。因此这些结果反映当前固定搜索预算下的性能，不是参考报告约 98% recall 的性能，也不能取代相同高召回目标下的下一轮实验。

B 与 A 的吞吐接近，而 C 有稳定且随并发扩大的差距。并发 8 时三者都使用约 8 个 CPU 核，尚不能据此区分逐向量同步、workspace、共享存储或其他 CPU/内存开销；不能直接归因为租户大锁串行化。

独立 GDB 子进程采样已完成：每版本 60 次暂停，每次观察 8 个查询线程，共 480 个查询线程快照。必须先识别查询线程 ID，再包含这些线程的所有快照；只筛选含 `Measure` 的栈会漏掉超过 12 帧的深层 RocksDB 调用，产生严重偏差。粗粒度分类如下（并非 CPU 时间占比；暂停/恢复会扰动调度）：

| 栈位置 | 原始 A | 当前 legacy B | 当前 tenant C |
|---|---:|---:|---:|
| RocksDB 路径 | 271 | 265 | 229 |
| workspace 获取/重置 | 44 | 49 | 83 |
| head 搜索 | 108 | 106 | 77 |
| posting 搜索 | 43 | 50 | 61 |
| rwlock | 1 | 0 | 17 |
| 其他 | 13 | 10 | 13 |

C 中部分 rwlock 栈直接位于 posting 搜索，而不是 tenant 生命周期入口；workspace 的热点包括去重表 `memset`。此外旧存储使用 single-CF/PinnableSlice 批量 MultiGet，新共享存储使用另一个 MultiGet 重载，C 栈中可见逐项 `Version::Get`。这些是归因线索，不足以将整个差距归结于单一因素。受调试器扰动的运行不纳入 QPS 表。

**MultiGet 单变量对照已完成，未支持这个重载是主要回退原因。** 仅在独立源码/静态库副本中换成 single-CF/PinnableSlice 重载，使用同一已构建 tenant 索引，其余条件不变；交错运行每组 3 次、预热 5 秒、测量 15 秒。控制/替换版本的 QPS 中位数为：并发 1 **3,311.9 / 3,261.1**，并发 8 **16,909.5 / 16,659.9**，分别约 -1.5%，没有改善。12 轮无错误，完整 10K 查询输出逐字节一致。保留这个负结果，不将候选优化冒充已确认瓶颈或提交到产品代码。artifact 为 `sift-study/multiget-ablation/`。

**Workspace 单变量对照确认了一个显著的并发开销来源。** 在另一个独立副本中将共享 workspace 工厂改为线程局部 workspace，限定单租户、只读、无淘汰，保持同一已构建索引、存储实现、搜索参数和版本标签同步不变。相关模板实例化对象全部重编译；与控制版本交错运行，每组 3 次、预热 5 秒、测量 15 秒：

| 并发 | 控制版本 QPS | 线程局部 workspace QPS | 变化 | 控制 / 线程局部 p95 ms |
|---|---:|---:|---:|---:|
| 1 | 3,318.2 | 3,344.1 | +0.8% | 0.343 / 0.339 |
| 8 | 17,022.0 | 21,420.2 | +25.8% | 0.613 / 0.446 |

12 轮全部无错误，完整 10K 查询输出逐字节一致。并发 8 的三次线程局部结果为 21,420 / 21,741 / 21,334 QPS，控制为 17,056 / 16,854 / 17,022；改善在本次交错重复中一致出现，而单线程变化很小。该实验同时改变了共享池互斥协调和 workspace 跨线程/CPU 迁移，尚未将两者分别量化，不能单独宣称全部收益来自去掉 mutex 或改善缓存局部性。

它也没有消除全部差距：原始 SPANN 主实验并发 8 为约 29,380 QPS。逐向量版本检查、RocksDB 存储布局/缓存配置等仍是未分离因素，不能凭栈中出现就认定为已证实瓶颈。

该诊断实现使用 pointer-keyed TLS，不具备生产需要的实例代际/回收协议，不可直接用于正式多租户生命周期。诊断阶段没有修改产品；后续正式实现采用 §6.1 的独立实例 ID、线程槽位和卸载回收协议。保留本次诊断的原始结果，artifact 为 `sift-study/workspace-ablation/`。

#### 四租户缓存结果

48 个计时轮次全部成功。完整 10K 查询 strict recall@5：100K/200K/300K/400K 租户分别为 **84.914% / 82.242% / 80.692% / 79.578%**。以下是并发 8、每组 3 轮的中位数：

| 请求分布 | 驻留槽位 | 总 QPS | p95 ms | CPU 核 |
|---|---:|---:|---:|---:|
| 随机均匀 | 8（全驻留） | 18,168 | 0.593 | 7.99 |
| 随机均匀 | 2 | 385 | 51.716 | 2.27 |
| 70/10/10/10 | 8（全驻留） | 22,302 | 0.591 | 7.99 |
| 70/10/10/10 | 2 | 650 | 46.457 | 2.33 |

热点是最小的 100K 租户，所以热点工作负载即使全驻留也更快；不能将所有提升解释为 LRU 收益。并发 8 的第二轮，均匀/热点两槽位分别在 30 秒测量期间发生 2,526 / 2,601 次在线加载和同样次数的淘汰，全驻留则没有加载或淘汰。这支持将两槽位的主要问题定位到反复恢复实例和容量等待，而不是计算资源饱和；缓存 miss 计数含等待/重试，不直接除以请求数当 miss rate。

两槽位也不等于更低 RSS：并发 16、均匀访问的进程峰值 RSS 中位数约 **449 MiB**，全驻留约 **383 MiB**。该指标包含启动恢复、workspace 和分配器高水位；不能据此推导内存泄漏，但再次说明 snapshot 字节计费与驻留数量不是硬 RSS 上限。

#### 插入、并发查询与 checkpoint

两组各完成 990K 初始构建和 10 批各 1K 插入，4 个写线程、每请求插入 1 条。分别为 0 或 4 个并发查询线程；查询使用参考 workload 的前 200 条，并在每一批后按实际分配 VID 映射验证阶段 truth。

| 并发查询线程 | 总插入时间 s | 总 checkpoint 时间 s | 仅插入 vectors/s | 插入+checkpoint vectors/s |
|---|---:|---:|---:|---:|
| 0 | 0.502 | 13.974 | 19,913 | 691 |
| 4 | 0.679 | 14.602 | 14,727 | 654 |

这些吞吐不包含初始构建、逐阶段 accuracy 查询和重新打开，不能与参考报告不同参数/后端的插入数字直接比较。按照每 1K 条显式 checkpoint 的频率，checkpoint 占计入时间的 **95.6%–96.5%**，是该批处理策略的主要成本。并发查询组累计完成 157,389 次查询、零查询错误，各批 p95 为 0.409–0.566 ms；延迟统计覆盖插入及 checkpoint 的合并窗口，绝大部分窗口是 checkpoint，不能宣称已经覆盖长时间持续写入。

两组 11 个阶段的 strict recall@5 均在 75.4%–75.6%，最终为 75.6%；checkpoint/Close 后重新打开的查询输出与关闭前逐字节一致。这里验证的是受控重新打开，不是额外的进程强杀/掉电实验。长时间持续写入、跨租户写入同时发生缓存淘汰，以及约 98% recall 的参数对照仍需单独扩展，不能把本轮结果称为这些场景的完成验证。

### 13.4 已批准优化的实施与归因（2026-11-04）

本轮只处理 multi-tenant 引入的开销及已批准的条件优化，不修改原 SPANN 的 visited-table 大小/清零策略，不引入通用 Dataset 并发重构、自动 checkpoint 或 tenant 计算配额。保留了改造前源码和可执行文件；下面的“基线”是优化前 multi-tenant 版本，不是原始 SPANN。

#### 实施范围和读路径结果

| 改动 | 当前实现 |
|---|---|
| Workspace 局部性 | 按运行时实例 ID 隔离的 TLS，复用长驻 worker；线程退出和实例卸载均回收缓冲区，见 §6.1 |
| VersionLabel | 恢复为原始 `cafdd0ab` 文件，移除新增的逐候选锁；继承的未证实风险仍保留在 §1.1 |
| 驻留查询入口 | `Search` 和文本查询所需的 `Describe` 使用不可变目录及节点 epoch，不取全局 cache/lifecycle 锁；miss、更新和生命周期慢路径仍有协调 |
| Posting 数据拷贝 | `PinnableSlice` 直接复制到请求 workspace 的 PageBuffer，去掉中间 `vector<string>`；不是零拷贝，也不向查询暴露 RocksDB pin 的生命周期 |
| Dirty checkpoint | 分段测量证实 snapshot 导入成本后，增加未变 chunk 复用；原 SPANN 全量序列化及 checkpoint 频率、发布和 durability 语义不变 |

读路径使用同一份已构建的 1M SIFT tenant 索引、10K 查询、L2 top5 和原有 `SearchInternalResultNum=32 / MaxCheck=256`。固定 CPU 0–7；并发 16 是在这 8 个 CPU 上运行 16 个 worker，不是 16 核扩展实验。每轮预热 3 秒、测量 15 秒，基线/优化版交错运行，各 3 次，表中为中位数；共享 block cache 始终为 64 MiB。

| 并发 | 基线 QPS | 四项读路径改动 QPS | 变化 | 基线 / 优化后 p95 ms |
|---|---:|---:|---:|---:|
| 1 | 3,422 | 3,450 | +0.8% | 0.333 / 0.331 |
| 4 | 9,999 | 12,990 | +29.9% | 0.481 / 0.350 |
| 8 | 17,232 | 24,390 | **+41.5%** | **0.607 / 0.374** |
| 16 | 16,604 | 22,179 | +33.6% | 3.623 / 3.410 |

单线程近似持平，不能宣称普遍单线程提速。并发 8 的进程峰值 RSS 中位数为 198.4 → 206.5 MiB；没有增大 block-cache 预算，但 workspace/分配器高水位仍影响 RSS。释放 workspace 表示归还分配器，不保证 RSS 立即下降。

独立消融也各运行 3 次；下面只改一个因素，不能把百分比相加：

| 并发 8 的变体 | QPS | 相对同组基线 |
|---|---:|---:|
| 基线 | 17,170 | — |
| 仅实例隔离 TLS | 22,018 | +28.2% |
| 仅恢复原 VersionLabel | 18,250 | +6.3% |
| 仅直接 PageBuffer 拷贝 | 17,014 | −0.9% |
| 仅 epoch 管理入口 | 17,190 | +0.1% |
| TLS + VersionLabel + 直接拷贝 | 24,109 | +40.4% |

主要可见收益来自 workspace 局部性和撤回新增的版本锁；没有把 mutex 与跨 CPU workspace 迁移各自的贡献单独量化。epoch 的价值还在于生命周期提交期间其他驻留 tenant 的查询可继续，不是本负载下的显著 QPS 提升。直接拷贝没有独立显示吞吐收益；在最终组合中再次移除它，C8 为 24,821 对保留时 24,659 QPS，C1 为 3,492 对 3,537 QPS（各 2 次），方向不一致，视为近似持平。

所有读路径及存储对照的完整 10K 输出与 tenant 基线逐字节相同；加入 chunk 复用后的最终产品也相同。strict recall@5 为 **76.244%**，这仍不是约 98% recall 的配置。准确率输出 SHA-256 为 `d2900b20b6e53f99c8918c8f7d9b341107ff0310545362666dd9a8b6455118ca`。

#### 换入换出：不是本负载下的全局 LRU 锁瓶颈

对优化前 manager 单独加入线程局部阶段计时，使用 100K/200K/300K/400K 四租户、均匀访问、2 个驻留槽位。下表百分比的分母是所有 worker 的查询累计 elapsed time，含 2 秒预热及 10 秒测量，**不是 CPU 占比，也不是单进程 wall-time 占比**。启动主线程不计入；条件变量等待含重新获取 mutex 的时间。

| 阶段 | C1 | C8 | C16 |
|---|---:|---:|---:|
| cache mutex 获取等待 | <0.01% | 0.17% | 1.01% |
| cache mutex 持有 | 0.01% | 0.27% | 0.46% |
| LRU victim 扫描 | <0.01% | <0.01% | <0.01% |
| 等待同 tenant 加载/淘汰 | — | 50.49% | 59.29% |
| 容量不足、无空闲候选的等待 | — | 20.83% | 22.60% |
| `VectorIndex::LoadIndex` | 88.89% | 22.03% | 10.51% |
| 实例析构/释放 | 3.60% | 0.84% | 0.42% |

`LoadIndex` 平均分别为 21.16 / 20.00 / 20.85 ms；本地工作快照已经复用，仍需反复恢复内存实例。C8 中 cache mutex 总持有 0.259 秒，约占 12 秒运行窗口的 2.2%，并非饱和的串行资源；全驻留 C8 的获取等待/持有占 worker elapsed 的 0.05% / 0.07%。这里测的是 manager 的实例缓存锁，不是 RocksDB block-cache 内部锁。

因此没有继续重写 LRU 策略或为它引入复杂调度。优化前/后的一对额外 C8 对照，全驻留为 18,227 / 19,254 QPS，两槽位为 405 / 426 QPS；两槽位 p95 仍为 48.947 / 48.111 ms，**没有解决容量不足导致的反复加载**。这组仅各一次，不作为精确增益估计；结论也不能外推到上千 tenant 或更多 CPU 的所有工作负载。

#### 存储预算与布局：分开归因，没有强改默认值

先固定旧读路径，只改变节点共享 block-cache 预算，每项运行 2 次并反转预算顺序：

| 预算 MiB | C1 QPS | C8 QPS | C8 峰值 RSS MiB |
|---|---:|---:|---:|
| 64 | 3,551 | 17,354 | 199.4 |
| 256 | 4,617 | 21,869 | 402.6 |
| 1,024 | 4,857 | 22,533 | 438.3 |
| 3,072 | 4,909 | 22,639 | 438.0 |

这是资源投入曲线，不是无成本代码优化。旧 SPANN 路径的一次同预算对照，64 MiB / 3 GiB 的 C8 分别为 29,682 / 29,558 QPS；不能只看两个默认预算不同，就把旧路径与 tenant 的差距全部归因于缓存容量。

布局实验实际重写了 posting SST/blob，而不是仅修改 Open options；生成了 251,056,904 字节的 blob 文件，并单独采集 RocksDB 统计验证实际 blob 读写。转换发生在预热/计时前，不计入查询 QPS。旧读路径下，64 MiB 的 SST/blob 为 17,481 / 18,826 QPS，256 MiB 为 21,644 / 18,397 QPS。由于直接 MultiGet 路径会影响布局收益，又在四项改动保持不变时复核（C8，各 2 次）：

| 预算 MiB | 布局 | QPS | p95 ms | 峰值 RSS MiB |
|---|---|---:|---:|---:|
| 64 | SST | 24,659 | 0.369 | 207.7 |
| 64 | blob | 31,658 | 0.283 | 135.6 |
| 256 | SST | 35,125 | 0.260 | 415.4 |
| 256 | blob | 31,489 | 0.284 | 176.9 |

blob 在 64 MiB 时提高约 28.4%，但在 256 MiB 时比 SST 慢约 10.4%；收益依赖布局、预算和读 API 的组合。所有实验都是预热后的读，blob 依赖 OS 文件缓存，较低进程 RSS 不能直接等同于较低节点总内存，更不能据此推断冷存储表现。

**本轮保持生产默认 64 MiB 共享 cache 和现有 posting 布局。** 没有给每个 tenant 分配大 cache，也没有因一组正向结果强制切换全部存储。布局和预算诊断开关仅存在于隔离实验变体，不是新增生产配置项；现有 `CacheBytes` 仍可由部署显式设置。

#### Checkpoint：测量后实施 chunk 复用

先对原尺寸 990K 初始构建 + 10 批各 1K 插入做分段计时，4 个写线程、0 个查询线程，每批显式 checkpoint。10 次 checkpoint 共 14.005 秒，分解如下：

| 阶段 | 累计秒数 | 占 checkpoint elapsed |
|---|---:|---:|
| 原 SPANN 文件序列化 | 1.603 | 11.45% |
| 读取序列化文件 | 0.067 | 0.48% |
| `head_snapshots` chunk Put | **8.801** | **62.84%** |
| State DB checkpoint | 2.324 | 16.59% |
| Recovery 发布 | <0.001 | <0.01% |
| 清理旧产物 | 0.299 | 2.14% |
| 其他未单独拆分 | 0.910 | 6.50% |

本实验先等待每批插入返回，再调用 checkpoint；排空耗时不到 0.001 秒，不能推广成持续并发写时排空总是免费。以上只统计主线程相应阶段，未混入后续写线程的计时或初始构建。

这支持仅优化新增的 snapshot 导入层：比较完整字节而不是依赖弱 hash，复用未变块，清理缩短/消失文件的旧块，最后写 manifest。工作状态仍不可当作已发布恢复点；任何失败仍沿用节点 fencing 和旧 checkpoint + recovery log 的恢复规则。

随后从同一个**关闭后复制的 990K 初始节点**出发，固定四项读路径改动，仅对比 chunk 复用。0 查询线程交错运行各 3 次；4 查询线程各一次。以下为每轮 10 次 checkpoint 的总量，0 查询线程取中位数：

| 查询线程 | 原 snapshot 导入 s | chunk 复用 s | 缩短 |
|---|---:|---:|---:|
| 0 | 16.654 | **5.241** | **68.5%** |
| 4 | 18.241 | **5.095** | 72.1% |

主对照约快 3.18 倍；不能把前一个 profile 的 14.005 秒与这里的优化后值混算成同组增益。每轮 `head_snapshots` Put 从 **490 次 / 444,054,820 字节**降至 **41 次 / 19,917,851 字节**，payload 减少 **95.5%**；这是应用层 Put 字节数，不是底层压缩后物理写入量。0 查询线程的总插入时间为 0.356 / 0.359 秒，近似持平。

8 轮共 88 个准确率阶段均为 75.4%–75.6% strict recall@5，最终 75.6%；重新打开输出检查通过。并发查询对照零错误，但每批 p95 区间从 0.239–0.385 ms 变为 0.286–0.507 ms，**没有证据表明混合窗口的查询尾延迟也改善**。窗口是插入与 checkpoint 的合并窗口，缩短 checkpoint 后插入、序列化及比较读取占比发生变化；持续混合负载的查询 SLO 仍需单独评估。

#### 验证与复查材料

最终 Release 与 ASAN/leak 检查均通过 26 个 tenant 用例、1,648 个断言，包括未变 chunk 不被重写、缩短/消失/空文件清理、重开恢复、并发查询和缓存生命周期。真实 Python/TCP smoke 覆盖 CRUD、同连接并发、GIL 释放、旧 Search、超时/迟到响应及进程崩溃恢复；无 RocksDB 的原 BKT/KDT/SPANN 算法构建与用例通过。

独立 TSAN 压测完成 5,001 次 epoch 回收和 36,071,834 次读取；联合 workspace/epoch 压测完成 5,002 次回收，长驻线程未退出时存活 workspace 为零。**这不是整个原 SPANN 并发读写实现通过 TSAN 的声明。** §1.1 的继承风险仍未据此消除。

可复查 artifact 位于本次 session 的 `files/optimization-batch/`：`baseline/` 保存优化前来源，`results/manifest.json` 和 `analysis.json` 保存主实验命令、binary hash、原始结果和阶段汇总；`interaction-results/` 保存布局/API 交互；`checkpoint-results/` 保存相同初始节点上的对照、Put 计数及各阶段召回率；构建/回归日志、TSAN 程序和实验脚本也一并保留。生产源码没有加入这些诊断计时器。

## 14. 单 RocksDB 引擎方案：设计提案，尚未实现

### 14.1 结论与边界

**两个 RocksDB 引擎可以合并，但不能把现有 CF 放进同一个 DB 后直接复用整库回滚。**
建议评估“单引擎、两个逻辑 CF 组、仅恢复 state CF”的方案；当前生产实现仍保持两个引擎。
本节不是已完成迁移、已测得性能提升或更强 durability 的声明。

这里合并的是引擎，不是删除 application recovery log，也不是把 postings 和日志混成一个 CF。
`postings` 需要 append merge operator；日志、catalog、snapshot chunk 的读写及保留策略不同，
应继续用独立 CF。对外 tenant API、tenant/index ID namespace、VID 分配顺序、epoch 生命周期和
“只承诺进程崩溃恢复、不承诺断电恢复”的契约不变。cached query 不应因此增加全局锁。

### 14.2 为什么直接合并会破坏恢复

当前 checkpoint 的重要性质是：**可回滚的 state 和不能随它回滚的 redo log 在不同引擎。**

例如 checkpoint 覆盖 tenant sequence 100，之后操作 101 已成功返回。现有恢复流程从
State DB checkpoint 创建工作副本，再从独立 Recovery DB 读取并重放 101。
如果合并后仍把整个 DB 替换成 checkpoint，101 的日志也会被回滚；这个已确认操作将丢失。

另一种错误做法是保留最新合并 DB，仅加载旧 head snapshot 后重放 101。live postings
可能已经包含 101 的部分或全部效果，甚至包含相关 split/reassign 的多次写入。
当前 posting merge 是字节追加，不是按 operation ID 去重的事务；不能假定重复应用是幂等的。
RocksDB 内部 WAL 也不能单独重建尚未 checkpoint 的内存 BKT/head 与 version map。
因此，合并引擎不能同时省略 application log 和旧 state 的恢复能力。

### 14.3 推荐候选：单引擎、state-only checkpoint

| 逻辑组 | CF | 恢复时的处理 |
|---|---|---|
| State | `postings`、`head_snapshots`、`checkpoint_meta` | 从已发布 checkpoint 整组重建，再重放新日志 |
| Recovery | `recovery_log`、`catalog`、`recovery_meta` | 保留 live DB 中的最新内容，绝不从旧 checkpoint 覆盖 |
| RocksDB 必需 CF | `default` | 不存业务数据，不参与 state 回滚 |

一个 node owner 持有唯一 DB handle、CF handles 和共享 cache；posting view 只保留引擎引用及
namespace，不拥有 checkpoint、drop CF 或关闭引擎的权限。查询仍然直接访问当前 `postings` CF，
不引入每次读取都要查找多个 generation 的 overlay。

新恢复点不是可直接替换 live DB 的整库目录，而是一个 immutable **state checkpoint group**：
包含三个 State CF 的导出文件、完整导入元数据、逻辑 tenant catalog、每个 tenant 的 covered
sequence、格式版本和校验信息。所有路径必须约束在注册的 checkpoint 目录内。
导出元数据需要明确、可校验、可版本化的磁盘编码，不能直接保存 C++ struct 的内存布局。

本机 RocksDB 7.6 的公开头文件提供
`Checkpoint::ExportColumnFamily` 和 `DB::CreateColumnFamilyWithImport`。
前者会 flush 指定 CF，在同文件系统上 hard-link SST；后者创建新 CF 并导入文件，默认 copy。
这些 API 的存在只是可行性依据，**尚未验证它们在本系统中的恢复时延、空间成本和全部崩溃边界**。
特别是 import 的公开契约说明：调用期间崩溃后 CF 可能已经存在；恢复不能简单假设它不存在。
当前生产 postings 为 SST 布局；未来若启用 blob，必须另行验证导出/导入是否完整覆盖 blob 依赖。

### 14.4 写入、checkpoint 与发布顺序

正常写入保留现有顺序：在 tenant submission 临界区内分配 VID 并追加 application log；
随后执行 index/posting 更新，等待该请求关联的维护任务，成功后才返回。所有这些写入可以使用
同一 RocksDB WAL，但仍是多个写操作，不能称为“log + 全部 index 更新已经成为一个事务”。
除非另外证明安全，State 与 Recovery 写入都不关闭 WAL。

checkpoint 的候选流程如下：

1. 进入节点 checkpoint 协调，停止新的 mutation，并排空已提交写入和维护。所有可能修改
   State CF 的路径，包括 dirty eviction 和 snapshot 导入，都必须纳入同一个 freeze 边界；
   不修改状态的 resident query 可以继续运行。
2. 序列化 head/version 状态，以现有 chunk 复用方式更新 `head_snapshots`，写入对应 catalog。
   得到每个 tenant 的 covered sequence；不能用一个 RocksDB sequence number 代替它们。
3. 保持 State 不再变化，依次导出三个 State CF，持久记录导入元数据及完整性 manifest。
   多次 CF export 本身不是一个跨 CF 原子操作；一致性来自这个覆盖所有 State mutation 的 freeze。
4. 确认 checkpoint group 完整后，在同一 WriteBatch 中发布 `recovery_meta/current`、
   `catalog/tenants` 及必要的格式/完整性标识，再执行与现有承诺一致的 WAL flush。
5. 只有发布成功后，才删除各 tenant covered sequence 以内的 application log，并回收不再引用的
   checkpoint/恢复临时产物。失败则 fencing；不能先删日志再更新指针。

现实现的普通 Put/Merge 使用默认 `WriteOptions`，没有每次请求的 `sync=true`；
checkpoint publication 的 `FlushWAL(false)` 也不是 fsync。合并不能悄悄把这些结果描述为
machine-crash/power-loss durable。若将来增强该契约，文件、目录和 WAL 的 sync 顺序需要独立设计。

### 14.5 恢复状态机：只替换 State，不替换 Recovery

恢复期间整个 node 不接收请求，不发布 resident index 指针。候选步骤：

1. 打开 live DB 的实际 CF 集合，保留并验证三个 Recovery CF，读取它们的最新 published pointer。
   在碰 State 之前验证被引用 checkpoint group 的文件、版本、catalog 和导入元数据。
2. 在 Recovery 元数据中记录 restore intent：目标 checkpoint、格式及恢复阶段。
   新格式的 bootstrap 必须能识别上次中断留下的缺失/部分创建 State CF，不能把这种中间态当作
   一个全新空节点；没有有效 intent 的异常布局则 fail closed。
3. 销毁所有旧 index、posting view 和 State CF handle，删除现有 State CF，再从同一个 immutable
   checkpoint group 重建三个 State CF。import 使用 copy，不消耗唯一恢复点。
   三个 CF 不要求一起原子创建，因为恢复阶段没有请求进入。
4. 若此时崩溃，下次启动重新检查 live Recovery CF，丢弃任何部分恢复的 State CF，重新执行步骤 3。
   不从部分恢复状态“接着重放”，也不删除被引用 checkpoint。
5. 加载 head/version snapshot，核对它的 catalog 与已发布 catalog；按每 tenant sequence 连续重放
   covered sequence 之后的 application log。遇到缺口、VID 不匹配或状态错误，fence 而不是跳过。
6. 重放全部成功后，才能清除恢复中的标志并允许服务。若在 replay 中间再次崩溃，仍从原 checkpoint
   重建 State 后重放，而不是对已经部分更新的 postings 再做 append。

重放完成后、发布下一个 checkpoint 前，旧恢复点所需的新日志仍必须保留。
分配新 index/artifact ID 的单调计数器属于 Recovery，不能被旧 snapshot 降回去。
在线 drop/recreate CF、后台在线格式迁移和多版本 query snapshot 都不在这个方案的第一阶段内。

### 14.6 其他方案与取舍

| 方案 | 优点 | 主要代价/风险 |
|---|---|---|
| 保留现有双引擎 | 恢复隔离清楚，已有故障验证，不改格式 | 两个 DB 的 WAL/元数据和写队列 |
| 单引擎 + state-only export/import | 保留直接 posting 读取；redo 不回滚；可能避免逐条复制 | 新 checkpoint 格式、恢复状态机及 RocksDB import 验证工作 |
| 单引擎整库 checkpoint + 只逻辑复制 State 到新 namespace | 可作为较简单的正确性原型，live 日志保持不动 | 恢复需扫描/重写 state；整库 checkpoint 还会 pin 日志 SST；未必比当前文件复制快 |
| 持久化 generation/多版本 posting overlay | 可通过选择旧视图回滚，不必恢复整个 State CF | 多层读、版本 GC、merge/base 语义及空间放大；直接影响 cached-query 热路径 |
| 单 DB 后直接删除 application log | 表面上减少一份日志 | 内存 head 与多步 posting 更新没有原子持久化；当前架构下不可采用 |

两个现有引擎**已经共享同一个 block cache**。合并不等于 cache 内存减半；大多数 CF 的
memtable、SST 和业务数据仍然存在，也不能假设 RocksDB 后台线程简单减半。
可能节约的是重复的 DB/WAL/manifest 管理及部分写队列开销，收益需要实测。

相反，一个 CF 引起的 write stall 可能影响整个合并 DB。大量 snapshot chunk、posting compaction
或日志清理可能拖慢 application-log append，扩大写入尾延迟；必须保留 CF 级统计并测试这种干扰。
统一 WAL 的保留期限也受尚未 flush 的 CF 影响。不能仅统计少了一个目录或一次 Open 就宣称优化。
本轮 §15.14 已观察到 Shared State DB 的 snapshot 写回触发 RocksDB write-thread 等待，
甚至使需要 dirty eviction 的 Search 等待写入。合并引擎的原型必须测量 Recovery CF 是否
进一步受同一引擎的 backpressure 影响；这不是已经运行了单引擎对照、或证明它必然更慢。

### 14.7 迁移与验收门槛

迁移只做离线转换：关闭并 checkpoint v1 节点；从明确的 v1 恢复点构建独立 v2 目录；
验证 namespace、向量数、删除状态、covered sequence、query 结果及重开恢复；最后由显式部署动作
切换根目录。保留 v1 副本，禁止原地覆盖。v1/v2 都必须拒绝不支持的格式，不能自动猜测或降级打开。
只要 v2 已经接受新写入，就不能声称切回旧 v1 副本是无数据损失的 rollback。

实现前至少完成以下实验，且与同一 workload 的双引擎对照：

- 在 export、manifest 完成、publication、日志裁剪、逐个 CF drop/import 和 replay 中途注入进程崩溃；
  验证所有已确认操作、tenant 隔离、VID 连续性、删除状态及重复恢复。
- 覆盖 checkpoint 后已有新日志、未确认但已记录的写入、损坏/缺失 snapshot、部分创建 CF、
  未注册目录、磁盘不足及 import 失败；不能只验证一次正常 Close/Open。
- 测量 1M/10M、32/128 tenants 的 checkpoint/recovery 时间、应用及物理写入量、WAL 保留量、
  峰值磁盘/RSS、compaction/write stall，以及并发读写 p95/p99。
- 在相同 cache、召回率和 CPU 条件下确认 cached-query 不回退，并测出足以抵偿复杂度的实际收益。

**当前建议：保持双引擎作为交付版本；先做独立的 state-only export/import 原型及故障矩阵，
再决定是否迁移。** 这是基于恢复正确性和实现成本的选择，不是认定单引擎必然更慢。

## 15. 多规模租户实验（2026-11-04 至 05，本轮完成）

### 15.1 对照定义与复现边界

本轮以已发布的 `b1818eec2909e2a5288f45d177d9abf20f0be624` 为实现基线；
后续改动保持本地，未提交或 push。实验输出、逐次命令、二进制和输入 SHA256、原始 CSV
保存在本次 session 的 `files/overnight-20261104/`。所有计算限定在 CPU 0–15，
保留至少 60 GiB 磁盘及 256 GiB 主机可用内存；实验进程的合计 RSS 上限为 64 GiB。
构建、GDB 采样和并发正确性诊断的耗时不混入受控 serving 吞吐表。
本机可见 CPU 为 AMD EPYC 7V12、96 logical CPUs；所用 CPU 0–15 属于 NUMA node 0
且对应不同 core。工具链为 GCC 11 / C++17、RocksDB 7.6。C1/C8 计时使用 CPU 0–7，
C16 另用 CPU 0–15；每个 native read run 预热 3 秒、测量 15 秒，各场景三轮。

需要区分三个对照：

- **tenant / direct**：同一批 head、graph、postings、命名空间和共享 cache。
  direct 绕过 `TenantIndexManager` 的查询路由与驻留 bookkeeping，但仍保留实例隔离 workspace。
  两者之间的差异用于估计 manager 的增量成本，包括参数校验、query copy、epoch 和 counters，
  不是纯锁耗时，也不代表共享存储的全部成本。
- **单租户 / private RocksDB**：复制同一个索引和逐字节相同的 postings，
  将 namespaced key 转为原来的二进制 posting ID key；先要求查询输出完全相同。
  对照 cache 均为 64 MiB，但原 private backend 的 blob 布局与共享 backend 的 SST 布局不同；
  该差异属于端到端实现比较，不能全部归因于 tenant 路由。计时用的 private 对照保留已发布
  版本的 workspace 实现，不让它同时获得本轮 managed-workspace 改动。
- **公开 FILEIO/TiKV 报告**：固定到 Microsoft 分支 revision
  `0d69bb53d3e441679781c7ccee489409f0483a00` 的
  [Benchmarking.md](https://github.com/microsoft/SPTAG/blob/0d69bb53d3e441679781c7ccee489409f0483a00/docs/Benchmarking.md)。
  它使用 990K 初始数据加十轮 1K 插入、200 queries、K=5、16 search threads，
  并非本机相同二进制、CPU affinity、存储和测量协议的受控基线。

切分 tenant 会改变每次查询的搜索空间，因此不同 tenant 数之间的 QPS 变化不能直接称为
tenancy overhead。精确 private-DB 对照只覆盖 1M/1；多 tenant 的 direct 对照用于分离
manager 成本，不冒充“128 个独立 private DB、相同总资源”的实验。

公开报告最终 1M 的两次查询轮次为：

| Backend | Recall@5 | QPS（两轮） | p95 ms（两轮） |
|---|---:|---:|---:|
| FILEIO | 98.6% | 1458.747 / 1411.742 | 15.570 / 15.652 |
| TiKV | 98.2% | 1086.154 / 1081.841 | 16.462 / 16.999 |

不能把低召回率配置的高 QPS 与这两行直接相除，再声称是 tenancy overhead 或速度提升。

### 15.2 数据、流量与准确性检查

数据为 SIFT1B 的前 10M UInt8/128D 向量，使用平方 L2；下载范围经过 HTTP 206、
Content-Range 和长度验证，前 1M 与此前公开输入的 SHA256 一致。采用 seed `20261104`、
Zipf exponent 1.15 的确定性分区，每个 case 内 tenant 数据互不重叠，每 tenant 10K–1M。

实验矩阵为 **1M × 1/8/32 tenants、2M × 128 tenants、10M × 32/128 tenants**。
128 tenants 即使全部取 10K，也已需要 1.28M；因此小规模的 128-tenant case 明确是 2M，
不标作 1M。10M 不使用单租户，以遵守每 tenant 的 1M 上限。
benchmark 显式设置 `maxTenants=128`，不修改生产默认 admission limits。

| Case | 每 tenant 最小 / 中位 / 最大向量数 |
|---|---:|
| 1M / 1 | 1,000,000 / 1,000,000 / 1,000,000 |
| 1M / 8 | 44,800 / 78,488 / 390,312 |
| 1M / 32 | 13,828 / 18,210.5 / 216,071 |
| 2M / 128 | 10,672 / 11,480 / 188,374 |
| 10M / 32 | 89,585 / 180,668.5 / 1,000,000 |
| 10M / 128 | 19,832 / 31,628 / 1,000,000 |

六个 fixture 合计 25M case-vectors，但它们重复使用同一个前 10M 数据源，
不是 25M 不同原始向量。构建阶段部分任务相互重叠，且后三组使用诊断性的 8 builder threads；
构建耗时不作为与公开报告或不同 case 的受控速度比较。

准确性使用公共 query 集的前 200 条、每个 tenant 的 exact top-5 truth，分别计算严格 ID recall
和公开报告采用的距离容差 recall。逐条验证返回 VID 属于该 tenant、无重复，以及返回距离等于
该 tenant 原始向量的精确 L2，不能只比较全局平均 recall。steady-state serving 使用完整
10K public queries；因此与公开报告的 200-query 短轮次还有工作集和测量窗口差别。

均匀流量与 hot 流量分开；hot 固定为 70% 请求访问最大的 tenant，其余均分。
全驻留与约四分之一驻留槽位的压力场景分开。warmup 和 measured 阶段复用同一组 worker/TLS，
全驻留场景先加载全部 tenant；各轮交换 tenant/direct 顺序，保留每 tenant 的 p50/p95/p99、
错误数、CPU、RSS 和 cache counters。snapshot-byte 目标不是进程 RSS 硬限额。

本轮测的是原生 C++ API，不包含 Python/TCP/网络序列化成本；不执行全局 drop-cache，
属于 warm steady-state 测量。计时运行将原生 stdout 诊断日志导向 `/dev/null`，
保留 stderr、结构化 CSV 和退出码，避免高 churn 下产生大量日志文件；
结果不包含业务日志落盘成本，但仍包含库内日志格式化、stdio 锁和 `/dev/null` 写入。
共享主机上的其他使用者仍可能造成波动，因此保留重复轮次。

### 15.3 已完成的校准与正确性诊断

全部六个 case 的默认及高预算 tenant/direct 输出逐字节一致；1M/1 精确 private-DB clone
在两种预算下也一致。全部 25M case-vectors 完成全量 posting 扫描：检查版本、VID 范围、
所有 live vector 的覆盖及当前版本的向量字节，避免将近似搜索漏召回误判为持久化丢失。

| Case | 默认 recall 宏平均 | 默认 tenant 范围 | 默认 hot70 加权 recall | 高预算 recall 宏平均 | 高预算 tenant 范围 |
|---|---:|---:|---:|---:|---:|
| 1M / 1 | 76.20% | 76.2% | 76.20% | 98.80% | 98.8% |
| 1M / 8 | 84.98% | 79.3–88.2% | 81.25% | 99.61% | 98.9–100% |
| 1M / 32 | 89.51% | 83.8–93.0% | 85.57% | 99.95% | 99.6–100% |
| 2M / 128 | 91.93% | 82.3–94.5% | 85.21% | 99.99% | 99.2–100% |
| 10M / 32 | 81.67% | 73.8–86.1% | 76.24% | 99.38% | 98.2–99.9% |
| 10M / 128 | 87.74% | 73.1–92.8% | 79.62% | 99.84% | 97.9–100% |

上述 recall 使用公开报告的距离容差，宏平均对每 tenant 等权；hot70 列按预定流量权重计算，
不是向量数加权。高预算为 8192/512，生产默认仍是 256/32。小 tenant 的高 recall 会掩盖
大 tenant 的低 recall，因此同时列出范围及 hot70，不只给一个全局平均。

并发写入及恢复使用独立 fixture 副本，4 writer、8 reader，并发周期 checkpoint；
新增输入取源数据最后 50K，与这里的 1M/2M 基础数据不重叠。正常运行重开前后的查询完全一致。

| 诊断场景 | resident slots | 已确认插入 | 已确认删除 | 恢复后全量验证 live vectors |
|---|---:|---:|---:|---:|
| 1M / 8，正常完成 | 8 | 8,192 | 8 | 1,008,184 |
| 1M / 8，写入中 SIGKILL | 2 | 4,096 | 8 | 1,004,088 |
| 2M / 128，正常完成 | 32 | 8,192 | 128 | 2,008,064 |
| 2M / 128，写入中 SIGKILL | 32 | 4,096 | 128 | 2,003,968 |

四组均通过逐 VID、版本和原始 payload 验证；两次崩溃恰好未留下额外未确认插入。
验证器也要求任何恢复出的未确认 VID 对应这个 tenant 已记录但尚未消费的 insertion attempt，
而不是仅验证其 VID 小于 recovered count。该分支不能声称已被这两次实际 crash 覆盖。
这里只验证进程崩溃，不是掉电持久性，也不是所有 crash boundary 的穷举。
这些诊断与构建有重叠，journal 也带来额外开销，不能作为受控写吞吐比较。

单独保留的历史 1M fixture 上，查询预算校准如下；这些是配置诊断，**未修改生产默认值**：

| MaxCheck / SearchInternalResultNum | Recall@5 |
|---|---:|
| 256 / 32（当前 managed 默认） | 76.2% |
| 4096 / 64 | 87.0% |
| 8192 / 128 | 92.4% |
| 8192 / 256 | 96.2% |
| 8192 / 384 | 98.1% |
| 8192 / 512 | 98.8% |

高召回对照采用 8192/512，仍需逐个新 fixture 报告实际 recall，不能将这一个 fixture 的
98.8% 套用到其他 tenant。也不声称与公开配置完全一致：head 构建、replica 参数、
BufferLength 等仍有差别，且当前 dynamic searcher 会重算 search posting page limit。

### 15.4 128-tenant profiling 与候选优化

两组 GDB capture 各取 40 次快照、8 个 query worker，共 320 个 query-thread stack。
这些是受 debugger 干扰的离散调用栈，不是精确 CPU-time 占比，也不用其 QPS 作性能结论。

| 场景 | 调用栈分类 |
|---|---|
| 2M/128、全部驻留、均匀 C8 | workspace 初始化/重置 156；RocksDB 118；其余搜索 46 |
| 2M/128、32 槽位、均匀 C8 | index load 262；eviction 32；workspace 18；Acquire 6；RocksDB 2 |

压力场景指向索引重建/线程创建、析构及加载日志，而不是单一 manager LRU mutex 占主导。
其中 `clone3/pthread_create` 会受到 ptrace 特别明显的影响，不能将这些采样次数解释为原生
线程创建耗时比例。首次压力 capture 因大量 inferior 输出经 GDB 转发而超时，已保留失败记录；
重试将 inferior 输出重定向到 `/dev/null`，完整采样成功。

全驻留时，当前 64-slot TLS 上限会在 128 tenants 均匀访问中重建部分 workspace。
BKT 还会按较大的 refine budget 分配 heap/hash，普通查询却只用较小预算。
正在对照两个独立原型：按实际 posting 长度申请 shared-store buffer；
以及保持原有 heap 容量和哈希行为、仅延迟 heap 元素初始化并清理实际访问过的 hash slot。
后者不通过缩小搜索预算或提高 TLS 上限获得收益。原型已通过碰撞/扩容/重置检查及
跨规模查询逐字节一致性检查。

三个候选采用轮换顺序，各 3 轮、每轮 3 s warmup + 15 s measurement，共 54 个独立进程；
计时期间没有其他本次任务的构建、扫描或 profiling。以下为各指标分别取中位数：

| 场景 | 基线 QPS | sparse heap/hash QPS | QPS 变化 | 基线 / 优化 p99 ms |
|---|---:|---:|---:|---:|
| 1M/1 C8，默认预算 | 24,241 | 25,522 | +5.3% | 0.403 / 0.382 |
| 1M/32 C8，均匀 | 15,909 | 29,469 | +85.2% | 0.735 / 0.422 |
| 2M/128 C8，均匀 | 7,857 | 21,587 | +174.7% | 1.745 / 0.553 |
| 2M/128 C8，hot70 | 16,866 | 29,413 | +74.4% | 1.475 / 0.555 |
| 2M/128 C1，均匀 | 2,298 | 3,568 | +55.2% | 0.671 / 0.414 |
| 1M/1 C8，8192/512 高预算 | 1,641 | 1,630 | −0.7% | 5.421 / 5.468 |

例如 2M/128 C8 均匀场景，基线三轮为 7,878 / 7,849 / 7,857 QPS，优化为
21,636 / 21,409 / 21,587；进程 peak RSS 中位数从约 3.38 GiB 降到 2.02 GiB。
高预算场景的优化三轮为 1,665 / 1,621 / 1,630 QPS，不能声称它也有明显加速。
全部运行无查询错误。原始记录位于 `runs/candidate-timings/`。

独立的 lazy posting-buffer 原型仅在 128-tenant 均匀 C8 下约 +1.6%，其他场景约
−1.0% 至 +0.6%，且部分场景 RSS 上升；暂不合入它。

基于上述结果，已将 heap 延迟初始化和 visited-table 稀疏清理接入本地源码；
只对 instance-managed BKT workspace 启用，保持原有 heap 容量、搜索预算、64-slot TLS 上限、
非复制语义以及 legacy/KDT 的默认初始化路径。主性能矩阵仍待完成。
早期原型只用于普通 query 路径选择；最终构建必须重编所有 header 依赖，包括 iterator 实现，
不能仅替换 BKT object 后便声称所有旧 API 都已验证。

本地实现已通过 Release 与 ASAN/LSAN 的 28 个 multi-tenant cases、16,427 个 assertions；
冻结版本在六种规模、两种查询预算下的输出均与原实现逐字节一致。另对 1M/1 的完整 10K
queries 检查高预算 baseline / 优化 / private 输出，一致；这不是对全部 10K queries 做了
新的 brute-force ground truth。legacy BKT、KDT 和 BKT iterator 检查也已通过。
ASAN 首次在进入测试前因本机
unlimited stack 导致 shadow mapping 冲突而退出；仅对测试子进程设置 8 MiB stack 后通过，
没有改变主机级配置。

针对换入时创建 maintenance threads 的第二个原型：仅对 managed load 延迟启动
BKT/SPDK worker，到第一次提交维护任务时启动，不取消维护任务，也不在 cached query 上加锁。
关闭时将 abort 谓词更新放到 queue mutex 内，遵循 condition-variable 的同步规则。
它已通过 200 轮并发首次提交/排空/关闭、128 tenants/32 slots 的 8,192 次插入和 128 次删除、
压力场景查询输出一致性，以及“Insert 完成、记录 ACK 之前”SIGKILL 的恢复检查。
最后一项实际恢复了 4,096 个已记录 ACK 的插入及 1 个未记录 ACK 的插入，逐向量 payload 验证通过，
补充覆盖了前两次 crash 未触发的 unacknowledged 分支。首次写入可能承担推迟的启动成本，
需要和读取收益一起评估。完整压力对照完成后，已将该实现接入本地源码；
只改 managed load，legacy 和 build/refill 路径保持 eager。最终 Release 与 ASAN/LSAN
均通过 30 cases、17,101 assertions；最终规模输出和恢复复验也已完成。尚未 commit/push。

### 15.5 换入换出压力下的关闭竞态

首次 worker 压力比较在完成 14 个 native runs 后停止：workspace-only 的
1M/32、8 resident slots、C8 hot70 运行超过 180 s。其 warmup 已完成且无查询错误，
measurement 未完成；不能将该次或未完成的三轮比较算作成功实验。
同一场景随后独立重复 10 次均完成，尚未直接捕获原超时进程的栈，因此不把它的根因
直接归结为某一把锁。

进一步对旧 `ThreadPool` 做快速创建/关闭测试，在约 35K 轮后稳定停滞到 60 s watchdog。
对该自有子进程的 GDB capture 显示：主线程停在 `join()`，剩余 worker 停在
condition-variable wait。这个最小程序没有 tenant manager、LRU 或 RocksDB。
析构函数先在 queue mutex 外设置 abort，再 notify；worker 可以在检查旧谓词之后、
真正进入 wait 之前错过通知。atomic abort 本身并不能避免这种 lost wakeup。

修复将停止谓词更新置于同一个 queue mutex 内，再释放锁并 notify/join。
这是原有线程池的关闭同步修复，不是增加 cached-query 全局锁，也不依赖延迟启动。
修复版本已完成同一最小程序的 100K 轮快速启动/关闭；加入了启动后及任务排空后的关闭
回归用例，完整 Release/ASAN 回归也已通过。完成的 120-run 压力比较以修复后的
eager worker 为对照，不将这次关闭修复混算为 lazy-worker 的性能收益。
保留原始失败、最小复现和后续验证记录，避免把修复 liveness 与读取性能收益混为一谈。

### 15.6 有限驻留容量下的完整对照

五种 multi-tenant 规模，驻留槽位为 tenant 数的 1/4；C1/C8、uniform/hot70、
两种实现、各三轮，共 120 个 native runs，全部完成且无查询错误或超时。
每轮 3 s warmup + 15 s measurement，轮换实现顺序，期间无其他本次任务的计算作业。
对照组为 **workspace 优化 + 关闭同步修复**，不是未优化的原始版本；
实验组仅额外启用 deferred maintenance workers。下表均为三轮中位数：

| 规模 | 并发 | 流量 | eager / deferred QPS | QPS 变化 | eager / deferred p99 ms |
|---|---:|---|---:|---:|---:|
| 1M/8 | 1 | uniform | 116.6 / 123.6 | +6.0% | 34.89 / 33.52 |
| 1M/8 | 1 | hot70 | 275.7 / 289.0 | +4.8% | 33.97 / 33.16 |
| 1M/8 | 8 | uniform | 450.8 / 500.8 | +11.1% | 69.31 / 58.01 |
| 1M/8 | 8 | hot70 | 757.1 / 809.8 | +7.0% | 47.44 / 43.70 |
| 1M/32 | 1 | uniform | 339.5 / 373.6 | +10.0% | 18.77 / 18.04 |
| 1M/32 | 1 | hot70 | 1012.8 / 1090.7 | +7.7% | 6.28 / 6.00 |
| 1M/32 | 8 | uniform | 1506.0 / 1771.6 | +17.6% | 19.69 / 18.58 |
| 1M/32 | 8 | hot70 | 2160.1 / 2436.7 | +12.8% | 19.25 / 17.96 |
| 2M/128 | 1 | uniform | 472.2 / 527.1 | +11.6% | 7.78 / 7.89 |
| 2M/128 | 1 | hot70 | 1283.2 / 1420.4 | +10.7% | 3.78 / 3.44 |
| 2M/128 | 8 | uniform | 1578.1 / 1967.1 | +24.6% | 13.65 / 10.25 |
| 2M/128 | 8 | hot70 | 4696.3 / 6190.2 | +31.8% | 10.66 / 6.54 |
| 10M/32 | 1 | uniform | 48.9 / 50.7 | +3.8% | 86.12 / 84.20 |
| 10M/32 | 1 | hot70 | 175.5 / 178.1 | +1.5% | 83.02 / 81.62 |
| 10M/32 | 8 | uniform | 351.7 / 382.1 | +8.6% | 89.90 / 86.46 |
| 10M/32 | 8 | hot70 | 494.7 / 545.7 | +10.3% | 89.19 / 86.54 |
| 10M/128 | 1 | uniform | 178.3 / 186.6 | +4.6% | 81.52 / 81.57 |
| 10M/128 | 1 | hot70 | 573.0 / 596.7 | +4.1% | 20.61 / 20.22 |
| 10M/128 | 8 | uniform | 998.4 / 1221.8 | +22.4% | 76.41 / 67.19 |
| 10M/128 | 8 | hot70 | 3073.7 / 3838.5 | +24.9% | 25.18 / 21.47 |

例如 2M/128 C8 hot70，同轮配对的 QPS 提升范围为 +31.39% 至 +32.52%；
10M/128 C8 uniform 为 +19.30% 至 +23.61%。较小的百分比不作统计显著性结论，
也不声称所有场景的尾延迟或 RSS 均下降。
2M/128 C8 hot70 的 CPU 使用从约 5.05 提升到 6.76 cores，
peak RSS 从 0.695 到 0.689 GiB；10M/128 C8 uniform 则从 6.10 到 7.04 cores、
0.958 到 0.991 GiB。改善主要体现为吞吐/并行利用率，不是普遍节省驻留内存。
这些结果支持消除只读换入时不必要的 worker 创建，但不证明 LRU 锁在任意并发规模都不会争用。

原始结果及逐轮 QPS、CPU、RSS、warmup 后 cache-counter 增量位于
`runs/worker-pressure-fixed/{summary,analysis}.json`。
此处仍是默认查询预算，recall 见 §15.3；不能把高预算 recall 与此表吞吐拼接。

### 15.7 最终实现的正确性复验

冻结的 `final-config-v3` 及其 `production/` 保留最终源码对应的二进制和 SHA256。
六种规模在默认、高预算下均以有限 resident slots 重新查询，12 组输出与原实现的已验证
输出逐字节一致。1M/1 完整 10K queries 的高预算 tenant/direct/private 输出也与基线一致；
legacy BKT、KDT、BKT iterator 复验通过。

| 最终实现诊断 | resident slots | ACK 插入 / 删除 | 验证到的未 ACK 插入 | 全量验证 live vectors |
|---|---:|---:|---:|---:|
| 2M/128，并发读写、周期 checkpoint、正常重开 | 32 | 8,192 / 128 | 0 | 2,008,064 |
| 1M/8，Insert 完成、ACK journal 写入前 SIGKILL | 2 | 4,096 / 8 | 1 | 1,004,089 |

两组均通过逐 VID、版本和原始 payload 的 posting 扫描。正常重开前后查询一致，
返回向量的距离重新计算通过。记录在 `runs/final-validation/`、`runs/final-mixed128/`
和 `runs/final-crash8-unack/`；这些复验有资源隔离的并行工作，不能据此比较写入吞吐。
这不覆盖掉电、全部 crash boundary、网络端到端行为，也不构成对所有并发竞态的证明。

### 15.8 最终默认预算、全部驻留：manager 增量

最终版本完成 132 个 native runs，六种规模、C1/C8、uniform/hot70、tenant/direct 各三轮。
全部成功，计时阶段的 tenant cache miss/load/eviction 增量均为零。
以下 QPS 和 p99 各自取三轮中位数；差异定义为 `(tenant QPS / direct QPS - 1) × 100%`：

| 规模 | 并发 | 流量 | tenant / direct QPS | QPS 差异 | tenant / direct p99 ms |
|---|---:|---|---:|---:|---:|
| 1M/1 | 1 | uniform | 3670.8 / 3656.9 | +0.38% | 0.356 / 0.350 |
| 1M/1 | 8 | uniform | 25403.3 / 25679.2 | −1.07% | 0.381 / 0.378 |
| 1M/8 | 1 | uniform | 4125.2 / 4117.4 | +0.19% | 0.372 / 0.371 |
| 1M/8 | 1 | hot70 | 4003.7 / 4001.3 | +0.06% | 0.365 / 0.360 |
| 1M/8 | 8 | uniform | 27919.7 / 28013.7 | −0.34% | 0.422 / 0.425 |
| 1M/8 | 8 | hot70 | 27329.5 / 27397.5 | −0.25% | 0.407 / 0.408 |
| 1M/32 | 1 | uniform | 4276.5 / 4284.2 | −0.18% | 0.380 / 0.379 |
| 1M/32 | 1 | hot70 | 4805.2 / 4832.2 | −0.56% | 0.340 / 0.340 |
| 1M/32 | 8 | uniform | 29033.4 / 29000.9 | +0.11% | 0.425 / 0.431 |
| 1M/32 | 8 | hot70 | 32622.2 / 32722.1 | −0.31% | 0.382 / 0.381 |
| 2M/128 | 1 | uniform | 3553.8 / 3506.0 | +1.36% | 0.426 / 0.428 |
| 2M/128 | 1 | hot70 | 4613.1 / 4629.1 | −0.35% | 0.388 / 0.389 |
| 2M/128 | 8 | uniform | 21630.3 / 21275.7 | +1.67% | 0.553 / 0.556 |
| 2M/128 | 8 | hot70 | 29232.7 / 29337.3 | −0.36% | 0.556 / 0.560 |
| 10M/32 | 1 | uniform | 2813.0 / 2790.1 | +0.82% | 0.529 / 0.513 |
| 10M/32 | 1 | hot70 | 3073.7 / 3105.8 | −1.03% | 0.545 / 0.503 |
| 10M/32 | 8 | uniform | 19617.4 / 19537.3 | +0.41% | 0.551 / 0.544 |
| 10M/32 | 8 | hot70 | 21605.2 / 21675.3 | −0.32% | 0.526 / 0.525 |
| 10M/128 | 1 | uniform | 2995.9 / 2985.3 | +0.36% | 0.561 / 0.563 |
| 10M/128 | 1 | hot70 | 3097.2 / 3126.6 | −0.94% | 0.522 / 0.514 |
| 10M/128 | 8 | uniform | 18228.5 / 18196.5 | +0.18% | 0.690 / 0.683 |
| 10M/128 | 8 | hot70 | 20065.9 / 20231.6 | −0.82% | 0.645 / 0.646 |

QPS 差异为 −1.07% 至 +1.67%，没有表现出明显的 manager 串行化吞吐损失；
小幅正值不解释为 manager 本身带来加速，也不能把这些差异当作严格的统计上界。
各场景三轮 QPS 的 `(最大值 - 最小值) / 中位数`，tenant 为 0.18%–5.38%，
direct 为 0.26%–3.20%；不能把百分之一量级的差异全部解释为实现因果。
尾延迟仍需单独看，例如 10M/32 C1 hot70 的 p99 为 0.545 / 0.503 ms，
不能仅用 QPS 接近就声称延迟完全相同。

最终 tenant 的 C8 uniform 进程资源如下，CPU 均约 7.98–7.99 cores：

| 规模 | peak RSS GiB | C8 / C1 QPS |
|---|---:|---:|
| 1M/1 | 0.179 | 6.92× |
| 1M/8 | 0.355 | 6.77× |
| 1M/32 | 0.955 | 6.79× |
| 2M/128 | 1.999 | 6.09× |
| 10M/32 | 1.344 | 6.97× |
| 10M/128 | 2.327 | 6.08× |

RSS 包含 index、RocksDB、测试进程和 thread-local workspaces，不是纯 index 大小；
OS page cache 不包含在此 RSS 内。全驻留并不表示每个 tenant 的 query workspace 都能驻留在
64-slot TLS cache 中。与 §15.6 相比，容量压力带来的吞吐下降远大于这里的 manager 增量：
例如 2M/128 C8 uniform 从全驻留 21,630 降至 32 slots 的 1,967 QPS。
这是本次配置下的换入成本，不能被“cached 路由开销小”掩盖。
原始逐轮、每 tenant 结果见 `runs/final-resident/{summary,analysis}.json`。

### 15.9 最终实现与原 private RocksDB：端到端差异

1M/1、同一 head/graph/version/posting 内容、64 MiB block-cache budget、相同查询预算；
private 使用原版本实现，tenant/direct 使用最终版本。36 个运行全部成功。
以下为三轮中位数，负差异表示 tenant QPS 低于 private：

| 预算 / Recall@5 | 并发 | tenant / direct / private QPS | tenant 相对 private | tenant / private p99 ms |
|---|---:|---:|---:|---:|
| 256/32 / 76.2% | 1 | 3604.7 / 3630.5 / 4241.1 | −15.01% | 0.362 / 0.277 |
| 256/32 / 76.2% | 8 | 24993.3 / 25089.5 / 29731.4 | −15.94% | 0.387 / 0.316 |
| 8192/512 / 98.8% | 1 | 254.8 / 255.6 / 316.0 | −19.37% | 4.797 / 3.378 |
| 8192/512 / 98.8% | 8 | 1599.2 / 1600.4 / 1808.7 | −11.58% | 5.538 / 4.771 |

这些差距明显大于 tenant/direct 差距，不能报告为“全部 tenancy 开销约 1%”。
例如默认 C8，tenant 三轮为 25,018 / 24,993 / 24,836 QPS，private 为
29,614 / 29,754 / 29,731，差异并非仅由单轮波动产生。
高预算 C8 的 tenant/private peak RSS 为 0.230 / 0.182 GiB。

此表同时包含共享存储路径、namespace key、SST/blob 布局、压缩/缓存行为及实现差异，
尚不能将其全部归因于 namespace 编码；相同 block-cache 上限也不等于相同有效缓存覆盖率。
独立 RocksDB 计数器和更大 cache 的对照见 §15.13，不改变当前 cache 默认配置。
原始记录见 `runs/final-private/{summary,analysis}.json`。

### 15.10 高召回预算、全部驻留

另行完成 66 个运行：全部使用 8192/512、C8、64 MiB cache；每组 tenant/direct 各三轮。
全部无查询错误，所有 tenant 运行在测量阶段保持全驻留且无 miss/load/eviction。
这组诊断配置不修改生产默认的 256/32；召回率仍以 §15.3 的前 200 条 exact truth 为准。
当前 tenant 管理 API 没有暴露这两个搜索预算参数；8192/512 由隔离诊断构建覆盖，
不能将表中高召回结果当成现有远程 API 的默认质量保证。

| 规模 | 流量 | tenant / direct QPS | QPS 差异 | tenant / direct p99 ms | tenant peak RSS GiB |
|---|---|---:|---:|---:|---:|
| 1M/1 | uniform | 1593.1 / 1594.3 | −0.08% | 5.567 / 5.568 | 0.232 |
| 1M/8 | uniform | 1956.5 / 1951.5 | +0.26% | 5.502 / 5.532 | 0.772 |
| 1M/8 | hot70 | 1832.5 / 1833.9 | −0.08% | 5.122 / 5.128 | 0.738 |
| 1M/32 | uniform | 2228.1 / 2229.7 | −0.07% | 5.228 / 5.242 | 2.364 |
| 1M/32 | hot70 | 2251.2 / 2250.3 | +0.04% | 4.787 / 4.707 | 2.282 |
| 2M/128 | uniform | 1953.3 / 1950.3 | +0.15% | 5.793 / 5.799 | 4.728 |
| 2M/128 | hot70 | 2291.8 / 2290.7 | +0.05% | 4.998 / 4.975 | 4.691 |
| 10M/32 | uniform | 1577.3 / 1569.1 | +0.52% | 6.215 / 6.241 | 2.776 |
| 10M/32 | hot70 | 1541.1 / 1543.2 | −0.14% | 6.011 / 5.956 | 2.684 |
| 10M/128 | uniform | 1674.5 / 1654.4 | +1.22% | 6.862 / 6.842 | 5.106 |
| 10M/128 | hot70 | 1573.4 / 1576.9 | −0.22% | 6.132 / 6.096 | 5.043 |

高预算显著增加搜索工作和 workspace RSS，不能把默认预算的 QPS 与高预算召回率拼在一起。
例如 1M/1 默认召回 76.2%、约 25.4K QPS，而本表 98.8% recall 对应约 1.59K QPS。
此外，2M/128 hot70 的 tenant 三轮为 2054.3 / 2304.3 / 2291.8 QPS，
direct 为 2061.2 / 2292.5 / 2290.7；首轮两者同步偏低，未删除该轮。
此场景 tenant 跨轮范围为中位数的 10.91%，其余场景为 0.21%–1.07%。
原始记录见 `runs/final-high-resident/{summary,analysis}.json`。

### 15.11 C16 对照与公开报告的定位

单独在 CPU 0–15 执行 18 个运行：1M/1、C16、两种预算、三个 backend，各三轮。
全部成功；下表 cache 为 64 MiB：

| 预算 | tenant / direct / private QPS | tenant 相对 private | tenant / private p95 ms | tenant / private p99 ms |
|---|---:|---:|---:|---:|
| 256/32 | 46599.1 / 46782.5 / 55258.9 | −15.67% | 0.392 / 0.325 | 0.415 / 0.344 |
| 8192/512 | 2853.7 / 2884.0 / 3262.1 | −12.52% | 6.029 / 5.276 | 6.249 / 5.445 |

高预算 tenant recall 为 98.8%，进程 peak RSS 中位数约 0.320 GiB，平均使用约 15.95 cores。
可与 §15.1 中 FILEIO 的 98.6%、1412–1459 QPS、p95 15.57–15.65 ms，
以及 TiKV 的 98.2%、1082–1086 QPS、p95 16.46–17.00 ms 并列参考。
但不能据此宣称 multi-tenant 比旧系统快若干倍：硬件、revision、数据排列、图、
搜索参数、cache、query 工作集、插入历史和测量方法均不相同。
**本机的可靠端到端结论仍是同索引 private RocksDB 对照中的差距，而非跨机器比值。**
原始记录见 `runs/final-private-c16/{summary,analysis}.json`。

### 15.12 最终版本相对本轮已发布基线的复核

最终版本冻结后，再与本轮开始时的已发布版本做 12 个交错运行：
2M/128、128 slots、C8、默认预算、uniform/hot70、各三轮。
这是最终整体改动的复核，不是用候选实验的最佳一轮代替最终结果。

| 流量 | 基线 / 最终 QPS | 改善 | 基线 / 最终 p99 ms | 基线 / 最终 peak RSS GiB |
|---|---:|---:|---:|---:|
| uniform | 7818.5 / 21126.2 | +170.21% | 1.751 / 0.564 | 3.384 / 1.998 |
| hot70 | 16667.1 / 28916.7 | +73.50% | 1.493 / 0.567 | 3.388 / 1.982 |

uniform 的三次成对改善为 +170.21% / +171.60% / +169.41%，
hot70 为 +73.66% / +73.47% / +73.50%；两者 CPU 都约 7.98 cores。
有限驻留容量下的 deferred-worker 收益另见 §15.6；高预算并没有同等幅度的 workspace 收益。
所有计时运行均无查询错误，输出等价性由 §15.7 的独立最终验证覆盖。
原始记录见 `runs/final-cached-confirmation/{summary,analysis}.json`。

### 15.13 存储诊断与 cache 容量取舍

为解释 §15.9 的 private 差距，在同一线程执行 1000 条固定预热查询，
然后对 5000 条固定查询启用 RocksDB PerfContext/IOStats；分别覆盖两种预算、
tenant/direct/原 private，共六次诊断。计数器运行不进入吞吐表。
以下为 64 MiB cache 下的前台累计值；GB 使用十进制：

| 预算 | shared / private 返回的 posting payload GB | shared / private 读取字节 GB | shared / private checksum 秒 | shared / private output-files 计时秒 |
|---|---:|---:|---:|---:|
| 256/32 | 0.231 / 0.231 | 0.511 / 0.237 | 0.067 / 0.023 | 0.879 / 0.633 |
| 8192/512 | 3.483 / 3.483 | 8.296 / 3.575 | 1.110 / 0.355 | 11.790 / 9.735 |

tenant 与 direct 的访问计数完全相同。高预算 shared 记录 1,834,293 次 SST block read、
664,125 次 block-cache hit；private 的 SST 数据块在本次预热后没有记录 read，
但仍有 2,560,000 次 blob read。两者返回 payload 相同，shared 读取字节约为 private 的
2.32 倍，证明存在此配置下的存储读放大差异，而不只是 manager 路由。
这里的 read 包括 warm OS page cache 命中，**不是 SSD 实际读取量**；
计时可能嵌套，不能相加当作 CPU 百分比分解。本次记录也不足以把差距归结为已证实的解压瓶颈。

随后保持索引、二进制、查询预算不变，将两边的 block-cache 上限同时改为 256 MiB，
另做 18 个 C8 交错运行，各三轮：

| 预算 | tenant / direct / private QPS | tenant 相对 private | tenant / private p99 ms | tenant / private peak RSS GiB |
|---|---:|---:|---:|---:|
| 256/32 | 36761.3 / 36927.8 / 29679.7 | +23.86% | 0.271 / 0.317 | 0.394 / 0.125 |
| 8192/512 | 2246.1 / 2246.4 / 1803.3 | +24.55% | 3.943 / 4.772 | 0.435 / 0.181 |

相较 64 MiB 批次，tenant QPS 中位数分别增加约 47.1% / 40.5%，
private 基本不变。跨 cache 批次没有逐轮交错，因此这些百分比是两批中位数之比，
不是成对置信区间；每个 cache 批次内的三种 backend 则使用交错对照。
这个反转说明不能给出与 cache/layout 无关的固定“tenancy tax”，
也说明相同 cache **上限**并不等于相同实际内存使用：private 的 blob payload 走 OS cache，
而 shared 可以用更大的进程内数据块缓存换取 CPU/访问开销下降。

当前已经支持通过 `Limits.cacheBytes` 配置共享 cache，不需要为此新增查询路径锁或改存储格式。
**生产默认仍为 64 MiB**；256 MiB 是本次 1M/1 的有条件容量建议，不推广为所有 tenant 规模的默认值。
下一步存储优化应比较实际总内存和 warm/cold 工作集，再评估 SST block/blob 布局，
而不是仅因看到 shared/private 差距就继续修改 manager 全局锁。
原始证据见 `runs/storage-profile/` 与 `runs/final-private-cache256/`。

256 MiB 的后续相同查询诊断进一步确认 cache 效果：高预算 shared 的 block read 从
1,834,293 降到 76,401，读取字节从 8.296 降到 0.336 GB，
output-files 计时从 11.790 降到 6.083 秒；private 仍读取 3.575 GB blob 数据。
固定 1000 条预热并不保证大 cache 已完全热，因此不要求 read count 为零。
三种 backend 的默认预算 200 queries 和高预算完整 10K queries 输出也逐字节匹配
64 MiB 参考输出。记录见 `runs/storage-profile-cache256/`、`runs/cache256-validation/`。

### 15.14 受控混合读写与新发现的压力瓶颈

在 CPU 0–7 顺序执行 12 个运行，4 writers + 8 readers，uniform routes，
默认搜索预算、64 MiB block cache。对照是已有 workspace 优化和关闭竞态修复、
但 eager maintenance workers 的版本；候选增加 deferred workers。
1M/8 使用全部 8 slots、每 tenant 插入 4096 条；2M/128 使用 32 slots、
每 tenant 插入 64 条。两者各三轮、轮换顺序。

插入计时包含调用方 ledger flush、删除和后台 checkpoint 干扰，不含最后的 checkpoint、
重开与全量核验。checkpointer 每次返回后等待约 250 ms 再调用，**不是保证每 250 ms 完成一次**。
reader 计数包含写入计时外的启动/收尾，不能把它除以 mutation_seconds 作为精确 read QPS。

| 规模 / slots | eager / deferred insert ACK/s | 中位数差异 | eager / deferred read p99 ms | 更新后 macro Recall@5 |
|---|---:|---:|---:|---:|
| 1M/8 / 8 | 2950.2 / 3048.1 | +3.32% | 3.471 / 3.494 | 84.9875% |
| 2M/128 / 32 | 157.2 / 155.7 | −0.93% | 347.229 / 419.040 | 91.9406% |

1M/8 的成对插入速率变化为 +17.69% / +46.41% / −30.05%，有 3.57–3.87 秒的 checkpoint
调用离群值，不能宣称稳定写入加速。2M/128 的插入速率接近，但 deferred 的三轮 read p99
为 451.015 / 369.677 / 419.040 ms，高于 eager 的 345.314 / 360.403 / 347.229 ms；
**只读换入收益并不等于混合压力下所有尾延迟都改善。**
每轮 128-tenant case 仅完成一次后台 checkpoint 调用，调用耗时约 52.8–53.5 秒。

全部 12 轮无 API/query 错误，共 245,760 个 insert ACK；重开前后查询一致，
每轮都验证了完整 posting payload、VID/version 和 live-vector 覆盖。
更新后 truth 从原始向量、实际插入 VID/payload 和删除集合重新计算，
删除命中原 top-5 时重扫原始集合；不是沿用插入前 truth 来计算 recall。
插入数据来自不属于这些 1M/2M fixture 的预留 50K 向量。
两种实现每轮的 macro recall 一致。原始记录见 `runs/final-mixed-timings/`。

随后只对 owned child 做 40 次 GDB 诊断快照，结果不用于吞吐表：

- 40/40 个 checkpoint-thread 样本等待 `m_updates` 的 write lock；
  所以 53 秒是 **Checkpoint 调用总耗时**，不能说成 53 秒 stop-the-world。
- 320 个 Search-thread 样本中，178 个在 dirty eviction 的 `Save` 内等待
  RocksDB write thread；另有缓存状态等待和 load-config 日志调用。
  Insert 也出现 `DBImpl::DelayWrite` / `WriteThread::LinkOne`。
- 这说明混合压力下，cache miss 可能为了淘汰 dirty tenant 而受存储写入 backpressure 阻塞。
  样本不是 CPU 百分比，也没有证明这次尾延迟差异全部由 deferred startup 引起。

这些证据不支持把主要问题简单归结为 LRU mutex；它们指向 dirty snapshot 写回、
共享 State DB 的写入停顿，以及 checkpoint 的公平进入问题。
针对其中可有界验证的一项，另行隔离评估了只增大 `head_snapshots` CF memtable 的候选，
不修改 posting/recovery 参数、cache 预算、搜索预算、文件格式或 cached-query 锁路径。
诊断及完整数据复验见 `runs/mixed-pressure-profile/`。
首个隔离原型误用了不存在的 `snapshots` 名称，实际仍为原配置，其 12 轮结果保留在
`runs/snapshot-buffer-mixed/`，不作为调大 buffer 的有效证据。修正原型显式核验运行后
RocksDB OPTIONS 中 `head_snapshots.write_buffer_size`，防止仅以候选名称代替配置验证。

本轮仍没有相同 durability 语义的非 tenant 写入对照。公开报告的 FILEIO/TiKV 插入速率
约 164.8 / 66.1 vectors/s 使用单索引、不同参数且插入时没有查询，
不能与上表直接相除来推断写入 tenancy overhead。

### 15.15 快照 memtable 候选：有取舍，因此不合入默认配置

修正后的候选仅将共享 State DB 的 `head_snapshots` CF write buffer 从 4 MiB 改为 16 MiB，
其他 CF、搜索和 cache 配置保持不变；两边均已有 workspace、deferred-worker 和关闭竞态修复。
重新执行 12 个交错混合读写运行，**每轮均从实际 RocksDB OPTIONS 验证 4/16 MiB 生效**，
并完成 posting、更新后 exact truth、重开和错误计数检查。

| 规模 / slots | 4 / 16 MiB insert ACK/s | 中位数差异 | 4 / 16 MiB read p99 ms |
|---|---:|---:|---:|
| 1M/8 / 8 | 3991.5 / 2646.8 | −33.69% | 3.506 / 3.431 |
| 2M/128 / 32 | 156.7 / 183.7 | +17.21% | 398.844 / 169.663 |

128-tenant 压力场景三次插入速率改善为 +18.24% / +15.80% / +16.54%，
read p99 从 378.7–422.2 降至 123.2–222.4 ms，说明存储缓冲确实是可影响该瓶颈的因素。
但全驻留 8-tenant 的三次写入速率分别下降 33.69% / 31.12% / 23.32%；
不能只挑压力场景的收益就修改全局默认。checkpoint 在 128-tenant 场景仍约需 45 秒，
因此该候选也没有解决公平进入问题。

**决定：不合入 16 MiB 默认值，不新增未经需求确认的自动调节策略或配置 API。**
生产继续使用原 4 MiB write buffer；候选仅作为后续按负载调优的证据保留。
所有有效候选运行的 macro recall 与对应控制一致。记录见 `runs/head-buffer-mixed/`，
二进制和单文件改动冻结在 `snapshot-buffer-v2/`；它们不是当前生产构建。

### 15.16 本轮交付与尚未解决的问题

| 项目 | 本轮结论 |
|---|---|
| 已实施 | managed workspace 的稀疏清理与 heap 延迟构造；managed load 的 maintenance-worker 延迟启动；原 ThreadPool 关闭丢唤醒修复 |
| 验收证据 | 六个规模、25M case-vectors 全 posting 检查；最终输出一致性、Release/ASAN/LSAN、原 BKT/KDT/iterator；并发更新和 SIGKILL 恢复 |
| 常驻路由 | 默认预算的 22 个场景，tenant/direct QPS 中位数差异 −1.07% 至 +1.67%；不是全部 tenancy 成本，也不是严格置信上界 |
| 端到端读开销 | 64 MiB cache 的同索引 private 对照仍有约 11.6%–19.4% 差距；256 MiB 可反转，但实际 RSS 更高，不能隐去内存取舍 |
| 混合压力 | dirty eviction 的写入 backpressure、较大 read p99 和 checkpoint 进入等待仍存在；没有用不稳定的平均值掩盖它们 |
| 质量边界 | 1M/1 默认 Recall@5 为 76.2%；98.8% 属于高预算诊断，生产预算和 API 没有在本轮偷偷改变 |
| 单 DB | §14 是设计提案，未实施引擎合并、存储格式迁移或耐久性变更 |
| 发布 | 本轮源代码、工具和文档保持本地；未 commit/push，已发布基线不变 |

最终读吞吐阶段共 282 个成功 native runs，另有 120 个 worker-pressure 对照；
混合读写包括 12 个 eager/deferred 对照和 12 个有效 buffer 对照。
误配置原型的 12 个运行单独保留，不混入有效 buffer 证据。
这些数字不包含诊断插桩运行，也不把三次重复当成独立硬件复现。

后续优先处理 checkpoint 的有界进入与 dirty-writeback backpressure，
并明确生产搜索质量/预算配置需求；日志开销也是高 churn 下的可见成本。
尚无 cold-SSD、开放环路/TCP 排队、长期 tenant QoS、同耐久性 private 写入对照、
掉电恢复或全部故障边界的结论。自动 checkpoint 调度和 compute admission/fairness
仍按原范围保留，不把本轮结果描述为“所有 multi-tenant 优化已经完成”。

## 16. 对齐原 SPANN、收敛 multi-tenant 补丁（2026-11-06）

本节保留 11 月 6 日版本及 BKT 实验记录。11 月 9 日的 head 配置修复见 §17；下列数值未重新测量为 KDT，原二进制和实验材料保持不变。

### 16.1 撤回与保留

以原始 `cafdd0ab` 为行为参考，不把此前为提高 benchmark QPS 增加的通用优化混入 tenancy feature：

| 处理 | 具体范围 |
|---|---|
| 撤回 | visited table 稀疏清零、heap 延迟构造/vector backing；`Heap.h` 恢复为原始文件 |
| 撤回 | BKT/SPANN maintenance worker 延迟启动，恢复原 eager startup |
| 撤回 | 未使用的 KDT instance-workspace hook；KDT 头文件恢复为原始文件 |
| 缩小范围 | 严格 datatype/metadata/resultnum 解析仅启用于 tenant 服务模式；普通 legacy 查询保留原解析行为 |
| 保留：feature 必需 | 实例隔离 TLS 及回收、epoch/lease、实际 VID 预留与重放、请求级维护完成、在线只读 checkpoint 与错误传播 |
| 保留：新模块内部 | SharedRocksDB 的 PageBuffer 直接拷贝、TenantIndexManager 的 snapshot chunk 复用 |
| 单独审查的公共前置修复 | 已复现的 ThreadPool 关闭丢唤醒与一致 drain 检查、完整 socket 帧发送队列、客户端 callback/关闭生命周期保护 |

最后一行不应宣传为 tenancy 算法优化。后续组织 upstream 提交时，应将其作为独立 prerequisite，再提交最小 multi-tenant feature；此轮未自动创建或推送提交。原 `VersionLabel`、`Dataset` 和 private `ExtraRocksDBController.h` 不改，未引入通用并发重构。

### 16.2 存储与算法参数

共享 State DB 的 posting CF 复制原 `RocksDBIO(wal=false)` 配置：`IncreaseParallelism`、`OptimizeLevelStyleCompaction`、128 MiB target file、16 GiB level base、4 levels、16 subcompactions、16 MiB write buffer；启用 integrated blob（最小 64 B、8 GiB blob file、无压缩、GC age cutoff 0.4、round-robin compaction），以及 10-bit Bloom 和 filter-memory optimization。唯一需要替换的 merge operator 仍执行相同的 posting append，但键带 tenant/index namespace。

默认 block cache 恢复 3 GiB，两个共享引擎共用同一个对象；显式部署配置仍可覆盖。Recovery CF 使用原 `RocksDBIO(wal=true)` 的普通 DB/CF 默认值，而不是 posting/blob profile。新增加的 `head_snapshots`、`checkpoint_meta` 没有原 private DB 的对应 CF，保留 4 MiB write buffer / 2 buffers；这属于新存储模块设置，不声称所有 CF 都等同于 posting DB。RocksDB WAL 及进程崩溃持久性契约未变。

新建 tenant 不再覆盖原参数默认值，包括：

| 参数 | 对齐值 |
|---|---:|
| `MaxCheck / SearchInternalResultNum` | 4096 / 64 |
| `InternalResultNum / ReplicaCount / ReassignK` | 64 / 8 / 0 |
| `ExcludeHead / OneClusterCutMax / InPlace` | true / false / true |
| SelectHead ratio / thresholds / split factor | 0.2 / 6、25 / 5 |
| BKT k-means K / neighborhood size | 32 / 32 |
| SelectHead / SSD build / search / append threads | 4 / 16 / 2 / 1 |
| BKT head build threads | 1 |
| `DataBlockSize` | 1,048,576 |

表中为参数定义的默认值，原有 SelectHead 算法仍可在构建时自动调整 threshold。BKT head 构建恢复单线程默认，不为缩短本轮实验偷偷保留原先的参数覆盖；独立 fixture 可并行构建，但总 CPU 仍限制在 0–15。

**当时仍有的配置差异：** 11 月 6 日版本固定使用 BKT head（原 SPANN 默认 KDT）；这不是 tenancy 的必要条件，已在 §17 修复。目录和容量由 tenant 决定；注入共享 posting store、关闭旧应用恢复入口、查询触发 merge 和独立 async-append queue；`DeletePercentageForRefine=2` 防止在线保存重排 VID。不是声称所有设置与裸 SPANN 完全相同。已有索引的图/配置不会因重开自动重建，已有 SST 也不会因打开 blob option 自动全部迁移；本轮新结果必须来自重建的 fixture。

### 16.3 对齐时发现的必要正确性修复

恢复 `ExcludeHead=true` 后，原有“全部删除后重开”回归实际失败：查询返回了 3 个已删除的 head VID。原因是 SPANN 的 head 候选映射未检查全局 version/deletion map；此前强制 `ExcludeHead=false` 掩盖了这个问题。

最小修复只在注入 shared posting store 的 managed `SearchIndex` 路径过滤已删除 head 候选，保留普通 legacy 行为，不恢复全量 posting 的非原始配置。回归逐个删除 64 个 seed，并在每次删除后查询、最后 close/reopen；同时验证真实 blob checkpoint/reopen、legacy 宽松解析与 tenant 严格解析。完整 Release 与 ASAN/LSAN 通过 28 个用例、2,434 个断言；Python/TCP smoke 与原 BKT/KDT/iterator 通过。Python 动态加载本机 jemalloc 需要进程级 `LD_PRELOAD`，该运行环境问题未转成产品代码修改。

### 16.4 重测设置与证据目录

本轮数据和二进制隔离在 `/mnt/sptag-tenant-alignment-7de262b3-20261106`；旧实验不覆盖。根文件系统仅余约 38 GiB，因此新构建、临时文件与全部新 fixture 改用有约 782 GiB 空闲的 `/mnt`。同组 tenant/direct/private 均使用该文件系统；跨旧批次的绝对 QPS 还受到存储位置和算法参数变化影响。

复用此前公开 SIFT 的只读向量分区和 exact truth，但重新构建六组 1M/1、1M/8、1M/32、2M/128、10M/32、10M/128 索引。每组为 UInt8/128D、L2 top5、不等大小 tenant；总计 25M case-vectors，源数据仍只有前 10M 独立向量。正确性检查包含 head payload/VID map 与 posting，而不是错误地要求全部 live vectors 出现在 postings。

读性能采用相同已构建内容的 tenant/direct 对照；1M/1 额外复制相同 head/graph/version/postings 到原 private RocksDB 路径，使用同一份已回退公共优化的静态库。确认实际 blob 文件和持久化 OPTIONS，而非只对照代码。固定共享/私有 cache 为各 3 GiB、搜索预算 4096/64；warm OS cache，不清除系统缓存。完整 10K 查询用于吞吐，前 200 条逐 tenant exact truth 用于 Recall@5；private 对照另检查完整 10K 输出逐字节相等。

测量固定 CPU 0–7、nice 10；C8 是 8 个查询 worker，private 三路另测 C1。每轮预热 3 秒、测量 15 秒、三次轮换/交错重复；全驻留测 uniform/hot70，压力场景槽位为 tenant 数的四分之一。构建可用 CPU 0–15，但不得与受控计时重叠。资源护栏为 aggregate RSS 64 GiB、host available memory 256 GiB、新盘 free 60 GiB。恢复 ReplicaCount=8 后，新 fixture 连同 checkpoint/working 副本实占约 43 GiB；为容纳恢复临时副本和混合实验克隆，将本轮自行设置的新增磁盘估算上限从 50 调整为 80 GiB，当时仍有约 740 GiB 空闲，其他护栏不变。初始设置另行保存，不删除旧证据以腾空间。

六组已经全部重建并完成验证。10M/128 的第一次构建达到本轮 4 小时单任务上限时，已发布 107 个 tenant；通过正常恢复入口续建剩余 21 个，再完成全量检查，没有降低原算法参数来赶时间。构建用 BKT 单线程默认，较旧低参数配置明显更慢。

本轮累计 **114 个成功的受控读计时运行**：private 三路 18 轮、全驻留 66 轮、压力 30 轮；另有准确率、全向量检查、混合更新与恢复实验。只测 native C++ API，**不包含 TCP/Python 开销**。§15 的 +170.2%、76.2% recall，以及 SST/private-blob 的约 15% 差距仍是历史版本结果，不能重新标为当前版本。

### 16.5 同配置、同索引的 private 对照：旧约 15% 差距不再复现

**设置：总计 1M 向量、1 个 tenant、全部驻留、同一组 head/graph/version/postings、搜索预算 4096/64、双方各 3 GiB 配置 cache、同一 `/mnt` 文件系统。** `private` 使用原 `RocksDBIO` 路径；三路链接相同的当前静态库，不是将不同版本的公共 heap/search 代码混在一起。`direct` 保留共享 storage 和实例隔离 workspace，但绕过 TenantIndexManager。

| 并发 | tenant QPS | direct QPS | private QPS | tenant 相对 private 的吞吐差异 |
|---|---:|---:|---:|---:|
| C1 | 1,270.87 | 1,261.87 | 1,279.40 | −0.67% |
| C8 | 8,037.60 | 7,975.20 | 8,100.07 | −0.77% |

- C8 的 tenant/direct/private p99 分别为 **1.166 / 1.166 / 1.156 ms**，peak RSS 为 **0.211 / 0.174 / 0.155 GiB**。同样配置 cache 不等于同样实际内存；tenant 的两个 DB、恢复/快照和生命周期状态仍有成本。
- C8 三轮 QPS 范围：tenant **7,969–8,148**，private **7,968–8,129**，明显重叠。这里的约 0.8% 是本轮观测的中位数差异，**不是统计显著的固定开销，也不是严格的开销上界**。
- 三路完整 10K 查询、50K 个结果的输出逐字节一致；SHA-256 均为 `69741ae815946bc05eea8fda70844375380cde4fdf2026e8f2b6e62b12bf763b`。
- 原约 15% 比较混入了不同 storage profile。新同配置对照不再复现该差距，但本轮同时恢复了 cache、算法和公共 workspace 行为，**不能宣称旧差距全部由 blob 单项解释**。
- 此处 private 端到端对照只覆盖 1M/1，不外推为 128 个 private DB 的成本；跨硬件的 FILEIO/TiKV 数字仍不能用于计算 tenancy overhead。

证据：`runs/private-comparison/`、`runs/verify-first-five/effective-options.json`。实际 blob 文件及 23 项持久化 DB/CF/table options 已核对相同，包括 16 个 background jobs、16 个 subcompactions、16 MiB write buffer / 6 buffers、blob、Bloom 和 compaction 设置。

### 16.6 多规模全驻留与缓存压力

以下是独立的 tenant/direct 矩阵批次，不能把其中某行与 §16.5 的不同轮次混算差异。Recall 为前 200 个 query 的逐 tenant 距离容差 Recall@5，再对 tenant 等权平均；单 tenant 的搜索空间随分区改变。

| 总向量 / tenants | 单 tenant 向量范围 | C8 uniform tenant QPS | direct QPS | tenant p99 ms | tenant peak RSS GiB | macro Recall@5 |
|---|---:|---:|---:|---:|---:|---:|
| 1M / 1 | 1,000,000 | 8,046 | 7,940 | 1.157 | 0.211 | 97.700% |
| 1M / 8 | 44,800–390,312 | 7,453 | 7,385 | 1.560 | 0.652 | 99.450% |
| 1M / 32 | 13,828–216,071 | 8,186 | 8,112 | 1.504 | 2.121 | 99.863% |
| 2M / 128 | 10,672–188,374 | 4,877 | 4,870 | 2.571 | 4.426 | 99.917% |
| 10M / 32 | 89,585–1,000,000 | 6,402 | 6,376 | 1.732 | 3.165 | 99.006% |
| 10M / 128 | 19,832–1,000,000 | 4,655 | 4,662 | 2.960 | 5.369 | 99.634% |

多 tenant 全驻留 hot70 的 QPS 依次为 **7,888 / 8,601 / 7,304 / 6,896 / 6,239**。全部 11 个 uniform/hot70 场景中，tenant 相对 direct 的 QPS 差异为 **−1.14% 至 +1.34%**，没有观察到明显的常驻 manager 吞吐瓶颈；不代表整个 tenancy 没有内存、恢复或缓存 miss 成本。

压力场景仅保留四分之一驻留槽位：

| 总向量 / tenants | slots | uniform QPS | uniform p99 ms | hot70 QPS | hot70 p99 ms |
|---|---:|---:|---:|---:|---:|
| 1M / 8 | 2 | 383 | 80.76 | 619 | 56.27 |
| 1M / 32 | 8 | 1,153 | 23.62 | 2,104 | 21.65 |
| 2M / 128 | 32 | 1,195 | 17.02 | 3,027 | 12.25 |
| 10M / 32 | 8 | 303 | 99.81 | 454 | 99.09 |
| 10M / 128 | 32 | 787 | 92.50 | 2,070 | 30.89 |

压力查询输出与同 fixture 全驻留输出逐字节相同，所有计时轮次 query errors 为 0。表明当前 LRU/加载回收保持结果一致，但 miss 的尾延迟仍然显著，不能以全驻留 QPS 代表缓存压力下的服务质量。

当前 1M/1 的约 8K QPS 搭配 **97.7% recall**，历史约 25K QPS 搭配 **76.2% recall**；预算、构图参数及被撤回的优化均不同，不应把 QPS 降幅解释为新增 tenancy overhead，也不应继续引用旧 +170.2% 为当前收益。128-tenant 全驻留 peak RSS 为约 4.4–5.4 GiB；本轮未做单因素消融，不能将这部分变化单独归因于某一个撤回项。

证据：`runs/resident/`、`runs/pressure/`、`runs/accuracy-first-five/`、`runs/accuracy-final/`、`runs/pressure-accuracy/`。

### 16.7 混合读写、删除与进程崩溃

2M/128，4 writers + 8 readers，uniform 查询，CPU 0–7；每 tenant 插入 128 条，共 **16,384 个 ACK 插入、128 个 ACK 删除**。独立线程每完成一次 checkpoint 后等待 250 ms 再发起下一次。每种容量仅运行一次，下面是验收样本，不当作三轮中位数或稳定优化收益：

| slots | 插入 ACK/s | read p99 ms | query errors | 更新后 macro Recall@5 |
|---|---:|---:|---:|---:|
| 128，全驻留 | 534.37 | 6.35 | 0 | 99.916% |
| 32，压力 | 315.57 | 41.85 | 0 | 99.916% |

两组均通过 ACK/VID/payload/删除状态核对及 close/reopen 输出一致性；每组更新后的 128K 个返回结果均核对真实向量距离。全驻留的周期 checkpoint 调用为约 1.00–6.80 s；压力组一次调用为 **52.43 s**。这包含等待更新等成本，不是所有查询停顿 52 秒；checkpoint 有界进入/公平性问题仍未解决。本轮没有重做该长调用的栈采样归因，也没有同耐久性 private 写入对照，不能计算写入 tenancy tax。

1M/8、2 slots、4 writers + 8 readers 的两个 SIGKILL 场景均通过恢复和全量检查：

- 写入途中被杀：恢复 **256 个已 ACK 插入**，核对 1,000,248 个 live vectors。
- Insert 已执行、ACK 记账前被杀：恢复 **256 个已 ACK 插入及 1 个未 ACK 插入**，核对 1,000,249 个 live vectors。结果未知的写入仍不能盲目重试。

初始六组全量核对共 **25M case-vectors**，覆盖 head-only 向量及 posting 的真实 payload/VID/版本。没有将“head 中有但 posting 中没有”误报为丢数据；对齐时实际发现的已删除 head 返回问题见 §16.3，修复后本轮未再观察到错误。禁用 RocksDB 的 legacy server/client 构建也通过。

证据：`runs/mixed-2m128-s128/`、`runs/mixed-2m128-s32/`、`runs/crash-1m8-during-writes/`、`runs/crash-1m8-after-apply/`、`runs/verify-final/`。

### 16.8 当前交付与复现

代码保留在工作区，**未新增 commit/push**。精确还原的原文件包括 `Heap.h`、KDT header、`VersionLabel`、`Dataset`、private RocksDB controller 和原参数定义；新增存储配置只在 SharedRocksDB 中对齐，不重构 private controller。

最终汇总为实验根目录的 `results-summary.json`；逐轮命令、二进制 SHA、配置环境、CSV、资源护栏和失败/重试记录均保留。`aligned-source-final.tar.gz`、`aligned-code-final.patch`、`aligned-manifest-final.json` 冻结最终源代码和二进制身份。构建 fixture 后只对两个条件做了格式化，重建了最终测量二进制；格式化前源代码、二进制和 manifest 另存，没有混淆计算逻辑不同的版本。

本轮验证的新可执行文件在 `/mnt/sptag-tenant-alignment-7de262b3-20261106/Release/`，没有覆盖仓库原 `Release/` 的旧二进制；运行当前实现时需使用该目录，或从当前源码重新构建，不能将旧二进制当作对齐版。

复现入口为 `build_all.py`、`run_study.py`、`verify_fixtures.py`、`measure_all.py` 和 `report_results.py`；这些脚本保留绝对数据/产物路径，需在新目录中调整，并使用新的 run tag，不能覆盖现有证据。核心 native 命令仍为仓库中的 `tenantperf build-many / accuracy / query`。

结论限于上述 warm-cache native 场景：同配置的 1M/1 private 差距约 0.7%–0.8%，常驻 manager 增量在约 ±1.4% 的观测范围内；内存、冷加载及混合写尾延迟仍有成本。单 DB 合并、自动 checkpoint、tenant compute admission/fairness、cold SSD/TCP 队列和掉电恢复仍未实现或未验证，不因这次收敛而扩展范围。

## 17. 复用原 SPANN 的 head 选择（2026-11-09）

`TenantIndexManager::Create` 删除强制 `Base.IndexAlgoType=BKT`。不传 head 时不设置该参数，直接继承原 SPANN 的默认值（当前 KDT）；显式 BKT/KDT 通过原 `SetParameter` 和 `VectorIndex::CreateInstance` 构建。Manager 不复制新的 head 默认配置，也不改树、图、搜索预算或构建算法。

KDT 仅补齐已用于 BKT 的两项 managed 适配：`UseInstanceWorkspaces` 选择实例隔离 TLS；managed `SaveConfig` 将 workspace 的 hash exponent 写入序列化配置，而不改在线查询使用的成员值。普通非 managed KDT 的 workspace 和保存行为保持原样。

### 17.1 接口和持久化兼容

- 内部 C++ `Create(..., count, headIndexAlgo=IndexAlgoType::Undefined)`：省略表示继承默认值；显式接受 `BKT`、`KDT`，其他枚举在分配 index ID 前拒绝。
- C++/Python `CreateTenant(..., metric, headIndexAlgo)`：最后一个参数可选，省略、null/None 或空字符串继承默认值；非空只接受 `"BKT"` / `"KDT"`。原六参数调用仍有效。
- CLI：`tenanttool ROOT create TENANT TYPE METRIC VECTORS.bin [BKT|KDT]`。
- 默认 Create 和其他操作仍发送原 v1 请求。显式 head 的 Create 使用 v2：原 29-byte header 后、tenant 字符串前增加一个 uint8 `IndexAlgoType`。服务端接受 v1/v2，响应仍为 v1；v2 只允许 Create 和合法显式 head。旧服务端拒绝 v2，而不是悄悄忽略选择。
- head 由原配置文件保存，load/replay 不覆盖它。因此旧 BKT tenant 仍加载为 BKT；新默认 KDT 和显式 BKT 可在同一节点共存。没有原地切换或自动重建已有索引。

```python
client.CreateTenant("default", seed, 128, 4, "Float", "L2")         # 原 SPANN 默认
client.CreateTenant("bkt", seed, 128, 4, "Float", "L2", "BKT")     # 显式选择
client.CreateTenant("kdt", seed, 128, 4, "Float", "L2", "KDT")
```

本次只扩展 head 选择，不开放任意参数透传。Python 对该方法使用 SWIG `compactdefaultargs`，避免可选参数产生的 overload dispatch 绕过已有 ByteArray buffer typemap；不修改公共 Python 输入转换规则。

### 17.2 验证与性能边界

现有回归扩展了默认值继承、显式 BKT/KDT、非法 head、保存/重载、不同 head 并发更新与进程崩溃重放，以及 v1/v2 协议和 Python/CLI 调用。独立产物目录为 `/mnt/sptag-tenant-heads-7de262b3-20261109`；可执行文件使用其中 `Release/`，不覆盖 §16 的原测量二进制。

Release 和 ASAN/LSAN 均通过 28 个 tenant 用例、2,482 个断言；legacy KDT/BKT/iterator 的 3 个用例、197 个断言通过。真实 Python/TCP/CLI smoke 覆盖默认与显式 head、CRUD、并发调用和崩溃恢复；ROCKSDB=OFF server/client 构建通过。额外使用 11 月 6 日旧二进制创建 BKT fixture，再用新版本重开、插入、删除并核对保存配置，确认没有隐式切换 head。首次 Python smoke 暴露的 SWIG overload buffer 转换问题已用上述局部 feature 修复，失败与修复后的日志均保留。

产物根目录保存 `head-selection.patch`、`source.tar.gz`、`manifest.json` 和 `logs/`，含本次差量、源码/二进制 SHA、构建命令及验证记录。代码未新增 commit/push；仓库 `Release/` 的旧二进制未替换。

`tenantperf` 的 private builder 同样删除固定 BKT，未来新建默认 fixture 时两侧都继承原 SPANN。§16 的 114 轮和约 0.77% 差异仍是 **BKT 对 BKT** 的历史结果；本次没有重新执行六规模性能矩阵，也不宣称新默认 KDT 有相同 QPS/recall。

## 18. 基于 Benchmarking 分支的多层集成

集成基线为 Microsoft `users/qiazh/pre-merge-tikv-bugfix` 的
`539f6a60711c490d72902176cb0aad608bcb60e4`。移入的是原 `tenant` 分支
`9ace1e3ff12bb4484cd76e43bbcc5fa4413376a0` 相对 `cafdd0ab` 的 multi-tenant 差异，
而不是将整个 `main` 合入该开发分支。原分支及既有实验数据不变。
后续该集成的 PR base 应选择上述开发分支；若仍选 `main`，会把开发分支尚未进入
`main` 的大量上游改动一起带入 PR，不再是仅含 multi-tenant 的差异。

本节记录基线集成时的状态；后续完整 preset 接入和范围审计见 §19。

### 18.1 保留上游算法和层间存储编码

- 保留上游 `m_extraSearchers`、多层 build/search/update、global/local head ID 映射及每层 version map。
- 复用上游 `DBKey(postingID) = layer * MaxID + postingID`；共享 RocksDB 只在外面加 tenant/index namespace，不新增每层 DB 或 CF。managed 模式固定 `ShareDB=true`，创建和 VID 预留检查编码范围，避免溢出或跨层碰撞。
- 原生 Manager 的可选 `postingLayers` 通过原 `SetParameter("Layers", ..., "BuildSSDIndex")` 设置；0 表示继承默认值。`Create(..., IndexAlgoType::BKT, 2)` 可创建两层 tenant，不把 BKT/两层写成生产默认值。现有 socket、CLI、C++/Python 客户端接口保持兼容，本轮不扩展它们的层数参数。
- tenant 只管理实例隔离 workspace、共享 DB 的生命周期、单调 VID 预留及日志、更新完成确认和快照。split/merge/reassign 及上游新增的 async append 都传播同一个请求完成上下文，包括跨层派生任务。

上游 version map 的 `Count()` 现在是存活成员数，删除会减少它，不能用作下一个 VID。
managed 实例因此单独维护分配水位，保存到上游已有的 `Base.VectorSize`；删除不复用 VID，
重载和日志 replay 使用同一水位。在线保存只序列化水位副本，不修改查询正在读取的配置。
快照按保存的层数包含所有 `DeletedIDs.bin_<layer>`，不再假设旧版的单一 version map、
posting-size 和 checksum 文件。

### 18.2 RocksDB 接口适配与数据兼容

该上游分支的 `KeyValueIO::Merge` 新接口要求返回合并后字节数，共享 adapter 与 private
RocksDB 一样执行 Merge 后 Get。构建调用的 `MergeWithCAS` 使用 RocksDB 原子 Merge；
这不是要求每次追加做客户端读改写。posting 批量读取允许并发 split/merge 后已删除的
key 返回空 buffer，其他 I/O 错误仍返回失败。

同时补齐直接阻挡该分支 RocksDB 构建/运行的兼容点：正确的 controller include、
唯一的 `RocksDbIOUringEnable` 定义、private adapter 的 `MergeWithCAS`，
Merge 首次创建 key 时原 merge operator 的空 base-value 处理，
以及批量读取的 buffer 容量和错误路径。RocksDB options、算法参数及非 managed
SPANN 的多层流程未改成另一套配置。

**节点格式为 `SPTAG-Tenant-2`。** 上游 head 映射、posting ID 和 version-map 文件格式
与旧基线不同，不能安全地原地打开旧 `SPTAG-Tenant-1` 节点；本分支会明确拒绝，而不是
悄悄重解释或修改其数据。使用新的节点目录重新导入原始向量；不提供自动磁盘格式迁移。
原 `tenant` 分支仍可访问原节点。

本次集成不是性能报告。完整 `Benchmarking.md` workload/preset 的接入及重测仍需单独完成；
原生两层支持并不表示现有 `tenantperf` 命令已自动采用文档中的所有参数。

### 18.3 集成验证

Release 与 ASAN/LSAN 均通过 28 个 tenant 用例、2,517 个断言，覆盖一层/两层混合 tenant、
并发插入、删除后继续分配 VID、全部层的快照重载与进程退出重放；额外覆盖 private RocksDB
首次 Merge 建 key、返回长度和已删除 posting 的批量读取。旧 KDT/BKT/iterator 的 3 个用例、
197 个断言通过；真实 Python/TCP/CLI smoke 以及 `ROCKSDB=OFF` server/client 构建通过。

使用上游 C++20 构建设置，GCC 11、`TBB=ON`、`FOLLY=ON`、`LARGEVID=OFF`、RocksDB 7.6。
恢复上游锁定的 zstd/Folly 子模块；主机旧 liburing 头文件无法编译该版 Folly，
因此在实验目录私有安装 liburing 2.15，没有升级主机软件或修改子模块源码。
早期失败及最终通过的记录均保留在
`/mnt/sptag-tenant-benchmark-7de262b3-20261109` 的日志和 manifest 中。

## 19. 完整实验 preset 与最小改动范围

### 19.1 配置复用和优先级

原生 `TenantIndexManager::Create` 新增末尾可选参数
`const Helper::IniReader *configuration = nullptr`。先将 INI 的 `Base`、
`SelectHead`、`BuildHead`、`BuildSSDIndex` 四节交给原 SPANN `SetParameter`，
再应用显式的非默认 `headIndexAlgo` / `postingLayers`，最后施加 tenant 必须拥有的
类型、维度、距离、目录、容量和存储/恢复约束。不保存调用者的 INI 指针。
未提供配置的调用仍继承上游默认值，不自动选择 BKT 或实验 preset。
可选参数保留旧源码调用方式；原生 C++ 依赖需随库重编译，不承诺旧二进制 ABI。

索引输出文件必须是安全的 tenant-relative 路径；head 文件名复用所选 BKT/KDT
实例的 `GetIndexFiles()` 验证。非法 head、层数、编码范围及越界文件路径在分配
tenant ID 和创建目录之前拒绝。其余参数沿用上游解析器的行为，这不是新的严格 INI
schema，也不是允许不可信远程用户任意设置索引参数的接口。共享 RocksDB 的 engine
options 仍由节点管理，不能通过某个 tenant 的 INI 改变。

本轮只开放原生 C++ 配置入口及实验工具的 `TENANT_PERF_CONFIG` 环境变量；
socket、CLI、C++/Python 客户端没有新增任意 INI 透传协议。

### 19.2 可复用的 SIFT1M 实验配置

[`Test/tenant_sift1m.ini`](../Test/tenant_sift1m.ini) 同时展开
[`Benchmarking.md`](Benchmarking.md) 的显式配置与固定基线
`539f6a60` 的 `Test/src/SPFreshTest.cpp::BuildLargeIndex` 隐式配置。
主要设置为 UInt8/128D/L2、BKT、两层 posting、8 build threads、
Posting/SearchPosting/Buffer=4/4/4、Internal/SearchInternalResultNum=64、
Ratio=0.2、ExcludeHead=false、AppendThreadNum=4、ReassignThreadNum=0、
MergeThreshold=10、Sampling=4、InPlace=true、OneClusterCutMax=true。
MaxCheck=4096、ReplicaCount=8 继承该基线默认值，并在持久化配置中核对。
SelectThreshold/SplitFactor/SplitThreshold 的输入 0 由原实现推导为 5/5/10，
不能把保存后的非零值误判为配置未生效。

参考 workload 为初始 990K + 10 批各 1K 插入、4 writers、0 concurrent readers、
每阶段 200 queries / 16 query-driver threads / top-5，无删除。输入路径和实际
workload 由实验命令显式提供；这份 INI 不会自动启动原 `SPFreshTest` harness。
额外的 10K-query 稳态读、多 tenant、LRU pressure 和混合读写实验是扩展，
不是参考文档中的原始 workload。

必须保留并披露的差异：

| 项目 | 上游参考 preset | 本轮受控 RocksDB 实验 |
|---|---|---|
| posting backend | FILEIO；文档也有 TiKV 场景 | private RocksDB / shared RocksDB |
| head/SSD refinement threshold | 0.4 | managed 必须为 2，避免回收/重编号破坏稳定 VID |
| 旧 SPANN WAL / recovery | 原 harness 设置 | managed 禁用，使用 tenant application log；不等于关闭 RocksDB WAL |
| async merge/append | 原参数机制 | managed 固定 false / 0，保证请求完成边界 |
| 批前 compaction | 原 insertion harness 调用 `ForceCompaction` | 不模拟不存在的 per-tenant shared compaction |
| 写入完成边界 | 原 harness 异步插入及批次 barrier | 每次请求等待其跨层派生维护完成，另计 checkpoint |

private 控制从同一个 shared fixture 导出 head、graph、映射、各层 version map 和
posting 内容，不独立重建另一棵随机树。`tenantperf legacy` 表示**同一当前二进制**
中的普通 private RocksDB 路径，不表示旧源码版本。`direct` 与 `tenant` 使用同一个
shared fixture，前者绕过 Manager；只有它们的差异才能用于估计 Manager 调度成本。
不能把 private/shared 的总差异都归因于 Manager，也不能与文档的 FILEIO/TiKV
绝对 QPS 直接作性能优劣结论。

### 19.3 公共修改范围审计

审计对照集成基线 `539f6a60`，不是对照另一个未包含多层实现的 `main`。
保留的公共修改及其必要性如下：

| 修改类别 | 保留原因与边界 |
|---|---|
| VectorIndex / SPANN 生命周期接口 | 注入 posting store、跟踪请求派生工作、稳定 VID 和逐层快照；普通索引保持原路径 |
| BKT/KDT workspace factory、在线配置保存 | 多实例的实例身份与安全回收、并发搜索时保存配置；不替换搜索/选点算法 |
| SPANN 异步任务上下文、错误及统计同步 | Insert ACK 必须覆盖 split/merge/reassign 和跨层派生任务；managed drain/stats 行为受注入 store 限定 |
| ThreadPool 停止和排空 | tenant 卸载不能遗留访问已释放实例的后台工作，不是新的调度优化 |
| RocksDB adapter | 多层接口兼容、首次 Merge、返回长度、批量读取容量/错误处理；未更改 engine 默认 options |
| socket 完整包发送与 client 生命周期 | 并发 RPC 不能交织报文；回调、重连、超时及析构必须有明确所有权 |
| server / C++ / Python 路由与构建 | 显式选择 tenant 服务并保持 legacy API/wire 兼容；绑定处理 GIL/缓冲区生命周期 |

`Heap`、`Dataset`、`VersionLabel` 和 BKT/KDT/SPANN 参数默认值定义文件没有引入
实验调优；保留原 BKT/KDT、SelectHead、RNG 和多层 split/merge/reassign 算法。
本轮新增的生产差量仅为可选原生配置、配置边界检查及避免层范围乘法溢出的检查。
实验 preset、private 克隆和时间线采集位于测试工具；更细的 lifecycle I/O
诊断助手保留在实验产物目录，不添加 production tracing hooks。

结论是没有需要为本轮性能实验额外合入的通用优化。两 DB、whole-tenant LRU、
无自动旧格式迁移、无 exactly-once/auth/节点级计算预算/断电持久性保证等既有边界
不因这次配置接入或范围审计而改变。PR 准备与创建仍按用户要求暂缓。

## 20. 对齐 preset 后的规模、更新与尾延迟实测

### 20.1 方法与可解释范围

产物位于
`/mnt/sptag-tenant-benchmark-7de262b3-20261109/study-20261109`。
使用 §19 的配置、同一组不可变 SIFT 输入及逐 tenant exact squared-L2 truth。
1M/1、1M/8、1M/32、2M/128 使用固定 seed 20261104 的不重叠 shuffle/Zipf 分区；
canonical update 则使用原始、未 shuffle 的 SIFT1M 前缀，二者的 VID/truth 不能混用。
记录实际保存的所有配置、输入/二进制 SHA、运行命令和原始 CSV，而非只记录配置模板。

主机报告 AMD EPYC 7V12，实验限制在 CPU 0–15、nice 10；受控计时不与自己的
构建、其他计时任务重叠，但不是独占整机。private 和 shared 均使用 3 GiB
RocksDB block-cache 容量，shared 的容量是节点总预算；`maxResidentBytes=0`，
仅改变 resident tenant slots。没有清空主机 page cache。

读实验为进程内调用，不含 TCP/Python 开销；使用完整 10K queries、3 秒 warmup、
15 秒计时，3 次轮换/交替顺序。下表为三次 QPS 和 p99 各自的中位数，不是合并
原始请求后的分位数。all-resident 在 warmup 前预填全部 tenant，并核对计时期间
load/eviction 增量为零。QPS 只计窗口内完成的请求，延迟包含尾部 drain 请求；
没有 coordinated-omission correction。

#### 同索引 private/direct/Manager 对照

1M/1 的三条路径使用同一份 head、graph、映射、逐层 version map 和 posting 内容；
最终二进制上的完整 10K queries 结果逐字节一致。private 克隆使用上游构建所用的
`MergeWithCAS`，不使用会改变 SST/blob 放置行为的 `Put`。早期 Put 控制结果保留，
但不用于下表；修正后重测了 18 个窗口。最终汇总采用 18 个单 tenant 加 54 个
多 tenant 窗口，总计 72 个完整窗口。

| 路径 | Query threads | QPS 中位数 | p99 中位数，ms | 进程峰值 RSS 中位数，MiB |
|---|---:|---:|---:|---:|
| private，原 RocksDB adapter | 1 | 467.53 | 2.790 | 165.8 |
| direct shared，绕过 Manager | 1 | 425.33 | 3.180 | 1,440.3 |
| Manager shared | 1 | 420.80 | 3.253 | 1,449.5 |
| private，原 RocksDB adapter | 16 | 5,998.00 | 3.427 | 308.3 |
| direct shared，绕过 Manager | 16 | 5,713.87 | 3.583 | 1,625.1 |
| Manager shared | 16 | 5,690.53 | 3.599 | 1,637.3 |

Manager 相对 direct 的 QPS 差为 C1 -1.07%、C16 -0.41%；完整 managed shared
路径相对 private 则为 C1 -10.00%、C16 -5.13%。因此不能用较小的 Manager
包装层差异宣称整个 shared 方案没有成本，也不能沿用旧基线的约 0.77% 总差结论。

即使配置、Merge 构造和逻辑内容匹配，**物理数据库布局仍不相同**：本次 private
约 5.95 MB SST + 1.214 GB blob，shared 约 1.242 GB SST、无 blob。
相关目录字节数保存在 `matched-control.json`。key 编码、CF、恢复及数据库布局属于
两个 storage path 的区别；没有通过额外 compaction 或 cache 调优抹平它们。
private/shared 的峰值 RSS 差也不能直接归为 Manager 的常驻内存开销，
更不能由这些峰值推断具体对象的稳态内存归属。

### 20.2 多 tenant 与 whole-tenant LRU

均为 C16。`hot70` 将 70% 请求分给最大的 tenant-000，其余均匀分配。

| 数据 / tenants | 流量 | direct 全驻留 QPS | Manager 全驻留 QPS / p99 ms | Manager 1/4 slots QPS / p99 ms |
|---|---|---:|---:|---:|
| 1M / 8 | uniform | 6,381.47 | 6,463.67 / 3.558 | 244.67 / 242.056 |
| 1M / 8 | hot70 | 6,282.53 | 6,329.67 / 3.324 | 435.13 / 191.311 |
| 1M / 32 | uniform | 7,520.13 | 7,548.40 / 3.241 | 546.27 / 92.322 |
| 1M / 32 | hot70 | 7,034.20 | 7,025.53 / 3.047 | 995.60 / 68.973 |
| 2M / 128 | uniform | 5,611.87 | 5,614.40 / 4.219 | 450.33 / 81.136 |
| 2M / 128 | hot70 | 6,553.93 | 6,519.33 / 3.367 | 1,323.00 / 64.937 |

全驻留的 Manager/direct QPS 差约 -0.53% 至 +1.29%，不把小幅正差解释成
Manager 提供了加速。1/4 slots 则明显受到 reload、入驻容量等待、回收和原生
加载初始化影响；这里的 cold 是 **tenant 未驻留**，不代表磁盘/page cache 冷。
这些数字不能外推成 cache miss 仍有几乎零开销。

uniform 的进程峰值 RSS 中位数，Manager 全驻留 / 1/4 slots 分别为：
8 tenants 2,479 / 1,842 MiB；32 tenants 5,612 / 1,875 MiB；
128 tenants 12,073 / 3,419 MiB。RSS 包含启动恢复和分配器保留，不等于
head 的净大小或稳态常驻用量；slots 也不是整个进程/OS cache 的硬内存上限。

首 200 queries 的逐 tenant truth 检查覆盖结果 ID、重复 ID、精确返回距离和完整性：

| 数据 / tenants | strict-ID recall@5 宏平均 | distance-tolerant recall@5 宏平均 | 最低 tenant tolerant recall |
|---|---:|---:|---:|
| 1M / 1 | 0.984000 | 0.984000 | 0.984 |
| 1M / 8 | 0.991000 | 0.991000 | 0.984 |
| 1M / 32 | 0.997625 | 0.997688 | 0.989 |
| 2M / 128 | 0.999141 | 0.999180 | 0.988 |

这不是完整 10K queries 的 exact-truth recall 报告。

### 20.3 参考插入 workload

990K 初始向量 + 10 批各 1K，4 writers、0 concurrent readers，每阶段 200 queries /
16 query-driver threads / top-5。10 批 insertion ACK 吞吐为 1,646.76–2,814.25 vectors/s，
单批插入耗时 0.355–0.607 秒，随后 checkpoint 单独耗时 0.146–0.459 秒。
不能把二者相加后又称为单纯插入服务时间，也不能与原 harness 的异步提交速率混用。

各阶段对当前 prefix 合并计算 exact truth，并核对 10K 插入的输入 ID/实际 VID
为一一映射。11 个数据阶段的 tolerant recall@5 均为 0.987；
最终结果与 reopen 后逐行一致，所有返回向量的距离精确匹配。
200-query 批次会新建工作线程；初始/reopen 阶段还包含 cold admission，
p99 分别约 218/223 ms，后续阶段约 11–16 ms，不能把这些短批次当成 §20.2 的稳态读。

### 20.4 checkpoint 的长调用是否等于查询暂停

在同一 2M/128 checkpoint 的两份独立节点副本上，各执行 4 writers、
8 readers、每 tenant 32 次插入，共 4,096 次；另有 1 秒读基线。
读线程最多提交 30 秒，避免诊断程序自己无限等待 checkpoint。
单独助手只跟踪文件 I/O 和线程标识，不给生产代码增加计时锁或 tracing hooks。

| 指标 | 128 slots，全驻留 | 32 slots，uniform pressure |
|---|---:|---:|
| 插入 ACK QPS | 1,845.35 | 134.22 |
| 读 p99 / 最大延迟，ms | 6.892 / 19.076 | 69.511 / 110.574 |
| 并发 checkpoint 总调用时间，ms | 3,280.252 | 31,356.137 |
| 至该 checkpoint 线程首次 snapshot I/O，ms | 2,069.403 | 30,397.399 |
| 首次 snapshot I/O 至返回，ms | 1,210.849 | 958.739 |
| checkpoint 调用期间完成的读 | 10,082 | 7,753 |
| snapshot 开始之后完成的读 | 3,609 | 0（读提交已结束，不能据此判定暂停） |
| 达到 30 秒读提交上限 | 否 | 是 |

首次 I/O 用**同一 checkpoint 线程**的 `indexloader.ini` save-open 定位，
不混入 eviction 线程的保存。此前时间包括锁等待、drain 和准备，并非精确的
单把 mutex 等待；其后还包含快照、State checkpoint、发布和日志处理，
不能把 ini 文件关闭当作全部持久化结束。

全驻留时，checkpoint 调用期间读完成的最大间隔仅约 3.27 ms，且真正开始
snapshot 后仍完成了 3,609 次读。因此 **3.28 秒 checkpoint 不是 3.28 秒全局读暂停**。
两个后续不写 dirty snapshot 的 checkpoint 仍分别花费约 403/645 ms，
同时继续完成读，说明不能只用 dirty head 序列化解释 checkpoint 的全部成本。

32 slots 时，checkpoint 直到持续冷读提交结束、写入接近排空后才开始 snapshot。
8,169 次读中，6,109 次在自身调用线程观察到 load I/O，其 p99 为 72.630 ms；
未观察到自身 load 的请求 p99 仍为 46.541 ms，其中也可能包含等待其他线程加载，
不能全部当作纯 resident hit。此次长 checkpoint 主要落在 snapshot **之前**，
而不是 31 秒持续执行磁盘快照。该样本不证明无限期饥饿，但也不能宣称持续
cold-load/写流量下 checkpoint 有有界进入时间：当前 shared-mutex admission
没有 writer-priority 保证。调整 admission/fairness 属于下一步待讨论的行为选择，
本轮没有擅自加入调度优化，也没有把诊断中的 30 秒上限加入生产代码。

两份副本都核对了 4,096 个 ACK VID、128,000 条结果的精确距离、逐 tenant 更新后的
truth 和 reopen 一致性；最终 tolerant recall 宏平均均为 0.999172。
上述 lifecycle 数字来自各一次诊断，不是三次稳态矩阵的统计置信区间。

### 20.5 记录与边界

Release、ASAN/LSAN 的 28 个 tenant 用例均通过 2,557 个断言，包括配置优先级、
路径拒绝、两层更新和重载。原 KDT/BKT/iterator 的 3 个用例 / 197 个断言、
重编译后的 Python/TCP/CLI smoke、`ROCKSDB=OFF` server/client 构建通过。

第一次 lifecycle 副本运行触及既定 80 GiB 累计磁盘增长保护而中止，未采用该次结果。
保留错误、命令、恢复记录和所有已完成测量；仅清理可重建的 `work-*` 副本及该次
中断的诊断 clone，并对日志无损压缩，再在**未提高资源上限**的条件下重跑两种 slots。
private 对照重测也有一轮在恢复临时工作副本时触及同一保护；该窗口作废，保留已经
完整结束的窗口，只补齐未完成窗口。完成的 lifecycle checkpoint/recovery 文件逐个
无损归档并校验原始 SHA 后释放副本空间，不提高磁盘额度。归档 manifest 包含还原
路径、校验和及权限；原始输入、canonical fixtures、结果和校验记录保留。
早期配置验证器误把自动阈值
当成 0、诊断助手首次编译及测试过滤器错误也保留在日志中，没有用失败轮次填补数据。

本轮没有做生产算法/默认值调优、checkpoint 公平性改造或旧节点迁移，也没有准备
或创建 PR。§16 的旧基线数字仍是历史结果，不用它替代本节的多层实测。
