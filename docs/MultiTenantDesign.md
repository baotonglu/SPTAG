# SPANN Multi-Tenancy: Initial System Design

状态：已实现单节点多租户的共享存储与恢复、同租户并发读写、增量工作快照、LRU 驻留缓存、管理员内部状态接口，以及基于现有 C++ `AnnClient` / SWIG Python `SPTAGClient` 的五种在线操作。**仅面向可信调用方，不提供认证授权、请求去重或完整资源治理。** 第 11 节记录实现范围，第 12 节记录接口契约；缓存设计见 §1.4。原有 BT comments 保留。

接口阅读入口：**[第 12 节：用户接口契约](#12-用户接口契约)**，首先说明在线客户端 API，再说明内部 C++ Manager、旧 socket 查询和离线 CLI；用户不需要直接操作 Manager。

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

- 节点生命周期屏障保证普通操作期间 tenant 不被删除；内部 lease 保证实例不被卸载。创建的构建阶段使用共享保护，发布、删除和关闭仍获得节点独占保护。
- 缓存状态和条件变量合并同 tenant 的并发加载，不覆盖整个查询/更新。已加载查询不持有提交锁。
- 单次插入等待本次操作所派生维护任务完成，不等待其他请求持续产生的全部任务；维护任务必须传递完成上下文。checkpoint、卸载和关闭仍排空整个索引。
- 查询不提供整个批次的事务快照：与更新重叠时可以看到部分进行中的变化；成功更新返回后的后续查询应看到其效果，受原有近似搜索语义限制。

### Checkpoint 和增量保存

- 独立的更新屏障使 checkpoint 暂停新更新，等待在途更新及维护结束；生命周期屏障固定 tenant 集合。
- 已加载 tenant 的只读查询不获取更新屏障，可以与 checkpoint 并行；按需加载会等待更新屏障，避免恢复文件与保存过程竞争。
- 保存路径必须只读地序列化在线索引，不做在线 refine、参数改写或查询触发的 merge。必要的底层并发修正仅限该路径的直接依赖。
- managed 索引的 version-map 扩容/访问及维护统计使用实例内保护；SPANN/BKT 保存配置时不改写在线查询使用的 hash 参数。非 managed 路径保留原行为。
- 每 tenant 记录当前日志序号、工作快照序号及快照存在性，仅重新保存 dirty tenant；未修改 tenant 的快照保留在共享 State DB 中，节点恢复点仍包含完整清单。
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

### 1.2 延后实现的 feature

- **tenant 级请求准入、并发限额和公平调度延期到分布式阶段。** 当前不实现；届时结合租户负载及 compute nodes 自动扩缩容设计，不作为本轮单节点 multi-tenant 完成的前置条件。
- **自动 checkpoint 调度：列入未来 feature，当前不实现。** 先完成 multi-tenant 相关功能及其直接依赖，再安排该功能。未来可考虑按时间或未纳入 checkpoint 的日志量触发，具体策略和阈值届时确定。
- 原有 SPANN 已有显式 checkpoint 执行入口，并由 SPFresh 更新流程在每轮更新后调用；缺少的是常驻服务的自动调度，不是本次 multi-tenant 改造造成的功能退化，也不作为当前 multi-tenant 功能完成的前置条件。
- 自动触发与增量保存是不同能力：当前已实现未修改 tenant 的快照复用，以及节点 checkpoint/恢复一致性保护。延期的只是自动调度；持续写入期间仍需上层显式调用 `Checkpoint()`，以控制日志增长和恢复重放量。

### 1.3 下一阶段单节点工作

- **自动驻留缓存：已实现。** LRU、容量目标、加载合并、lease 和 dirty eviction 见 §1.4；不向普通客户端开放 Unload。
- **生命周期并发：已实现创建构建阶段与目标租户卸载的隔离。** 创建发布、删除和关闭仍需全局提交协调；尚未实现无全局暂停的生命周期协议。
- **管理员状态与指标：已实现内部 `GetStats` 和离线 `tenanttool stats`。** 面向系统管理员，不扩展普通客户端的租户枚举权限；尚无独立在线管理员网络端点。
- **四租户 serving 性能实验：已完成，见 §13。** 保留改造前基线、改造后全驻留和缓存压力的独立结果，以及数据、配置与复现脚本。

### 1.4 单节点驻留缓存与生命周期实现设计

- 缓存单位是 **tenant 的整个 SPANN 实例**：head 图、version/map 等辅助状态和实例 workspace 一起加载/释放；postings 继续存储在共享 RocksDB，RocksDB block cache 独立配置。不是把所有 tenant 的 head 合并成一张图，也不是逐个 head 向量淘汰。
- 使用 LRU 淘汰空闲实例。`MaxResidentTenants` 默认 8，约束在线缓存中驻留及正在加载的实例数；`MaxResidentBytes` 默认 0（关闭字节目标），启用时按最近工作快照组件的总字节数计费。该计费可比较不同大小索引，但**不是 RSS 硬上限**：不包含线程栈、workspace 高水位、分配器、共享 DB、请求 payload 等；写入增长在下次保存后刷新计费。
- 若希望全部 tenant 在访问后保持驻留，可设置 `MaxResidentTenants=MaxTenants` 且 `MaxResidentBytes=0`；创建/恢复后仍按需加载，不做全量预热。没有单独的后台定时扫描或 RSS 压力探测器。
- 请求获得内部 lease 后才能访问实例。lease 存续期间禁止淘汰；在途维护包含在更新请求的 lease 内。空间不足时淘汰最久未使用且无 lease 的实例；候选都在使用时等待释放，不把这个缓存容量等待当成 tenant 计算准入或租户配额。
- 单个索引超过字节目标时允许它独占缓存，避免该 tenant 永远不可访问；统计明确报告超目标。加载/淘汰操作不持有全局缓存元数据锁进行磁盘 I/O；其他已驻留 tenant 的请求继续执行。相同 tenant 的并发加载合并，状态为 `Unloaded/Loading/Resident/Evicting`。
- 淘汰 dirty 实例先排空维护并保存工作快照，再释放内存；不会因此发布节点 checkpoint 或提前清理日志。启动时逐 tenant 验证、重放并保存必要工作快照，再释放该实例，避免所有 tenant 同时驻留；启动完成后按需加载。恢复仍是节点级，首次启动验证不是后台 lazy recovery。
- 本进程已恢复或保存成功的工作目录文件可直接用于再次加载，避免每次 cache miss 都把相同 head 从 RocksDB 重新复制到磁盘。该就绪标记不持久化；重启仍从已发布 checkpoint 建立新工作副本，不信任上次进程的临时文件。
- 锁顺序为管理操作协调锁（仅管理路径）→节点生命周期屏障→更新屏障→缓存元数据锁。普通查询的驻留命中不获取更新屏障。冷加载、淘汰和更新参与更新屏障；checkpoint 排除它们，但允许已驻留查询。缓存锁与 tenant 提交锁不覆盖查询、更新或 I/O 的完整执行。
- 创建仍串行化管理操作，但在节点共享保护下构建私有实例，其他 tenant 的读写可继续；构建结束后保存并释放私有实例，在不包含构建的全局独占提交阶段加入 Catalog 并发布节点 checkpoint。提交仍可能保存其他 dirty tenant，不承诺固定暂停时长。构建阶段可临时额外驻留一个实例，不计入在线缓存槽位；输入/构建峰值不是缓存预算的保证范围。
- 显式卸载只等待目标 tenant 的 lease，其他 tenant 不必停服。创建、删除、checkpoint、关闭串行协调，避免未提交构建状态进入其他 checkpoint；删除的节点级提交仍需全局协调，不宣称生命周期操作完全无全局暂停。
- 管理员通过内部 C++ 状态快照读取节点 failed/closed、缓存容量、驻留数/计费字节，以及每 tenant 的状态、lease 数、命中/缺失、加载/淘汰次数与耗时。计数为进程内诊断，不持久化；不增加普通客户端 List/Unload 权限。

| 路径 | 主要保护与锁顺序 |
|---|---|
| 驻留查询 | lifecycle shared → cache 短临界区取得 lease；执行时不持有 cache 锁 |
| 更新 | lifecycle shared → updates shared → 取得 lease；随后 tenant submit 临界区，索引更新在其外执行 |
| 冷加载/淘汰/卸载 | lifecycle shared → updates shared → cache 状态转换；释放 cache 锁执行 I/O |
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
Create(tenant_id, value_type, dimension, metric, seed_vectors, count)
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

现有 BKT head 的序列化包含 vectors、tree、graph 和 head 删除标记。完整 SPANN 恢复还需要匹配的 version/deletion map、posting sizes、checksums、映射和配置等状态。第一版复用现有文件序列化：写入 index_id 隔离的工作目录，再按 1 MiB 分块保存到 `head_snapshots` CF；恢复时从 CF 还原文件再调用现有 loader。这些临时文件不是权威恢复材料。

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
3. 确定每个索引已纳入该恢复点的连续日志进度，仅为未保存或日志序号变化的 tenant 重写 head/辅助状态；未修改和已卸载 tenant 复用工作 State DB 中的有效快照，仍记录完整索引清单。
4. 在状态保持一致的情况下创建新的 State DB checkpoint；检查所有序列化、数据库和文件操作结果。
5. 只有整个 checkpoint 完整后，才能在 Recovery DB 用同一 WriteBatch 更新 `recovery_meta/current` 和 `catalog/tenants`；调用 `FlushWAL(false)`，不要求 fsync。
6. 发布成功后按每 tenant 连续进度清理旧日志，然后清理显式登记的旧 checkpoint/工作副本；最后释放屏障。未发布或失败的 checkpoint 不得取代原有效恢复点。

清理阶段仍在更新屏障内，不预先保证更新暂停时长或 shared_mutex 的调度公平性。Create/DeleteTenant/Close 所触发的 checkpoint 已持有生命周期独占屏障，仍会暂停查询。暂未增加定时 checkpoint，嵌入调用方应按日志量或周期调用 `Checkpoint()`。析构只排空并释放，不隐式发布 checkpoint，保留异常退出/重放路径。

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

建议第一阶段：

- 为每个 index 实例提供独立 workspace pool。
- 复用现有 `SharedPoolWorkSpaceFactory` 的思路，并补充容量上限。
- 将 head index 的 workspace 也纳入实例隔离。
- workspace 必须属于具体运行时实例，不能在同一 index_id 卸载重载后复用旧实例的 I/O 绑定。
- 卸载前等待 workspace 归还及其关联 I/O 完成。

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

- 一个 tenant 一个 SPANN/BKT-head 索引；每次删除后重建分配新 index_id，没有 generation。
- 创建需要至少 **64 条种子向量**，不能创建空索引。所有向量都进入 postings；可以把已有索引内全部向量删除并正常 checkpoint/reload。
- 数据类型支持现有 `Int8/UInt8/Int16/Float`，距离支持 L2/Cosine。维度、类型、距离在创建后固定。Cosine 查询在内部副本归一化，不修改调用方输入。
- 默认最多 64 个 tenant、每 tenant 1,000,000 个已分配 VID、维度 4096、top-k 1000。VID 不回收，删除不会腾出这个计数配额；这些是容量边界，不是性能承诺。
- 两 DB 共用默认 64 MiB block cache；每 CF 配置 4 MiB write buffer、最多 2 个，两个 DB 各限制 2 个 RocksDB 后台任务。不是整个节点 RSS 的硬上限。
- 每索引采用保守的固定构建参数和少量维护线程；关闭 query-triggered merge、旧 WAL 和独立异步 append queue，写入在确认前等待本请求派生的维护。没有公平调度器或硬磁盘/内存配额。
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
CacheBytes=67108864
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
| `CreateTenant(tenant, vectors, count, dimension, value_type, metric)` | 在线创建唯一索引；种子数至少 64；类型如 `"Float"`，距离如 `"L2"` / `"Cosine"` |
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
| `cacheBytes` | 67,108,864 | 两个 DB 共用的 block cache 容量，不是进程总内存上限 |
| `maxResidentTenants` | 8 | 在线驻留及加载/淘汰保留的槽位数；须大于 0。恢复或私有构建的临时实例见 §1.4 |
| `maxResidentBytes` | 0 | 缓存工作快照字节计费目标，0 表示关闭；不是 RSS 上限，允许单个超大 tenant 独占 |

Open 不修改已有索引的数据配置；不要把更换 limits 当成索引缩容或重建。析构会排空并释放资源，但**不等同于成功 Close**，不会隐式发布新 checkpoint。调用方应显式检查 Close 的返回值。

### 12.4 服务端内部 C++ API：tenant 生命周期与数据操作

下表参数顺序与当前头文件一致；除 `List` 外，接口返回 `ErrorCode`。

| 接口 | 输入 | 成功输出及效果 |
|---|---|---|
| `Create(tenant, type, dimension, metric, vectors, count)` | tenant ID、固定索引配置和至少 64 条种子向量 | 构建索引并完成节点 checkpoint 后返回 Success；已存在的 tenant 不覆盖、不重建 |
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
