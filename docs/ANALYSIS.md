# GoLink 项目分析报告

> 分析对象：`C:\workspace\go\shortlink`（GoLink — 基于 go-zero 的高性能短链接系统）
> 分析时间：2026-09-11　分析方式：全量源码通读 + 5 模块实编译 + `go vet` + 配置/编排一致性核对

---

## 修复状态（2026-09-22 更新）

本报告的 P0/P1 问题在陆续修复。**下文描述的是修复前的状态**，
保留原文以便对照「问题 → 根因 → 修法」的分析过程。

| 问题 | 状态 | 提交 |
|------|------|------|
| P0-1 访问日志只在缓存未命中分支发送 | ✅ 已修复 | `01ed3b8`，详见 [P0-1-FIX.md](P0-1-FIX.md) |
| P0-2 Docker 内 Kafka 跨容器连接失败 | ✅ 已修复 | `71cc9ec` |
| P0-7 零测试、零 CI | ⏳ 未修 | — |
| P0-3 / P0-4 / P0-5 / P0-6 / P0-8 | ⏳ 未修 | — |
| P1-7 消费端逐条写库（无批量） | ⏳ 未修，已写入 README「已知瓶颈」 | — |

> 注：P0-1 修复后每条请求都产生一条 Kafka 消息，消费端随即暴露为瓶颈
> （生产 ≈7385 条/秒 vs 消费 ≈25 条/秒）。这不是修复引入的缺陷，而是修复
> 让既存的 P1-7 显性化。

---

## 0. 结论速览

| 维度 | 评价 | 说明 |
|------|------|------|
| 架构设计 | **良好** | go.work 五模块边界清晰；三级查询链路、Kafka 异步解耦的设计选型合理 |
| 代码可编译性 | **优秀** | 5 个模块 `go build` 全部通过，`go vet` 零告警（实测） |
| 功能完整性 | **有硬伤** | 用户系统整体未接线（死代码）；访问日志链路在热路径上被跳过；统计口径错误 |
| 测试覆盖 | **缺失** | 全仓库 **0 个 `_test.go`**，无 CI |
| 文档质量 | **表达好、数据有误** | 文档结构完整、可读性强，但布隆过滤器误判率、编排网络等描述与实现/数学不符 |
| **简历可用度** | **需先修 P0** | 当前状态下，面试官只要"点一次短链再看统计"就能发现 PV 不涨 |

**一句话**：这是一份"架构讲得比代码跑得通"的项目。骨架和文档的优秀程度明显高于实现完整度——**最致命的是重定向热路径不写日志，导致访问统计几乎永远为 0**，而这恰好是这类项目最容易被当场验证的功能。

---

## 1. 项目画像

| 项目 | 实测值 |
|------|--------|
| 语言 / 工具链 | Go（go.mod 声明 1.24.0，本机 go1.26.1 编译通过） |
| 模块数 | 5（`common` / `rpc/link` / `rpc/stats` / `api/gateway` / `service/logconsumer`）via `go.work` |
| 手写 Go 代码 | **约 1,574 行**（common 390 · rpc 545 · api 360 · service 123 · scripts 156） |
| 生成代码 | `*.pb.go` 4 个文件（goctl 1.10.1 生成，不手改） |
| 提交历史 | 14 个提交，跨度 2026-06-05 ~ 2026-06-08 |
| 分支 | `master`（与 `origin/master` 同步，工作区干净） |
| 外部依赖 | go-zero v1.10.2、GORM v1.25.5、go-redis v8.11.5、kafka-go v0.4.47、golang-jwt v5、etcd v3.5.21 |
| 部署 | docker-compose 8 服务（4 应用 + mysql/redis/etcd/kafka） |
| 测试 | **0 个测试文件** |
| 遗留代码 | `_legacy/`（早期 Gin 版本，已 gitignore 但仍在磁盘） |

### 1.1 模块依赖关系（实测，比文档更准确）

```
api/gateway  ──▶ rpc/link  ──▶ common
     │              │
     ├──▶ rpc/stats ┘
     │
service/logconsumer ──▶ common
```

`common` 被所有模块依赖且自身无内部依赖 → **这是一个正确的、无环的依赖方向**，值得在面试里讲。

---

## 2. 关键链路与设计评价

### 2.1 重定向（`GET /:code`）— 项目的性能主战场

```
HTTP :8888 → Gateway.redirectHandler
            → gRPC LinkRpc.Redirect            ← 1 次跨进程调用
               ├─ BloomFilter.Test             ← 7 次串行 Redis GETBIT
               ├─ Redis GET link:{code}        ← 1 次 Redis GET
               └─ (miss) MySQL → 回写 Redis → go Kafka.Send  ← 异步
            → 302 Found
```

**设计合理之处**

- 三级查询把"不存在的短码"挡在第一步，避免 MySQL 穿透，方向正确。
- 写路径 `MySQL INSERT → Bloom.Add → Redis SET` 与读路径顺序一致，不存在"缓存里有、布隆里没有"的窗口。
- 302（`http.StatusFound`）而非 301 —— 短链改造/失效可控，选型正确；且 handler 直接 `http.Redirect` 而不是返回 JSON，符合浏览器语义。

**问题（详见 §4）**

- **异步 Kafka 发送被写在了 MySQL 分支里**，缓存命中直接 `return`，热路径完全不产生访问日志。
- 布隆过滤器是 **fail-closed**：`exists, _ := Test(...)` 丢弃 error，Redis 抖动 → 全站 404，无降级。
- 布隆的 7 个 bit 是 **7 次串行 RTT**，未 pipeline / 未 Lua，自身就吃掉约 11% 的 P50 延迟（文档自述）。
- `status` / `expire_at` 只在 MySQL 分支校验，缓存命中时被绕过。

### 2.2 生成（`POST /api/shorten`）

`ShortenLogic`：URL 归一化（自动补 `https://`）→ 校验 → 自定义码查重 → Snowflake 取主键 → Base62 编码 → INSERT → 写布隆 → 写缓存。

设计亮点：**自定义短码时仍生成 Snowflake 主键**，保证主键语义不因"用户指定码"而分裂，这个细节考虑得比多数同类项目好。

问题：查重是 `First` 后 `Create` 的 **TOCTOU**；`First` 返回非 `ErrRecordNotFound` 的 DB 错误时被当作"码未占用"继续插入；`BloomFilter.Add` 的 error 被丢弃（一旦 Add 失败，该链接后续会被布隆永久误判为"不存在"）。

### 2.3 统计（`GET /api/stats/:code`）

`StatsRpc.GetStats` 查 `links` 一行 + `link_stats` **一行**（`First`，无 `ORDER BY`，无 `SUM`）→ 多天数据时只返回最早那天的 PV。字段 `click_count` 直接等于单日 PV。

### 2.4 部署编排

多阶段构建处理得当：`GOWORK=off` + 相对 `replace ../../common` 的目录布局在镜像内正确匹配（`/app/rpc/link` → `/app/common`），编译期无问题。两套配置（`127.0.0.1` 本地 / 服务名 Docker）分离也是对的。

---

## 3. 值得肯定的地方（面试可直接讲的亮点）

1. **依赖方向无环**：`common` 作为叶子模块被复用，`rpc/link` 与 `rpc/stats` 之间无耦合，gateway 只做编排不碰 DB。
2. **自定义短码 + Snowflake 主键共存**的主键语义设计，是一个容易被追问且有话可说的点。
3. **Base62 写得很干净**：栈上定长 buffer、零堆分配、从右向左填充后切片返回；`id == 0` 显式处理避免空串。
4. **Snowflake 参数自洽**：`10bit worker + 12bit seq → timeShift = 22`，`epoch=1672531200000`（2023-01-01 UTC）与文档一致，代码与文档没有对不上。
5. **Kafka 生产者参数有思考**：`LeastBytes` 分区均衡 + `BatchTimeout 10ms` / `BatchSize 100` 攒批。
6. **文档工程量足**：README + TECHNICAL.md 覆盖架构/数据模型/接口/部署/性能，还有耗时分解表——这是很多学生项目完全缺失的。
7. **编译与静态检查零告警**（本次实测），说明没有"贴上去就不管了"的代码。

---

## 4. 问题清单（按严重度排序）

### P0 — 功能性缺陷，必须修

#### P0-1　访问日志只在「缓存未命中」分支发送 → 统计数据几乎永远为 0 ⚠️ 最严重

`rpc/link/internal/logic/redirectLogic.go:43-46`

```go
longURL, err := l.svcCtx.RedisClient.Get(l.ctx, cacheKey(code)).Result()
if err == nil && longURL != "" {
    return &pb.RedirectResponse{LongUrl: longURL}, nil   // ← 直接返回，下面的 Kafka 永不执行
}
...
go func() { ... SendAccessLog ... }()                    // ← 只有走 MySQL 才会到这里
```

而 `shortenLogic` 在创建时就把链接写进了 Redis（TTL 3600s）。**结论**：一条短链创建后的绝大多数访问都命中缓存 → 不发 Kafka → `access_logs` 与 `link_stats` 不增长。

**实际效果**：PV 大约每（链接 · 小时）才 +1，`/api/stats/:code` 永远返回 0 或 1。README/TECHNICAL 里"Kafka 异步解耦访问日志""按天统计 PV/UV"在运行时是**不成立的**。

而且这个 bug 在压测里完全看不出来——因为错误只被 `logx.Errorf` 记一行日志，QPS/P50 全都正常。

**修法**：把 `go func(){ SendAccessLog }()` 提到 `Redis GET` 之前（或至少在缓存命中分支补一次），并补齐 IP / UA / Referer 字段（目前只塞了 `ShortCode` 和 `Timestamp`）。

#### P0-2　Docker 内 Kafka 跨容器连接必然失败（且被静默吞掉）

`docker-compose.yml:67`

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

`link-rpc` 与 `logconsumer` 都用 `kafka:9092` 作为 seed broker，但 Kafka 在 metadata 里返回的是 advertised 地址 `localhost:9092`，客户端随后会去连**自己容器内**的 9092 → 连接被拒。

叠加 P0-1 的"错误只打日志"，这个故障在 Docker 环境下**完全静默**。

**修法**：advertised 改为 `kafka:9092`，或配双 listener（`PLAINTEXT` internal + `PLAINTEXT_HOST` 供宿主）。

#### P0-3　UV 永远是 1，README 的「按天去重 UV」不成立

`service/logconsumer/main.go:115-121` 的 upsert 只更新 `pv`：

```go
DoUpdates: clause.Assignments(map[string]interface{}{
    "pv":         gorm.Expr("link_stats.pv + 1"),
    "updated_at": time.Now(),
})
```

`uv` 仅在 INSERT 时置 1，之后永不变化。TECHNICAL.md 里写了"UV 当前实现简化"（诚实），但 README 第 4 节直接宣称"按天去重 UV"——**两份文档互相矛盾**，面试时容易被抓。

**修法**：要么用 Redis `PFADD/ PFCOUNT`（HyperLogLog）算 UV，要么用 `access_logs` 的 `COUNT(DISTINCT ip)` 定时回填，要么就把 README 改成"UV 待实现"。

#### P0-4　统计接口口径错误：跨天数据只返回单日 PV

`rpc/stats/internal/logic/statsLogic.go:39-46`

```go
var stat model.LinkStat
l.svcCtx.DB.WithContext(l.ctx).Where("short_code = ?", code).First(&stat)  // 单行 + 无 ORDER BY
...
ClickCount: uint64(rpcResp.Pv)   // 实际是"最早那天的 PV"
```

`link_stats` 是 `(short_code, date)` 多行结构，但这里既没 `SUM(pv)` 也没查 `uv` 的汇总。**另外这行的 error 被完全忽略**——DB 出错或没记录时静默返回 0，调用方无法区分"确实 0 次"和"查询失败"。

**修法**：`SELECT short_code, SUM(pv) AS pv, SUM(uv) AS uv FROM link_stats WHERE short_code=? GROUP BY short_code`。

#### P0-5　用户系统整体未接线（大量死代码）

实测引用计数为 **0** 的组件：

| 组件 | 状态 |
|------|------|
| `common/model/user.go`（`User`） | 无任何引用；`AutoMigrate` 里也没有它 |
| `common/utils/jwt.go`（`GenerateToken` / `ParseToken`） | `GenerateToken` 从未被调用 |
| `common/middleware/auth.go`（`AuthHandler`） | **未挂到任何路由**（`routes.go` 只有 3 条无鉴权路由） |
| `common/utils/password.go`（bcrypt） | 无调用方 |
| `common/errcode` | 新代码零引用（只有 `_legacy/` 用） |
| `base62.Decode` | 无调用方 |

`gateway.api` 里也没有 `/login` `/register`。同时 `shortenLogic` 里 **`UserID: 0` 硬编码**，`links.user_id` 永远是 0，ER 图里 `users → links` 的关系实际不存在。

**影响**：README 说"Phase 2 完成用户系统"，但对外 API 完全看不到用户能力。这是"文档承诺 > 实现"的最大一处。

**修法（二选一）**：① 补 `/register` `/login`（bcrypt + JWT）+ 给 shorten 挂 `AuthHandler` 中间件、写入真实 `user_id`；② 若本期不做，就把 `users` 表/相关包移入 `_legacy/` 或 README 标注"未实现"，避免面试时被问穿。

#### P0-6　链接密码只存不校验，且明文落库

`shortenLogic.go:70-72` 把 `in.Password` 原样写入 `links.password`；`redirectLogic.go` **完全没有密码校验分支**。同时既然项目里已经有 `utils.HashPassword`（bcrypt），却没用在这条路径上。

**修法**：存 hash + 重定向时跳转到一个校验页，或直接把 `password` 字段与相关文档整体标注为"预留未实现"。

#### P0-7　零测试、零 CI

`find . -name "*_test.go"` → 空。对一个以"能不能进面试"为目标的项目，**没有测试**比"少个功能"更伤：`Base62`（往返一致性 + 边界）、`Snowflake`（并发生成唯一性）、`Bloom`（假阴性必须为 0）这三个纯函数是**零成本可测**的，恰好也是面试官最爱追问正确性的三处。

**建议**：优先补这 3 个包的单元测试（约 150 行），再加一个 GitHub Actions `go build + go vet + go test`。

另：仓库**没有 `.github/` 目录**（无任何 CI）、**没有 LICENSE**（实测），也没有 `.gitattributes`。

#### P0-8　全仓库换行符为 CRLF，未加 `.gitattributes`

实测 40 个 `.go` 文件 **100% 为 CRLF**（`gofmt -l` 会列出其中 12 个非生成文件，差异全是 `^M` 而非真实格式问题）。

**影响**：本身不影响编译（Go 接受 CRLF），但一旦在 CI 里加 `gofmt -l` 检查（Go 社区标准做法），**会全量失败**。修法：加 `.gitattributes`（`*.go text eol=lf`）后一次性 `gofmt -w` 归一化，或直接用 `* text=auto`。

---

### P1 — 健壮性与运维隐患

| # | 问题 | 位置 | 影响 |
|---|------|------|------|
| P1-1 | 布隆过滤器 fail-closed，`Test` 的 error 被 `_` 丢弃 | `redirectLogic.go:37` | Redis 抖动 → **全站 404**。应 fail-open（出错时放行到 Redis/DB）并降级告警 |
| P1-2 | `BloomFilter.Add` 的 error 被忽略 | `shortenLogic.go:79` | Add 失败 → 该短码被布隆永久"否定"，链接生成成功但永远打不开 |
| P1-3 | 自定义短码 TOCTOU（先 `First` 查重再 `Create`） | `shortenLogic.go:51-59` | 并发下靠唯一索引兜底，但错误信息是裸露的 `create link: ...`，应显式识别 duplicate key |
| P1-4 | `First(&existing)` 把 DB 错误当"未占用" | `shortenLogic.go:53-56` | DB 异常时会继续插入，掩盖真实故障 |
| P1-5 | 禁用/过期链接在缓存命中路径不生效 | `redirectLogic.go:43-46` | `status=0` 或已过期的链接仍会 302，且无缓存失效机制 |
| P1-6 | 无 singleflight、无 TTL 随机抖动 | `redirectLogic.go:60-61` | 热点 key 过期瞬间打穿到 MySQL；大量 key 同刻过期→雪崩。项目自己的 TODO 至今未做 |
| P1-7 | logconsumer 无死信/无重试，`ReadMessage` 隐式提交 offset | `main.go:80-121` | 处理失败的消息直接丢；且实现是**逐条写库**，与文档"消费者批量写入"不符 |
| P1-8 | gateway `depends_on` 缺 `stats-rpc` | `docker-compose.yml:137-139` | `zrpc.MustNewClient` 启动期阻塞，stats 未注册时 gateway 可能直接起不来 |
| P1-9 | 生产路径调用 `AutoMigrate`，3 个服务并发执行 DDL | 3 个 `serviceContext.go` + logconsumer | 多实例部署时 DDL 竞争；且 `AutoMigrate` 不含 `User`，与 `init_db.sql` 不一致 |
| P1-10 | Snowflake 未处理时钟回拨 | `snowflake.go:37-50` | NTP 向后跳时 `timestamp` 变小 + `sequence` 归零 → **可能生成重复 ID**（而 ID 是主键） |
| P1-11 | 布隆 7 次串行 `GETBIT` | `bloom.go:39-54` | 7 个 RTT；改 pipeline 或 Lua 一次往返即可，直接省下文档自述的那 11% 延迟 |
| P1-12 | `AccessLogMessage` 在两个包里重复定义 | `common/mq/producer.go:15` 与 `service/logconsumer/main.go:22` | 字段一改就得改两处，consumer 应直接引用 `common/mq` |

---

### P2 — 文档 / 工程整洁度

| # | 问题 | 说明 |
|---|------|------|
| P2-1 | **布隆误判率宣称 ~0.01%，与数学不符** | m=10,000,000 bit、k=7 时 `FPR=(1-e^{-kn/m})^7`：n=10万 → 6.5e-9；n=50万 → **0.02%**；n=100万 → **0.82%**；n=200万 → **13.8%**；n=500万 → **80.7%**。"0.01%"只在 n≤50 万时成立。要到 0.01% 需 m≈1920 万位。**面试官一旦追问布隆规模与容量的关系，这个数字会翻车** |
| P2-2 | "Bloom Filter 命中率 ~99.99%" 概念混淆 | 把"误判率"说成了"命中率"；n=100万时拦截率约 99.18% |
| P2-3 | 布隆实现与文档描述不符 | TECHNICAL.md 称"FNV-1a 的 double hashing 变体"，实际代码是 `fnv32a(data ‖ byte(i))`（加盐重哈希），并非 double hashing |
| P2-4 | 编排描述与实现不符 | 文档称"1 个自定义网络（bridge）"，`docker-compose.yml` 里**没有** `networks:` 段 |
| P2-5 | Kafka 卷路径错误 | `kafka_data:/bitnami/kafka` 是 bitnami 镜像的路径，当前用的是 `apache/kafka:3.9.0`（数据在 `/opt/kafka/...`）→ **数据实际未持久化** |
| P2-6 | 性能报告硬件与实际不符 | README 写 ASUS TUF F15 / i7-12700H / 15GB / Ubuntu 22.04，且是**另一台机器**上跑的；你的主力机是 F16 / i9-14900HX / 32GB Windows。若简历/面试引用这组数据，需能说清"那是我的 Ubuntu 测试机" |
| P2-7 | 默认凭证硬编码且服务端口直接发布到宿主 | `root:root123` 出现在 compose、3 个 yaml、2 个脚本；MySQL 3306 / Redis 6379（无密码）/ etcd 2379 / Kafka 9092 全部 `ports:` 暴露。可讲"生产会用 Secret + 内网隔离"，但代码里最好体现 |
| P2-8 | 文档说"Go 1.24 / 依赖 1024 节点"等细节与代码无关的过度承诺 | `SnowflakeWorkerID: 1` 固定写死，实际没有多节点部署方案；"水平扩展无压力"缺证据 |
| P2-9 | 残留物 | `scripts/benchserver/`（**空目录，未跟踪**）、`_legacy/`（已 gitignore 但仍在工作区，含旧 Gin 版本）、`.idea/` |
| P2-10 | `go.work.sum` 被 gitignore 但存在于磁盘 | Dockerfile 用 `COPY go.work.sum*` 兜住了，无功能影响，但属配置噪音 |
| P2-11 | 无 `LICENSE`、无 `.github/`（无 CI）、无 `.gitattributes` | 三项实测缺失；开源项目缺 LICENSE 会削弱"工程成熟度"印象 |
| P2-12 | 全仓 CRLF（40/40 个 `.go`） | 见 P0-8，会让未来的 `gofmt -l` CI 全量失败 |

### 已排除（实测无问题，不必担心）

- `git remote -v` → `https://github.com/Vierblatt/shortlink.git`，**URL 内无内嵌明文令牌**。
- 全仓扫描 `gho_` / `ghp_` / `github_pat_` / `sk-` / `AKIA` 形式的凭证 → **零命中**（只有配置里的 `root123` 默认密码，属 P2-7）。
- `go build` + `go vet` 五个模块全部零告警。

---

## 5. 性能数据的可信度复核

文档给出的 `QPS 20,852 / P50 4.53ms / P99 7.72ms`，**内部自洽性没问题**：

- 100 并发连接 ÷ 0.00453s ≈ **22,000 QPS**，与实测 20,852 吻合（含尾部损耗），说明不是编的。
- P99/P50 = 1.70，分布集中，与"无长尾"的结论一致。

但有三点需要在面试前想清楚：

1. **这次压测没有覆盖 Kafka 路径**（P0-1 导致缓存命中的请求压根不发消息），所以"Kafka 异步写入零阻塞"这条结论**没有被实验验证**，只是设计推断。
2. **耗时分解表（gRPC 22% / 布隆 11% / Redis 11% / bridge 11% / 其他 45%）是估算值**，文档没有说明是火焰图/pprof 实测还是推算。若被问"你怎么测出布隆占 11%"，需要有答案（建议用 `go test -bench` 或 pprof 真测一遍）。
3. 硬件是 i7-12700H/15GB 的 Ubuntu 机，与你简历上的主力机不同，**别把它当"我的电脑跑出来的"来讲**。

---

## 6. 改进路线图（按投入产出比排序）

### 第一步：让项目"能演示"（半天内，必须做）

1. 修 P0-1（Kafka 发送移到 Redis 命中之前）+ 补齐 IP/UA/Referer → **统计能动了**
2. 修 P0-2（advertised listener 改 `kafka:9092`）→ **Docker 下 Kafka 真的通**
3. 修 P0-4（`SUM(pv)` 聚合 + 处理 error）→ **统计接口数字对了**

做完 1-3，`curl 建链 → 多次访问 → 查 /api/stats` 这条 demo 链路才真正成立。

### 第二步：补上"简历承诺"（1~2 天）

4. 补 `/register` `/login` + JWT 中间件挂到 shorten，写入真实 `user_id`（用上已有的 `HashPassword`/`AuthHandler`）——否则删掉这批死代码并改 README
5. 补 `base62` / `snowflake` / `bloom` 三个包的单元测试 + GitHub Actions
6. 修 P1-1 / P1-2（布隆 fail-open + Add 必须报错），这是"高可用"话题的现成素材

### 第三步：把 P2 的数字改成"经得起追问的"（半天）

7. 重算并改写布隆误判率段落（给出 `n / m / k / FPR` 表格，主动说明容量上限）
8. 把"用户系统已完成"改为准确表述；把性能报告标注清楚**测试机型号**
9. 删 `_legacy/`、`scripts/benchserver/`；给 compose 补 `networks:` 或删掉文档里的描述
10. 加 `LICENSE` + `.gitattributes`（`*.go text eol=lf`）+ `.github/workflows/ci.yml`（`gofmt -l` / `go vet` / `go test` 三件套）
11. P1-6（singleflight + TTL 抖动）——这是**性价比最高的加分项**：代码量小、面试问得最多（穿透/击穿/雪崩三件套），做完能把 README 里那两个 TODO 关掉

### 可选加分项

- 布隆改 Lua 脚本 / pipeline（P1-11），顺手拿到"我优化掉了 11% 延迟"的实测对比
- `expire_at` 生效路径统一（P1-5）
- 加一个最小前端页面（`_legacy/web/index.html` 里其实已经有雏形）
- 给 Snowflake 补时钟回拨保护（P1-10，经典八股点）

---

## 7. 面试风险提示（重点）

按"面试官最可能点破的顺序"排列：

| 风险 | 触发方式 | 现在的答案 |
|------|---------|-----------|
| **统计永远是 0** | 建链 → 点 10 次 → 查 stats | 无。必被发现 |
| **用户系统是空的** | 看 `routes.go` / 问"怎么登录" | 无。README 说做了，实际是死代码 |
| **布隆 0.01% 算不出来** | 问"1000 万位能放多少链接" | 无。n>50 万就开始恶化 |
| **压测机不是本机** | 问"这数据在哪台跑的" | 需主动说明是 Ubuntu 测试机 |
| **性能分解是估算** | 问"怎么测出布隆占 11%" | 无实测依据 |
| 布隆挂 = 全站挂 | 问"Redis 挂了会怎样" | 现在会全站 404 |

**结论**：P0-1 ~ P0-6 是"会当场翻车"的，P2-1/P2-6 是"被追问会露出"的。二者都修完，这个项目的表达能力会从"讲得出"变成"经得起问"。
