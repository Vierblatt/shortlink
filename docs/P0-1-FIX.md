# P0-1 修复与验证记录

**日期**：2026-09-22
**问题**：`ANALYSIS.md` P0-1 —— 缓存命中的请求不写访问日志
**结论**：已修复；并顺带修复了 IP/UA/Referer 从未落库的问题

---

## 1. 问题确认

`rpc/link/internal/logic/redirectLogic.go` 原实现：

```go
longURL, err := l.svcCtx.RedisClient.Get(l.ctx, cacheKey(code)).Result()
if err == nil && longURL != "" {
    return &pb.RedirectResponse{LongUrl: longURL}, nil   // ← 缓存命中直接返回
}
// ...MySQL 回源之后才发 Kafka
```

Kafka 投递位于 MySQL 回源分支之后。而 `shortenLogic.go:83` 在**建链时**就写入缓存，
因此短链创建后几乎全部请求都走缓存命中分支 —— 访问日志基本不会被写下。
压测（QPS 20852）跑的正是这条路径，故「Kafka 异步日志统计」从未被压测覆盖。

同时 `AccessLogMessage` 的 `IP` / `UserAgent` / `Referer` 三个字段在
producer 侧从未被赋值，`RedirectRequest` 也没有对应字段可传。

## 2. 改动

| 文件 | 改动 |
|---|---|
| `rpc/link/link.proto` | `RedirectRequest` 新增 `ip` / `user_agent` / `referer` |
| `rpc/link/pb/link.pb.go` | 由 protoc 重新生成 |
| `rpc/link/internal/logic/redirectLogic.go` | 抽出 `sendLog` 闭包，缓存命中与回源两条路径都调用；补齐三个字段 |
| `api/gateway/internal/handler/clientinfo.go` | 新增：`clientIP`（取 XFF 首段，回退 RemoteAddr）、`truncate`（按 rune 边界截断到 512B） |
| `api/gateway/internal/handler/redirectHandler.go` | 提取客户端信息并传入 logic |
| `api/gateway/internal/logic/redirectLogic.go` | 新增 `SetClientInfo`，随 RPC 请求传递 |

设计取舍：

- Kafka 投递仍用 `context.Background()`：请求 ctx 在返回后即被取消，
  用它会导致异步投递被中断。
- `truncate` 按 rune 边界截断而非按续字节回退。后者会留下缺少后续字节的
  前导字节，仍是非法 UTF-8，落库即乱码。
- `clientIP` 的 XFF 可被客户端伪造，此处仅用于日志统计；代码注释中已标注
  若将来用于限流/鉴权必须改为只信任已知代理链。

## 3. 验证

### 3.1 端到端链路（功能）

构造冷启动场景：直接向 MySQL 插入短链（绕过 `Shorten`，缓存不预热），
并同步置位布隆过滤器，然后访问 3 次。

| 版本 | 3 次访问 → Kafka 消息数 |
|---|---|
| 修复前（`golink-link:baseline`） | **1** 条（仅第 1 次回源） |
| 修复后（`golink-link:local`） | **3** 条（1 回源 + 2 缓存命中） |

修复后 `access_logs` 表与 `link_stats` 均正确落库：

```
id  short_code   ip           user_agent     referer
6   coldtest01   172.21.0.1   FixProbe/1     https://ref.example.com/f1
7   coldtest01   172.21.0.1   FixProbe/2     https://ref.example.com/f2
8   coldtest01   172.21.0.1   FixProbe/3     https://ref.example.com/f3

link_stats: coldtest01 | 2026-09-22 | pv=3 | uv=1
```

### 3.2 压测对照（性能）

`wrk -t4 -c100 -d30s --latency`，同一场景、同一机器、连续两次运行：

| 版本 | QPS | P50 | P99 | Max |
|---|---|---|---|---|
| 修复前 | 7432 | 12.99 ms | 25.06 ms | 77.26 ms |
| 修复后 | 7386 | 13.11 ms | 24.85 ms | 55.80 ms |

差异 0.6%，在噪声范围内 —— **修复未引入可测量的性能回归**。

### 3.3 与 README 的 20852 QPS 差异说明

本次实测 7385 QPS，低于 README 记录的 20852。原因是**测试环境不同**，
不是代码回归。仓库历史里对此有明确记录：

| 环境 | QPS |
|---|---|
| Docker Desktop (Windows) | 775 |
| Ubuntu 22.04 裸机（完整 go-zero 链路） | 23,435 |
| Ubuntu 22.04 裸机（精简路径） | 31,150 |

（数据来自提交 `c170600` / `2fff062` 的压测报告）

README 的 20852 接近「Ubuntu 裸机完整链路」，本次测量在
**Windows 11 + Docker Desktop** 上完成，瓶颈是 WSL2 虚拟化层
（压测时 link-rpc 645% CPU、gateway 413%、Redis 94%，纯 CPU 受限）。

## 4. 修复暴露出的下游瓶颈

修复后每条请求都产生一条 Kafka 消息，消费端随即暴露为瓶颈：

```
生产速率 ≈ 7385 条/秒
消费速率 ≈ 25 条/秒      （10 秒采样：offset 2316 → 2566 → 2820）
LAG      ≈ 230,966 条
```

`logconsumer` 逐条 `ReadMessage` + 逐条 `db.Create`，无批量写入
（`ANALYSIS.md` P1-7 已记录）。原实现下约 2/3 的请求不产生消息，
该瓶颈被掩盖；修复后它成为必须处理的问题。

**这不是本次修复引入的缺陷，而是修复让既存缺陷显性化。**
若要支撑生产流量，消费端需改为批量写入 + 批量提交 offset。

## 5. 验证过程中发现的环境问题（均与业务代码无关）

### 5.1 Kafka advertised listener（P0-2）

`KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` 使容器内客户端
被重定向到自身容器：

```
kafka.(*Client).Produce: dial tcp [::1]:9092: connect: connection refused
```

改为 `PLAINTEXT://kafka:9092` 后正常（所有客户端在同一 Docker 网络内）。

### 5.2 单节点 broker 的内部 topic 复制因子

`apache/kafka:3.9.0` 未设置 `offsets.topic.replication.factor`，
单节点下 `__consumer_offsets` 无法创建，消费组协调器始终不可用：

```
[15] Group Coordinator Not Available
```

显式设置以下项后恢复：

```
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
```

### 5.3 数据卷挂载点

`docker-compose.yml` 将 Kafka 数据卷挂到 `/bitnami/kafka`，但使用的是
`apache/kafka` 官方镜像（实际 log dir 为 `/tmp/kafka-logs`），挂载点无效。

### 5.4 消费组首次加入需重启

kafka-go v0.4.47 在首次加入消费组时可能分配到 0 个分区
（组状态 `Stable` 但 `#PARTITIONS` 为空），重启 consumer 后恢复。

> 5.1–5.4 的修复均在**临时 override 文件**中完成，未改动仓库的
> `docker-compose.yml`。建议评估后合并进仓库。

## 6. 遗留

- `AccessLogMessage` 在 `common/mq/producer.go` 与 `service/logconsumer/main.go:22`
  两处重复定义（`ANALYSIS.md` P1-12）。本次未合并，因属于独立重构。
  当前两份定义字段名一致，JSON 契约不受影响。
- 消费端批量化（见第 4 节）。
- 压测若要复现 README 的 20852，需在 Linux 裸机或 WSL2 原生环境进行。
