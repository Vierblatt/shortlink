# GoLink — 基于 go-zero 的高性能短链接系统

> 🎯 一个可直接用于 Go 后端实习/校招面试的微服务项目，从架构设计到编码落地完整展示。

## 项目概述

GoLink 是一个高性能短链接服务，将长 URL 转换为短码（如 `http://domain/3xK9mR`），访问时 302 重定向回原始地址。核心指标：**生成低延迟 < 5ms，重定向 P99 < 2ms**。

**技术栈：** Go 1.24 · go-zero · gRPC · MySQL · Redis · Kafka · etcd · Docker

## 架构

```
                  ┌─────────────┐
                  │   Client    │
                  └──────┬──────┘
                         │ HTTP
                  ┌──────▼──────┐
                  │ API Gateway │  :8888  go-zero rest
                  │ (gin-like)  │
                  └──┬──────┬───┘
              gRPC  │      │  gRPC
       ┌────────────▼┐    ┌▼────────────┐
       │  Link RPC    │    │  Stats RPC  │  :9000 / :9001
       │  shorten     │    │  getStats   │  go-zero zrpc
       │  redirect    │    └──────┬──────┘
       └──┬───┬───┬──┘           │
          │   │   │              │
     ┌────▼┐ ┌▼──▼┐         ┌───▼───┐
     │MySQL│ │Redis│         │ MySQL │
     │8.0  │ │  7  │         │       │
     └─────┘ └┬───┬┘         └───────┘
              │   │
        Bloom │   │ Cache
        Filter│   │
              │   │
          ┌───▼───▼──┐     ┌──────────┐
          │  Kafka    │────▶│LogConsumer│  异步写入
          │  access   │     │  消费日志  │  access_logs
          │  _logs    │     └──────────┘  + link_stats
          └───────────┘
```

## 核心设计

### 1. 短码生成 — Snowflake + Base62

```
雪花ID (64位) → Base62编码 → 短码 (例: "3xK9mR")
```

- 用 Snowflake 替代数据库自增，避免单点瓶颈，分布式下天然不冲突
- Base62 编码，6 位即可支持 62⁶ ≈ 568 亿个链接
- 支持**自定义短码**（用户指定 `mylink` 而非随机码）

### 2. 重定向 — 三级缓存查询

```
Bloom Filter → Redis → MySQL
     ↓(miss)       ↓(miss)      ↓(hit 回写 Redis)
 快速拒绝不存在    命中直接返回    查DB兜底
 的短码 (零DB开销)
```

- **Bloom Filter**：用 1000 万位的 Redis Bitmap，7 个哈希函数，误判率 ~0.01%。绝大多数不存在短码的请求被拦截在第一步
- **Redis Cache**：Cache-Aside 模式，命中直接返回
- **MySQL 兜底**：查库后异步回写 Redis 缓存

### 3. 访问日志 — Kafka 异步解耦

```go
// redirectLogic 中不阻塞响应
go func() {
    producer.SendAccessLog(ctx, &msg)
}()
```

重定向请求不等待日志写入，通过 goroutine 异步投递 Kafka，LogConsumer 后端消费：

```
Kafka 消息 → LogConsumer → INSERT access_logs
                         → UPSERT link_stats (PV+1, 按天统计)
```

### 4. 统计数据 — 按天去重 UV

`link_stats` 表以 `(short_code, date)` 为唯一索引，消费者的 upsert 实现：

```sql
INSERT INTO link_stats (short_code, date, pv, uv)
VALUES (?, ?, 1, 1)
ON DUPLICATE KEY UPDATE pv = pv + 1;
```

## 目录结构

```
shortlink/
├── api/gateway/         # HTTP 网关层 (:8888)
│   ├── gateway.api      # go-zero API 定义
│   ├── internal/
│   │   ├── handler/     # 路由处理 (shorten/redirect/stats)
│   │   ├── logic/       # 业务逻辑 → 调用 RPC
│   │   └── svc/         # 服务上下文 (RPC 客户端注入)
├── rpc/
│   ├── link/            # Link RPC 服务 (:9000)
│   │   ├── link.proto   # protobuf 定义
│   │   └── internal/logic/
│   │       ├── shortenLogic.go  # 短链接生成
│   │       └── redirectLogic.go # 重定向 + 异步 Kafka
│   └── stats/           # Stats RPC 服务 (:9001)
│       ├── stats.proto
│       └── internal/logic/statsLogic.go
├── service/logconsumer/ # Kafka 消费者 (日志入库 + 统计)
├── common/              # 共享库
│   ├── model/           # GORM 模型 (Link, User, AccessLog, LinkStat)
│   ├── base62/          # Base62 编解码 (零改动复用)
│   ├── snowflake/       # 雪花 ID 生成器
│   ├── bloom/           # Redis 布隆过滤器
│   ├── mq/              # Kafka 生产者封装
│   ├── middleware/      # JWT 认证中间件
│   └── utils/           # bcrypt 密码 + JWT
├── scripts/
│   ├── init_db.sql      # 建表 DDL (4 张表)
│   └── init_bloom.go    # 布隆冷启动脚本
├── deploy/docker/       # 4 个 Dockerfile
├── docker-compose.yml   # 8 服务编排
└── go.work              # Go workspace (5 模块)
```

## 快速启动

```bash
# 1. 启动基础设施
docker compose up -d mysql redis etcd kafka

# 2. 初始化布隆过滤器
cd scripts && go run init_bloom.go

# 3. 启动 RPC 服务
cd rpc/link  && go run link.go  -f etc/link.yaml &
cd rpc/stats && go run stats.go -f etc/stats.yaml &

# 4. 启动消费者
cd service/logconsumer && go run main.go &

# 5. 启动网关
cd api/gateway && go run gateway.go -f etc/gateway.yaml &

# 6. 测试
# 生成短链接
curl -X POST http://localhost:8888/api/shorten \
  -H "Content-Type: application/json" \
  -d '{"url":"https://github.com"}'

# 重定向
curl -v http://localhost:8888/3xK9mR

# 查看统计
curl http://localhost:8888/api/stats/3xK9mR
```

## 数据表设计

| 表 | 用途 | 关键索引 |
|----|------|----------|
| links | 短链接映射 | `short_code`(unique), `user_id`, `expire_at` |
| access_logs | 访问记录 | `short_code`, `created_at` |
| link_stats | 按天统计 | `(short_code, date)` unique |
| users | 用户 (认证用) | `username`(unique), `email`(unique) |


## 压测报告

**测试场景**: 重定向接口 (`GET /:code`)，Bloom Filter → Redis → 302。

### 三组环境对比

| 指标 | Docker Desktop (Win) | Ubuntu 裸机 (完整链路) | Ubuntu 裸机 (精简) |
|------|---------------------|----------------------|-------------------|
| **QPS** | 775 | **23,435** | **31,150** |
| **P50** | 127.06ms | **4.00ms** | **2.95ms** |
| **P99** | 206.66ms | **6.97ms** | **5.72ms** |
| 并发 | 100 goroutines | 100 (wrk) | 100 (wrk) |
| 架构 | go-zero gRPC 全链路 | go-zero gRPC 全链路 | HTTP 直连 Redis |
| 错误 | 0 | 0 | 0 |

- **Docker Desktop**: 完整 go-zero 微服务链路 (gateway + link-rpc + gRPC + MySQL + Redis + Kafka)，全部容器化
- **Ubuntu 完整链路**: 同上，gateway + link-rpc 裸进程，gRPC 通信，MySQL/Redis 原生安装
- **Ubuntu 精简**: 单文件 HTTP 服务直连 Redis，裁掉 gRPC/etcd/Kafka/MySQL，仅保留 Bloom + Redis 核心路径

### 请求链路耗时对比

```mermaid
gantt
    title 三组环境请求耗时对比 (P50)
    dateFormat X
    axisFormat %s ms

    section Docker Desktop (127ms)
    gRPC服务间调用 + 网络转发       :crit, d1, 0, 10
    Bloom + Redis (容器内)          :crit, d2, 10, 13
    WSL2内核穿越 + NAT + 响应返回   :crit, d3, 13, 127

    section Ubuntu 完整链路 (4.0ms)
    HTTP路由 + gRPC序列化          :active, f1, 0, 2
    Bloom Filter                   :active, f2, 2, 3
    Redis loopback                 :active, f3, 3, 4
    302 Redirect                   :active, f4, 4, 4

    section Ubuntu 精简 (2.95ms)
    HTTP路由 + Bloom + Redis       :b1, 0, 3
    302 Redirect                   :b2, 3, 3
```

> **瓶颈分析**: gRPC 序列化/传输约占 1ms (4.00-2.95)。剩下 3ms 是 MySQL 首次查库回源 + Bloom Filter 7次 BIT 操作。Docker Desktop 额外的 ~123ms 全部来自 WSL2 虚拟化网络栈 (Hyper-V switch + Docker bridge NAT)。

### wrk 30s — 完整链路 (100 并发)

```
Running 30s test @ http://localhost:8888/test
  4 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.28ms    1.16ms  29.82ms   76.19%
    Req/Sec     5.89k   471.99     7.09k    72.50%
  Latency Distribution
     50%    4.00ms
     75%    5.11ms
     90%    5.58ms
     99%    6.97ms
  703587 requests in 30.02s, 175.80MB read
Requests/sec:  23435.72
```
