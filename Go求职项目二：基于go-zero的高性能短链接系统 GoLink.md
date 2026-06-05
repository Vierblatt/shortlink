# Go求职项目二：基于go\-zero的高性能短链接系统 GoLink

# Go 求职项目二：基于 go\-zero 的高性能短链接系统 GoLink

## 项目简介

GoLink 是一个基于 **go\-zero 微服务框架** 实现的生产级高性能短链接系统，支持短链接生成、重定向、访问统计、自定义短码、批量生成等核心功能。

该项目聚焦于**高并发、低延迟**场景，充分利用 go\-zero 的性能优势和微服务能力，解决了短链接系统的核心技术挑战：分布式唯一 ID 生成、高并发重定向、海量数据存储、实时访问统计等。项目代码简洁高效，技术亮点突出，是投递 Go 后端、基础架构、高并发开发岗位的绝佳项目。

## 技术栈

### 核心框架与组件

- **语言**: Go 1\.22\+

- **微服务框架**: go\-zero 1\.6\+

- **代码生成**: goctl

- **服务发现**: etcd

- **数据库**: MySQL 8\.0（持久化存储）

- **缓存**: Redis 7\.0（热点数据缓存、布隆过滤器）

- **消息队列**: Kafka（异步日志收集、访问统计）

- **ORM**: go\-zero 内置 GORM 集成

- **日志**: go\-zero 内置 Zap 结构化日志

- **监控**: go\-zero 内置 Prometheus \+ Grafana

- **链路追踪**: go\-zero 内置 OpenTelemetry

- **容器化**: Docker \+ Docker Compose

## 系统架构

```Plain Text
┌─────────────────┐
│    客户端       │
│  浏览器/API调用 │
└────────┬────────┘
         │ HTTP
         ▼
┌─────────────────────────────────────────┐
│         API 网关 GoGateway              │
│  统一入口 / 认证授权 / 限流熔断 / 日志监控 │
└───────────┬───────────────────┬─────────┘
            │ gRPC              │ gRPC
            ▼                   ▼
┌─────────────────┐     ┌─────────────────┐
│  短链接服务 RPC │     │  统计服务 RPC   │
│  生成/重定向    │     │  访问统计/分析  │
└─────────────────┘     └─────────────────┘
            │                   │
            │                   │ Kafka
            ▼                   ▼
┌─────────────────┐     ┌─────────────────┐
│  基础组件层     │     │  日志收集服务   │
│ MySQL / Redis   │     │  (异步写入)     │
│ etcd / Kafka    │     └─────────────────┘
└─────────────────┘
```

### 组件职责

1. **API 网关层**：对外提供统一 HTTP 接口，负责参数校验、认证授权、流量治理

2. **短链接服务 RPC**：核心服务，负责短链接生成、解析、重定向

3. **统计服务 RPC**：负责访问日志收集、数据统计和分析

4. **日志收集服务**：异步消费 Kafka 消息，批量写入数据库

5. **基础组件层**：提供数据存储、服务发现、消息队列等基础能力

## 核心功能

### 1\. 短链接生成

- 支持长链接转短链接

- 支持自定义短码

- 支持设置短链接有效期

- 支持批量生成短链接

- 支持短链接密码保护

### 2\. 短链接重定向

- 高并发低延迟重定向

- 支持 301/302 重定向

- 支持短链接状态检查

- 支持过期短链接处理

### 3\. 访问统计

- 实时访问量统计

- 访问来源统计

- 访问地域统计

- 访问设备统计

- 访问趋势分析

### 4\. 管理功能

- 短链接列表查询

- 短链接编辑与删除

- 短链接状态管理

- 用户权限管理

## 数据库设计

### 主要表结构

#### links 表（短链接信息）

```sql
CREATE TABLE `links` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `short_code` varchar(16) NOT NULL COMMENT '短码',
  `long_url` text NOT NULL COMMENT '原始长链接',
  `user_id` bigint unsigned NOT NULL COMMENT '创建用户ID',
  `expire_at` datetime DEFAULT NULL COMMENT '过期时间',
  `password` varchar(64) DEFAULT NULL COMMENT '访问密码',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '状态: 0-禁用, 1-启用',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_short_code` (`short_code`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_expire_at` (`expire_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### access\_logs 表（访问日志）

```sql
CREATE TABLE `access_logs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `short_code` varchar(16) NOT NULL COMMENT '短码',
  `ip` varchar(64) NOT NULL COMMENT '访问IP',
  `user_agent` text NOT NULL COMMENT '用户代理',
  `referer` text DEFAULT NULL COMMENT '来源页面',
  `country` varchar(32) DEFAULT NULL COMMENT '国家',
  `province` varchar(32) DEFAULT NULL COMMENT '省份',
  `city` varchar(32) DEFAULT NULL COMMENT '城市',
  `device` varchar(32) DEFAULT NULL COMMENT '设备类型',
  `browser` varchar(32) DEFAULT NULL COMMENT '浏览器',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_short_code` (`short_code`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### link\_stats 表（短链接统计）

```sql
CREATE TABLE `link_stats` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `short_code` varchar(16) NOT NULL COMMENT '短码',
  `date` date NOT NULL COMMENT '统计日期',
  `pv` int NOT NULL DEFAULT '0' COMMENT '页面浏览量',
  `uv` int NOT NULL DEFAULT '0' COMMENT '独立访客数',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_short_code_date` (`short_code`, `date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 关键技术实现

### 1\. 分布式唯一 ID 生成（雪花算法）

```go
type Snowflake struct {
    mu        sync.Mutex
    timestamp int64
    workerId  int64
    sequence  int64
}

const (
    workerIdBits  = 10
    sequenceBits  = 12
    maxWorkerId   = -1 ^ (-1 << workerIdBits)
    maxSequence   = -1 ^ (-1 << sequenceBits)
    timeShift     = workerIdBits + sequenceBits
    workerIdShift = sequenceBits
    epoch         = 1672531200000 // 2023-01-01 00:00:00
)

func NewSnowflake(workerId int64) (*Snowflake, error) {
    if workerId < 0 || workerId > maxWorkerId {
        return nil, errors.New("invalid worker id")
    }
    return &Snowflake{
        workerId: workerId,
    }, nil
}

func (s *Snowflake) Generate() int64 {
    s.mu.Lock()
    defer s.mu.Unlock()

    now := time.Now().UnixMilli()

    if now == s.timestamp {
        s.sequence = (s.sequence + 1) & maxSequence
        if s.sequence == 0 {
            for now <= s.timestamp {
                now = time.Now().UnixMilli()
            }
        }
    } else {
        s.sequence = 0
    }

    s.timestamp = now

    return (now-epoch)<<timeShift | (s.workerId << workerIdShift) | s.sequence
}
```

### 2\. 短码生成算法（Base62 编码）

```go
const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func Base62Encode(num int64) string {
    if num == 0 {
        return string(base62Chars[0])
    }

    var result []byte
    for num > 0 {
        remainder := num % 62
        result = append(result, base62Chars[remainder])
        num = num / 62
    }

    // 反转字符串
    for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
        result[i], result[j] = result[j], result[i]
    }

    return string(result)
}
```

### 3\. 高并发重定向实现

```go
func (l *RedirectLogic) Redirect(shortCode string) (string, error) {
    // 1. 先查布隆过滤器，快速过滤不存在的短码
    exists, err := l.svcCtx.BloomFilter.Test([]byte(shortCode))
    if err != nil {
        return "", err
    }
    if !exists {
        return "", errors.New("short link not found")
    }

    // 2. 再查Redis缓存
    longURL, err := l.svcCtx.RedisClient.Get(l.ctx, fmt.Sprintf("link:%s", shortCode)).Result()
    if err == nil && longURL != "" {
        // 异步发送访问日志到Kafka
        l.sendAccessLog(shortCode)
        return longURL, nil
    }

    // 3. 最后查数据库
    var link model.Link
    if err := l.svcCtx.DB.Where("short_code = ? AND status = 1", shortCode).First(&link).Error; err != nil {
        return "", errors.New("short link not found")
    }

    // 检查是否过期
    if link.ExpireAt != nil && time.Now().After(*link.ExpireAt) {
        return "", errors.New("short link has expired")
    }

    // 写入Redis缓存，设置过期时间
    l.svcCtx.RedisClient.Set(l.ctx, fmt.Sprintf("link:%s", shortCode), link.LongURL, 1*time.Hour)

    // 异步发送访问日志到Kafka
    l.sendAccessLog(shortCode)

    return link.LongURL, nil
}

func (l *RedirectLogic) sendAccessLog(shortCode string) {
    log := &AccessLogMessage{
        ShortCode: shortCode,
        IP:        l.ctx.Value("clientIP").(string),
        UserAgent: l.ctx.Value("userAgent").(string),
        Referer:   l.ctx.Value("referer").(string),
    }

    // 异步发送到Kafka，不阻塞主流程
    go func() {
        if err := l.svcCtx.KafkaProducer.SendMessage("access_logs", log); err != nil {
            logx.Error("send access log to kafka failed", zap.Error(err))
        }
    }()
}
```

### 4\. 布隆过滤器防止缓存穿透

```go
type BloomFilter struct {
    redisClient *redis.Client
    key         string
    size        int64
    hashFuncs   int
}

func NewBloomFilter(redisClient *redis.Client, key string, size int64, hashFuncs int) *BloomFilter {
    return &BloomFilter{
        redisClient: redisClient,
        key:         key,
        size:        size,
        hashFuncs:   hashFuncs,
    }
}

func (bf *BloomFilter) Add(data []byte) error {
    for i := 0; i < bf.hashFuncs; i++ {
        hash := fnv.New32a()
        hash.Write(data)
        hash.Write([]byte{byte(i)})
        index := hash.Sum32() % uint32(bf.size)
        
        if err := bf.redisClient.SetBit(context.Background(), bf.key, int64(index), 1).Err(); err != nil {
            return err
        }
    }
    return nil
}

func (bf *BloomFilter) Test(data []byte) (bool, error) {
    for i := 0; i < bf.hashFuncs; i++ {
        hash := fnv.New32a()
        hash.Write(data)
        hash.Write([]byte{byte(i)})
        index := hash.Sum32() % uint32(bf.size)
        
        bit, err := bf.redisClient.GetBit(context.Background(), bf.key, int64(index)).Result()
        if err != nil {
            return false, err
        }
        if bit == 0 {
            return false, nil
        }
    }
    return true, nil
}
```

## 项目目录结构

```Plain Text
GoLink/
├── api/                    # API 网关层
│   ├── gateway.api         # API 描述文件
│   ├── gateway.go          # API 服务入口
│   ├── config/             # 配置文件
│   ├── handler/            # 请求处理器
│   ├── logic/              # 业务逻辑
│   ├── svc/                # 服务上下文
│   └── types/              # 请求响应类型
├── rpc/                    # RPC 服务层
│   ├── link/               # 短链接服务
│   │   ├── link.proto      # RPC 描述文件
│   │   ├── link.go         # RPC 服务入口
│   │   ├── config/         # 配置文件
│   │   ├── logic/          # 业务逻辑
│   │   ├── svc/            # 服务上下文
│   │   └── pb/             # 生成的 protobuf 代码
│   └── stats/              # 统计服务
├── common/                 # 公共代码
│   ├── model/              # 数据库模型
│   ├── kafka/              # Kafka 客户端
│   ├── snowflake/          # 雪花算法
│   ├── bloom/              # 布隆过滤器
│   └── utils/              # 工具函数
├── deploy/                 # 部署文件
│   ├── docker/             # Dockerfile
│   └── docker-compose.yml  # Docker Compose 配置
├── scripts/                # 脚本文件
├── go.mod
└── go.sum
```

## 项目亮点与面试重点

1. **高并发设计**：展示了如何设计高并发低延迟的系统，包括缓存策略、异步处理、布隆过滤器等

2. **分布式问题解决**：实现了分布式唯一 ID 生成、分布式缓存、异步消息处理等

3. **性能优化**：通过缓存预热、批量写入、连接池优化等手段提升系统性能

4. **go\-zero 框架深入使用**：掌握 go\-zero 的微服务能力、性能优化技巧

5. **数据结构与算法**：展示了对雪花算法、Base62 编码、布隆过滤器等算法的理解和实现

6. **可扩展性**：系统架构清晰，易于扩展新功能

## 部署说明

使用 Docker Compose 一键部署：

```bash
# 克隆项目
git clone https://github.com/yourusername/golink.git
cd golink

# 启动基础组件
docker-compose up -d mysql redis etcd kafka

# 等待基础组件启动完成
sleep 60

# 初始化布隆过滤器，加载所有短码
go run scripts/init_bloom.go

# 启动所有服务
docker-compose up -d
```

## 扩展方向

1. 支持短链接二维码生成

2. 集成 IP 地址库，实现更精准的地域统计

3. 实现短链接访问实时监控大屏

4. 支持团队协作和权限管理

5. 实现短链接批量导入导出

需要我帮你把这个短链接系统也整理成和电商系统一样格式的**完整技术文档**吗？

> （注：文档部分内容可能由 AI 生成）
