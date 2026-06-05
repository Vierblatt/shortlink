package config

import (
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	MySQL struct {
		DataSource string
	}
	Redis struct {
		Addr     string
		Password string `json:",optional"`
		DB       int    `json:",default=0"`
	}
	BloomFilter struct {
		Key       string
		Size      int64
		HashFuncs int
	}
	CacheTTL          int    `json:",default=3600"`
	ShortLinkDomain   string `json:",optional"`
	SnowflakeWorkerID int64  `json:",default=1"`
}
