package handler

import (
	"net"
	"net/http"
	"strings"
	"unicode/utf8"
)

// maxMetaLen 限制 User-Agent / Referer 的长度。
// HTTP 头最大可达 1MB（http.Server.MaxHeaderBytes），不限制时单个请求
// 就能把大量数据写进 Kafka 和 MySQL，属于可被放大的写入面。
const maxMetaLen = 512

// clientIP 提取客户端 IP。优先取 X-Forwarded-For 的第一段——网关前若
// 有反代/LB，RemoteAddr 只会是代理地址。
//
// 注意：X-Forwarded-For 由客户端提供、可被伪造。此处用途是访问日志统计
// 而非访问控制，故接受该局限；若将来用于鉴权或限流，必须改为只信任
// 已知代理链中的地址。
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			xff = xff[:i]
		}
		if ip := strings.TrimSpace(xff); ip != "" {
			return ip
		}
	}

	// RemoteAddr 形如 "1.2.3.4:5678" 或 "[::1]:5678"。
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// truncate 按字节截断，并保证不切出半个 UTF-8 序列（否则落库后是乱码）。
func truncate(s string) string {
	if len(s) <= maxMetaLen {
		return s
	}
	// 从限制处向前找到最后一个完整字符的边界。只在续字节（10xxxxxx）上
	// 回退是不够的——那会留下一个缺少后续字节的前导字节，仍然是非法序列。
	cut := maxMetaLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
