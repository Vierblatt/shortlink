#!/usr/bin/env bash
# ============================================================================
# 全链路压测复现脚本
#
# README 记录的 QPS 20852 / P99 7.72ms 是在 Ubuntu 裸机（Linux 原生内核、
# 无虚拟化）上测得的。在 Windows + Docker Desktop 上跑同一套代码只能得到
# 约 7400 QPS —— 差异来自 WSL2/Hyper-V 虚拟化层，不是代码回归。
#
# 仓库历史记录了同一逻辑在不同环境下的差距：
#   Docker Desktop (Windows)                775 QPS
#   Ubuntu 22.04 裸机（完整 go-zero 链路）  23,435 QPS
#   Ubuntu 22.04 裸机（精简路径）           31,150 QPS
#
# 因此要在 Linux 上复现 README 的数字，请在 Linux 裸机（或 WSL2 中原生运行、
# 不经 Docker Desktop 转发）执行本脚本。
#
# 用法：
#   bash scripts/bench.sh              # 默认 4 线程 × 100 连接 × 30s
#   bash scripts/bench.sh 4 200 60     # 自定义 线程数 连接数 时长
#
# 前置：
#   1. 全栈已启动：docker compose up -d
#   2. 已安装 wrk（apt install wrk / 见下方各发行版说明）
#      - 若容器内压测，注意容器网络会引入额外开销，建议宿主机直跑
#   3. 已安装 python3（用于解析 JSON）
#
# 出口码：成功 0；缺少依赖 2；压测失败 1。
# ============================================================================

set -uo pipefail

THREADS="${1:-4}"
CONNECTIONS="${2:-100}"
DURATION="${3:-30}"
BASE_URL="${BASE_URL:-http://localhost:8888}"
# 短码限长 16 位（links.short_code varchar(16)）。用时间戳 + 随机数降低重复概率：
# 容器内 PID 常为小数值，仅用 $$ 会在多次运行间撞码。
# 长度 = 1 + 10(epoch) + 3(随机) = 14，留有余量。
BENCH_CODE="b$(date +%s)$((RANDOM % 900 + 100))"

need() {
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || { echo "缺少依赖: $c"; exit 2; }
  done
}

need wrk curl python3

# 环境自检：Docker Desktop 下压测结果不可用于横向比较
if grep -qiE "microsoft|wsl" /proc/version 2>/dev/null; then
  if [ -n "${DOCKER_HOST:-}" ] || docker info 2>/dev/null | grep -qi "Docker Desktop"; then
    echo "警告：检测到 Docker Desktop / WSL2 环境。"
    echo "      此环境下的 QPS 会显著低于 Linux 裸机（实测约 7400 vs 23435），"
    echo "      结果不适用于与 README 的数字横向比较。"
    echo
  fi
fi

echo "=== 1. 建短链 ==="
RESP=$(curl -s -m 10 -X POST "$BASE_URL/api/shorten" \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"https://example.com/bench\",\"custom_code\":\"$BENCH_CODE\"}")

CODE=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('code',''))" 2>/dev/null)
if [ -z "$CODE" ]; then
  echo "建短链失败，响应：$RESP"
  echo "（若提示已存在，换一个 code 或先清库）"
  exit 1
fi
echo "  短码: $CODE"

echo "=== 2. 预热（避开冷启动与缓存未命中的首次开销）==="
for _ in $(seq 1 200); do
  curl -s -o /dev/null "$BASE_URL/$CODE"
done
echo "  预热 200 次完成"

echo "=== 3. 压测：${THREADS} 线程 × ${CONNECTIONS} 连接 × ${DURATION}s ==="
echo "  目标: $BASE_URL/$CODE"
echo

wrk -t "$THREADS" -c "$CONNECTIONS" -d "${DURATION}s" --latency "$BASE_URL/$CODE"
RC=$?

echo
if [ $RC -ne 0 ]; then
  echo "压测异常退出（code=$RC）"
  exit 1
fi

cat <<'EOF'
=== 4. 结果判读 ===

对比 README 记录（Ubuntu 裸机）：
  QPS 20,852 | P50 4.53ms | P90 6.27ms | P99 7.72ms | 错误率 0%

若本机为 Docker Desktop / WSL2，得到约 7400 QPS 属预期，非代码问题。
判读瓶颈时可另开终端观察：

  docker stats --no-stream

若 link-rpc / gateway 的 CPU% 接近容器配额上限，瓶颈在容器 CPU；
若 Redis 接近 100%，瓶颈在 Redis 单线程；若 wrk 自身占满 CPU，
则压测端先饱和，需提高线程数或换机器。
EOF
