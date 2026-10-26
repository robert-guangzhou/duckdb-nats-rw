#!/usr/bin/env bash
# 测试执行入口（Test Plan 第四章；minis Docker 模式）
# 用法：仓库根目录执行 ./test/scripts/run-all.sh
# 每次运行的输出归档到 tmp/test-output/<UTC时间戳>/，禁止覆盖历史目录。
set -euo pipefail
cd "$(dirname "$0")/.."

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="tmp/test-output/$TS"
mkdir -p "$OUT"

COMPOSE="docker compose -f docker/docker-compose.yml"

echo "[1/5] 起容器 (nats + builder)"
$COMPOSE up -d nats builder

echo "[2/5] 等待 nats healthz (builder 侧探测，最多 60s)"
$COMPOSE exec -T builder bash -c '
  for i in $(seq 1 30); do
    code=$(curl -s -o /dev/null -w "%{http_code}" http://nats:8222/healthz || true)
    [ "$code" = "200" ] && exit 0
    sleep 2
  done
  echo "nats healthz timeout" >&2; exit 1
'
echo "healthz ok"

echo "[3/5] 构建 (make debug)"
$COMPOSE exec -T builder make debug 2>&1 | tee "$OUT/build-debug.log"

echo "[4/5] SQL 测试 (make test_debug)"
$COMPOSE exec -T builder make test_debug 2>&1 | tee "$OUT/test.log"

echo "[5/5] 完成，输出已归档: $OUT"
