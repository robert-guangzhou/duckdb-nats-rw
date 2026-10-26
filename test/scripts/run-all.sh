#!/usr/bin/env bash
# 测试执行入口（Test Plan 第四章；minis Docker 模式）
# 用法：仓库根目录执行 ./test/scripts/run-all.sh
# 每次运行的输出归档到 tmp/test-output/<UTC时间戳>/，禁止覆盖历史目录。
# SQL 测试以单进程直跑（官方 runner 多 worker 在 29GB 内存下会 OOM，见计划阶段一记录）。
set -euo pipefail
cd "$(dirname "$0")/.."

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="tmp/test-output/$TS"
mkdir -p "$OUT"

COMPOSE="docker compose -f docker/docker-compose.yml"

echo "[1/6] 起容器 (nats + builder)"
$COMPOSE up -d nats builder

echo "[2/6] 等待 nats healthz (builder 侧探测，最多 60s)"
$COMPOSE exec -T builder bash -c '
  for i in $(seq 1 30); do
    code=$(curl -s -o /dev/null -w "%{http_code}" http://nats:8222/healthz || true)
    [ "$code" = "200" ] && exit 0
    sleep 2
  done
  echo "nats healthz timeout" >&2; exit 1
'
echo "healthz ok"

echo "[3/6] Rust 单元测试 (cargo test)"
$COMPOSE exec -T builder bash -c 'cd /workspace/rust && cargo test 2>&1' | tee "$OUT/cargo-test.log" | tail -5

echo "[4/6] 幂等播种 (seed_stream)"
$COMPOSE --profile seed run --rm seeder sh /test/sql/fixtures/seed.sh 2>&1 | tee "$OUT/seed.log"

echo "[5/6] 构建 (make debug)"
$COMPOSE exec -T builder make debug 2>&1 | tee "$OUT/build-debug.log" | tail -3

echo "[6/6] SQL 测试（单进程逐文件）"
for t in test/sql/*.test; do
  echo "--- $t"
  $COMPOSE exec -T builder bash -c "cd /workspace && ./build/debug/test/unittest '$t' 2>&1" | tee "$OUT/$(basename "$t").log" | tail -3
done

echo "完成，输出已归档: $OUT"
