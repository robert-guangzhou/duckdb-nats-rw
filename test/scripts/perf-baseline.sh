#!/bin/bash
# 性能基线记录（CG-P1-1，2026-10-27；记录型指标，非门槛——对齐 Dev Plan 七）
# 在 minis 宿主机执行；结果追加写入 docs/perf-baseline.md（带 UTC 时间戳）
set -euo pipefail
cd "$(dirname "$0")/../.."
exec </dev/null

COMPOSE="docker compose -f docker/docker-compose.yml"
UNITTEST="./build/reldebug/test/unittest"
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="tmp/test-output/$TS-perf"
mkdir -p "$OUT"

$COMPOSE up -d nats builder >/dev/null 2>&1
for i in $(seq 1 30); do
  code=$($COMPOSE exec -T builder curl -s -o /dev/null -w '%{http_code}' http://nats:8222/healthz 2>/dev/null || true)
  [ "$code" = "200" ] && break
  sleep 2
done

# 计时助手：容器内 date 差值（秒，两位小数）
timed() {  # $1=测试文件  输出=秒
  $COMPOSE exec -T builder bash -c "cd /workspace && \
    s=\$(date +%s.%N) && $UNITTEST '$1' >/dev/null 2>&1 && rc=0 || rc=\$? ; \
    e=\$(date +%s.%N) ; echo \$(echo \"\$e \$s\" | awk '{printf \"%.2f\", \$1-\$2}') ; exit \$rc"
}

echo "== [1/3] COPY 50000 行（串行 publish-ack）=="
t_copy=$(timed test/perf/perf-copy.test) || { echo "COPY 失败"; exit 1; }
echo "copy: ${t_copy}s"

echo "== [2/3] scan 50000 行 ×3（暖路径）=="
t1=$(timed test/perf/perf-count.test)
t2=$(timed test/perf/perf-count.test)
t3=$(timed test/perf/perf-count.test)
echo "scan: $t1 / $t2 / $t3 s"

echo "== [3/3] 写入 docs/perf-baseline.md =="
{
  echo ""
  echo "## 基线 $TS（minis builder 容器：nats:2.11 单节点同宿主，reldebug 构建）"
  echo "- COPY 50000 行（串行 publish-ack，每行等 ack）：${t_copy}s"
  echo "- scan 50000 行全量物化 ×3：${t1}s / ${t2}s / ${t3}s"
  echo "- 口径：记录型基线（非门槛），供后续回归对比；构建环境见 docs/notes-build.md。"
  echo "- 复现：test/scripts/perf-baseline.sh（模板 test/perf/*.test）"
} >> docs/perf-baseline.md

# 回执归档
$COMPOSE exec -T builder bash -c "cd /workspace && $UNITTEST test/perf/perf-count.test 2>&1" | tail -2 > "$OUT/verify.log"
echo "完成：docs/perf-baseline.md 已追加 $TS 基线；验证输出 $OUT/verify.log"
