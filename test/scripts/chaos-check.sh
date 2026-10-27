#!/bin/bash
# 故障注入用例（CG-P0-3 / CG-HA-3，2026-10-27 覆盖面审查补测；在 minis 宿主机执行）
# C1（CG-P0-3）：COPY 中途杀 nats-server → COPY 报错、部分消息已发布（F4 非原子性演示）
# C2（CG-HA-3）：nats 停机时扫描报错；重启后恢复（scan.test 全套复验）
# 输出归档 tmp/test-output/<TS>-chaos/；时序敏感断言带一次重试（kill 竞态兜底）
set -uo pipefail  # 不用 -e：单用例失败继续跑完，汇总退出码
cd "$(dirname "$0")/../.."
exec </dev/null

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="tmp/test-output/$TS-chaos"
mkdir -p "$OUT"
COMPOSE="docker compose -f docker/docker-compose.yml"
UNITTEST="./build/reldebug/test/unittest"
FAIL=0

wait_healthz() {
  for i in $(seq 1 30); do
    code=$($COMPOSE exec -T builder curl -s -o /dev/null -w '%{http_code}' http://nats:8222/healthz 2>/dev/null || true)
    [ "$code" = "200" ] && return 0
    sleep 2
  done
  return 1
}
reseed() {
  $COMPOSE exec -T builder bash -c 'cd /workspace/rust && cargo run --bin seed -- nats://nats:4222' >/dev/null 2>&1
}

echo "== [C1] COPY 中途杀 server（range 100000，3s 后 docker kill；两次尝试兜竞态）=="
$COMPOSE up -d nats builder >/dev/null 2>&1
wait_healthz || { echo "C1: healthz 超时"; exit 1; }
c1_pass=0
for attempt in 1 2; do
  reseed   # 清 rt_chaos 等全部测试流，保证两次尝试独立
  $COMPOSE exec -d builder bash -c "cd /workspace && $UNITTEST test/sql/chaos/chaos-copy.test > /workspace/tmp/chaos-c1.log 2>&1"
  sleep 3
  docker kill nats-rw-nats >/dev/null 2>&1  # SIGKILL 立断（compose stop 为 10s 优雅排空，实测会放跑整个 COPY）
  sleep 8   # 等 unittest 结束（连接错误/超时传播完毕）
  $COMPOSE up -d nats >/dev/null 2>&1
  wait_healthz || { echo "C1: 重启后 healthz 超时"; FAIL=1; break; }
  $COMPOSE exec -T builder bash -c "cd /workspace && $UNITTEST test/sql/chaos/chaos-verify.test > /workspace/tmp/chaos-c1v.log 2>&1"
  copy_errored=$(grep -c 'All tests passed' tmp/chaos-c1.log 2>/dev/null || true)  # statement-error 用例通过 == COPY 如期报错
  partial_ok=$(grep -c 'All tests passed' tmp/chaos-c1v.log 2>/dev/null || true)
  echo "attempt$attempt: copy_errored=$copy_errored partial_ok=$partial_ok"
  if [ "${copy_errored:-0}" -ge 1 ] && [ "${partial_ok:-0}" -ge 1 ]; then c1_pass=1; break; fi
done
cp tmp/chaos-c1.log tmp/chaos-c1v.log "$OUT/" 2>/dev/null
[ "$c1_pass" = "1" ] || { echo "C1: FAIL（未同时满足：COPY报错 且 部分发布 1..99999）"; FAIL=1; }

echo "== [C2] 停机扫描报错 → 重启恢复 =="
$COMPOSE stop nats >/dev/null 2>&1
$COMPOSE exec -T builder bash -c "cd /workspace && $UNITTEST test/sql/chaos/chaos-down.test 2>&1" > "$OUT/c2-down.log"
down_ok=$(grep -c 'All tests passed' "$OUT/c2-down.log" || true)
$COMPOSE up -d nats >/dev/null 2>&1
wait_healthz || { echo "C2: 重启失败"; FAIL=1; }
$COMPOSE exec -T builder bash -c "cd /workspace && $UNITTEST test/sql/scan.test 2>&1" > "$OUT/c2-recovery.log"
recover_ok=$(grep -c 'All tests passed' "$OUT/c2-recovery.log" || true)
echo "C2: 停机报错用例=$down_ok（1=通过） 恢复复验=$recover_ok（1=通过）"
[ "${down_ok:-0}" -ge 1 ] && [ "${recover_ok:-0}" -ge 1 ] || FAIL=1

echo "== 汇总 =="; [ "$FAIL" = "0" ] && echo "chaos-check: PASS" || echo "chaos-check: FAIL"
exit $FAIL
