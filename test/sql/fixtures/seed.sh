#!/bin/sh
# 幂等播种：重建 seed_stream 并发布确定性的 10 条消息（seq 1..10）。
# 数据形状与 test/sql/scan.test 的期望严格对应（TP-02 组，见测试计划）。
set -e
S="${NATS_URL:-nats://nats:4222}"

nats --server "$S" stream rm -f seed_stream >/dev/null 2>&1 || true
nats --server "$S" stream add seed_stream --subjects 'seed.>' --storage file --retention limits

i=1
while [ "$i" -le 5 ]; do
  nats --server "$S" pub "seed.a.$i" "payload-a-$i" -H "unit: alpha"
  i=$((i + 1))
done
i=1
while [ "$i" -le 5 ]; do
  nats --server "$S" pub "seed.b.$i" "payload-b-$i" -H "unit: beta"
  i=$((i + 1))
done

nats --server "$S" stream info seed_stream | grep -iE "messages|last seq"
