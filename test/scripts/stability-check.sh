#!/bin/bash
# 阶段二稳定性验证：reldebug 构建上循环跑 scan.test（在 minis 宿主机执行）
cd /root/duckdb-nats-rw
: > tmp/stability.log
for i in 1 2 3 4 5; do
  timeout 60 docker compose -f docker/docker-compose.yml exec -T builder \
    bash -c 'cd /workspace && ./build/reldebug/test/unittest test/sql/scan.test > /workspace/tmp/one.log 2>&1'
  tail -1 tmp/one.log >> tmp/stability.log
  grep -c DEADLYSIGNAL tmp/one.log >> tmp/stability.log || echo 0 >> tmp/stability.log
done
echo "===== results (pass-line / signal-count x5) ====="
cat tmp/stability.log
