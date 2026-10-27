#!/usr/bin/env bash
# 测试执行入口（Test Plan 第四章；minis Docker 模式）
# 用法：仓库根目录执行 ./test/scripts/run-all.sh            # 默认 reldebug
#       TESTBUILD=debug ./test/scripts/run-all.sh           # 按需跑 debug+ASan
# 每次运行的输出归档到 tmp/test-output/<UTC时间戳>/，禁止覆盖历史目录。
#
# 硬性防护（plans/nats-rw-exit-storm-disk-guard-fix-plan.md，2026-10-26）：
#   - 所有测试命令在容器内层强制 timeout + ulimit -f 双限界——debug+ASan 构建
#     曾在进程退出期发生 DEADLYSIGNAL 无限风暴，无界重定向写满 minis 磁盘（337G+68G）。
#   - 构建日志经 head -c 管道限界（200MB）；构建进程不加 ulimit -f（debug 的
#     .o 文件可超 50MB，SIGXFSZ 会误杀编译器——V1 实测教训）。
#   - 默认构建 reldebug（与主计划 8.1 磁盘政策对齐）；debug 按需且用后删 build/debug。
#
# SQL 测试以单进程直跑（官方 runner 多 worker 在 29GB 内存下会 OOM，见计划阶段一记录）。
set -euo pipefail
# 脚本位于 test/scripts/，仓库根需上溯两级（历史上错写为一层，整跑必败——
# 2026-10-26 首次端到端执行时暴露，此前两轮均分步执行未触发）
cd "$(dirname "$0")/../.."

# docker compose exec -T 会吞调用方 stdin——统一切断，保证脚本在非交互/管道下可执行
exec </dev/null

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="tmp/test-output/$TS"
mkdir -p "$OUT"

# 构建选择与防护组合（debug+ASan 双防线，fix-plan 2026-10-26）：
#   主防线 setarch -R（关 ASLR）：宿主 mmap_rnd_bits=32 × gcc-12 旧 libasan 影子映射
#     随机碰撞 → DEADLYSIGNAL 无限风暴，实测 sysctl=28/关 ASLR 0/10 风暴（P3 实验）；
#     需要 builder 容器 seccomp:unconfined（见 docker-compose.yml）。
#   次防线 ASAN_OPTIONS：detect_leaks=0（实测不能单独可靠预防，仅纵深保留）。
TESTBUILD="${TESTBUILD:-reldebug}"
case "$TESTBUILD" in
  reldebug) ASAN_ENV=""; NOASLR="" ;;
  debug)    ASAN_ENV="env ASAN_OPTIONS=detect_leaks=0:abort_on_error=1"; NOASLR="setarch x86_64 -R" ;;
  *) echo "TESTBUILD 必须是 reldebug 或 debug（当前：$TESTBUILD）" >&2; exit 2 ;;
esac

COMPOSE="docker compose -f docker/docker-compose.yml"

echo "[1/6] 起容器 (nats + builder)"
$COMPOSE up -d nats nats_userpass nats_token builder

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

echo "[3/6] Rust 单元测试 (cargo test, timeout 600s)"
$COMPOSE exec -T builder bash -c 'cd /workspace/rust && timeout 600 cargo test 2>&1' | tee "$OUT/cargo-test.log" | tail -5

echo "[4/6] 幂等播种 (rust seeder: cargo run --bin seed, timeout 600s)"
$COMPOSE exec -T builder bash -c 'cd /workspace/rust && timeout 600 cargo run --bin seed -- nats://nats:4222 2>&1' | tee "$OUT/seed.log" | tail -2

echo "[5/6] 构建 (make $TESTBUILD, timeout 90min, 日志限界 200MB${NOASLR:+, 关ASLR})"
$COMPOSE exec -T builder bash -c "cd /workspace && timeout 5400 $NOASLR make $TESTBUILD 2>&1 | head -c 209715200 > /workspace/tmp/runall-build.log; rc=\${PIPESTATUS[0]}; cat /workspace/tmp/runall-build.log; exit \$rc" | tee "$OUT/build-$TESTBUILD.log" | tail -3

echo "[6/6] SQL 测试（单进程逐文件；timeout 300s + 单日志限界 50MB${NOASLR:+ + 关ASLR}；skip 视为失败）"
for t in test/sql/*.test; do
  echo "--- $t"
  $COMPOSE exec -T builder bash -c "cd /workspace && ulimit -f 102400 && timeout 300 $NOASLR $ASAN_ENV ./build/$TESTBUILD/test/unittest '$t' > /workspace/tmp/runall-last-test.log 2>&1; rc=\$?; cat /workspace/tmp/runall-last-test.log; exit \$rc" | tee "$OUT/$(basename "$t").log" | tail -3
  # 诚实检查（TP-07d）：require 失败导致的整文件 skip 不是通过（2026-10-26 假阴性事故）
  if grep -q "tests were skipped" "$OUT/$(basename "$t").log"; then
    echo "!!! $t 被 SKIP（扩展加载失败？）——视为失败" >&2
    exit 1
  fi
done

echo "完成，输出已归档: $OUT"
# 证据摘要（honest-check 红旗修复 2026-10-27）：报告数字的唯一权威来源
bash test/scripts/evidence-summary.sh "$OUT" | tee "$OUT/summary-echo.log"
echo "提醒：若本次为 TESTBUILD=debug，按主计划 8.1 政策用后删除 build/debug。"
