#!/bin/bash
# 证据摘要生成器（honest-check 红旗修复，2026-10-27）
# 从最近一次 run-all 归档目录的日志中提取权威数字，写入该归档的 summary.md 并输出。
# 规则：任何报告中的通过/断言计数必须引用本产物，禁止手工求和（"130+ 断言"夸大事故的根治）。
# 用法（minis 宿主机）：bash test/scripts/evidence-summary.sh [归档目录]
#   缺省取 tmp/test-output/ 下最新的非 chaos/perf 目录。
set -euo pipefail
cd "$(dirname "$0")/../.."
exec </dev/null

BASE="tmp/test-output"
if [ $# -ge 1 ]; then
  DIR="$1"
else
  DIR=$(ls -t "$BASE" 2>/dev/null | grep -v -- '-chaos$' | grep -v -- '-perf$' | head -1)
fi
[ -d "$BASE/$DIR" ] || { echo "evidence-summary: 归档不存在 $BASE/$DIR" >&2; exit 1; }

SUMMARY="$BASE/$DIR/summary.md"
{
  echo "# 测试证据摘要（机器生成，勿手改）"
  echo ""
  echo "- 归档目录：$DIR（UTC 时间戳命名；生成于 $(date -u +%Y-%m-%dT%H:%M:%SZ)）"
  echo "- 口径：SQL 断言计数逐字取自各归档日志尾行；总计=各行之和，仅此处允许求和。"
  echo ""
  echo "| 套件 | 结果（逐字） | 来源 |"
  echo "|------|-------------|------|"
  total=0
  for f in "$BASE/$DIR"/*.test.log; do
    [ -e "$f" ] || continue
    line=$(grep -E 'All tests passed|^assertions:' "$f" | tail -1 | tr -d '\000')
    n=$(echo "$line" | grep -oE '[0-9]+ assertions' | grep -oE '[0-9]+' || echo 0)
    if echo "$line" | grep -q 'All tests passed'; then
      total=$((total + n))
    else
      total="${total}+FAIL"
    fi
    echo "| $(basename "$f" .log) | ${line:-<无结果行>} | $(basename "$f") |"
  done
  echo ""
  echo "**SQL 断言总计（仅全过者计入）：${total%%+FAIL*}**$(echo "$total" | grep -q FAIL && echo '（存在失败套件！）' || echo '')"
  echo ""
  echo "- Rust 单测：见同归档 cargo-test.log 的 'test result:' 行（逐字引用）"
  grep -m1 'test result:' "$BASE/$DIR/cargo-test.log" 2>/dev/null | sed 's/^/  - /' || echo "  - <无 cargo-test.log>"
  echo ""
  echo "- 过期警告：证据采集于 $DIR，重新运行结果可能不同。"
  echo "- chaos/perf 证据在 *-chaos/*-perf 归档目录，另行引用其原日志。"
} > "$SUMMARY"

cat "$SUMMARY"
