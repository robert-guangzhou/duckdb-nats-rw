
## 基线 20261027T035553Z（minis builder 容器：nats:2.11 单节点同宿主，reldebug 构建）
- COPY 50000 行（串行 publish-ack，每行等 ack）：2.04s
- scan 50000 行全量物化 ×3：0.79s / 0.88s / 0.88s
- 口径：记录型基线（非门槛），供后续回归对比；构建环境见 docs/notes-build.md。
- 复现：test/scripts/perf-baseline.sh（模板 test/perf/*.test）
