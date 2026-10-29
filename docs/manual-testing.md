# nats_rw 手工测试指南

> 环境：minis 上的 Docker 容器（nats + builder），扩展已构建。
> 入口：ssh 到 minis → 进入 builder 容器 → 使用 DuckDB CLI。

## 启动环境

```bash
# 1. SSH 到 minis（局域网或 Tailscale）
ssh root@192.168.3.13
# ssh root@100.94.57.91   # Tailscale 备用

# 2. 启动全部容器（如已在运行则跳过）
cd /root/duckdb-nats-rw
docker compose -f docker/docker-compose.yml up -d nats nats_userpass nats_token builder

# 3. 进入 builder 容器
docker compose -f docker/docker-compose.yml exec builder bash

# 4. 播种测试数据（每次手工测试前建议重跑，确保状态干净）
cd /workspace/rust && cargo run --bin seed -- nats://nats:4222
# 输出 "seeded 'seed_stream' with 10 messages" 即成功
```

## 进入 DuckDB CLI 并加载扩展

```bash
cd /workspace && ./build/reldebug/test/unittest --interactive
```

> 如果 `--interactive` 不支持，改用：
> ```bash
> ./build/reldebug/duckdb --unsigned-extensions
> ```
> 然后手动加载扩展：
> ```sql
> LOAD '/workspace/build/reldebug/extension/nats_rw/nats_rw.duckdb_extension';
> ```

以下所有 SQL 假设已 `LOAD nats_rw` 成功。

---

## 核心用例

### 一、版本与加载

```sql
LOAD nats_rw;
SELECT nats_rw_version();
-- 预期：返回 "0.1.0"
```

### 二、读取（nats_rw_scan）

```sql
-- 1. 全量快照（10 条种子消息）
SELECT count(*), min(seq), max(seq)
FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222');
-- 预期：10 | 1 | 10

-- 2. 查看完整输出（6 列：stream/subject/seq/ts_nats/headers/payload）
SELECT * FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222') LIMIT 3;

-- 3. subject 通配符过滤（只取 seed.a.* 的 5 条）
SELECT CAST(payload AS VARCHAR)
FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222', subject := 'seed.a.>')
ORDER BY seq;
-- 预期：payload-a-1 到 payload-a-5（BLOB 显示为 \x 前缀）

-- 4. seq 范围（闭区间 [3,5]）
SELECT count(*), min(seq), max(seq)
FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222', start_seq := 3, end_seq := 5);
-- 预期：3 | 3 | 5

-- 5. 谓词下推（WHERE seq 自动翻译为 scan 参数）
SELECT count(*), min(seq), max(seq)
FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222')
WHERE seq BETWEEN 4 AND 7;
-- 预期：4 | 4 | 7（与手动传参 start_seq:=4, end_seq:=7 结果一致）

-- 6. json_extract（从 JSON payload 抽取字段）
SELECT device_id, location_zone
FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222',
    json_extract := ['device_id', 'location.zone'])
WHERE device_id IS NOT NULL;
-- 预期：空（种子 payload 不是 JSON）
-- 如需测 JSON，先执行下方 COPY 写入再扫
```

### 三、写入（COPY TO）

```sql
-- 1. 基本写入（整行 JSON，自动建流）
COPY (SELECT 'manual-test' AS v, 42 AS n)
TO 'manual_rt' (FORMAT nats_rw, url 'nats://nats:4222', create_stream 'true');
-- 预期：返回 Count=1

-- 2. 读回验证
SELECT count(*) FROM nats_rw_scan('manual_rt', url := 'nats://nats:4222');
-- 预期：1

-- 3. payload_column 模式（裸字节）
COPY (SELECT '中文测试内容' AS msg)
TO 'manual_cn' (FORMAT nats_rw, url 'nats://nats:4222',
    payload_column 'msg', create_stream 'true');
-- 预期：成功

SELECT decode(payload) FROM nats_rw_scan('manual_cn', url := 'nats://nats:4222');
-- 预期：中文测试内容

-- 4. json_extract round-trip
COPY (SELECT '{"device_id":"sensor-99","location":{"zone":"south"}}' AS data)
TO 'manual_json' (FORMAT nats_rw, url 'nats://nats:4222',
    payload_column 'data', create_stream 'true');

SELECT device_id, location_zone
FROM nats_rw_scan('manual_json', url := 'nats://nats:4222',
    json_extract := ['device_id', 'location.zone']);
-- 预期：sensor-99 | south

-- 5. 批量写入
COPY (SELECT i, 'batch-' || CAST(i AS VARCHAR) FROM range(100) t(i))
TO 'manual_batch' (FORMAT nats_rw, url 'nats://nats:4222', create_stream 'true');

SELECT count(*) FROM nats_rw_scan('manual_batch', url := 'nats://nats:4222');
-- 预期：100
```

### 四、认证（Secrets）

```sql
-- 1. 无认证访问认证服务器 → 被拒绝
SELECT count(*) FROM nats_rw_scan('any', url := 'nats://nats_userpass:4222');
-- 预期：报错（authorization violation）

-- 2. 创建正确的 secret
CREATE SECRET my_auth (
    TYPE nats_rw,
    URL 'nats://nats_userpass:4222',
    USER 'admin',
    PASSWORD 's3cret-pass',
    SCOPE 'nats://nats_userpass'
);

-- 3. 用 secret 写入 + 读取
COPY (SELECT 'auth-ok' AS v)
TO 'auth_test' (FORMAT nats_rw, url 'nats://nats_userpass:4222',
    secret 'my_auth', payload_column 'v', create_stream 'true');

SELECT count(*) FROM nats_rw_scan('auth_test', secret := 'my_auth');
-- 预期：1

-- 4. secret 提供 URL（不传 url 参数）
SELECT count(*) FROM nats_rw_scan('auth_test', secret := 'my_auth');
-- 预期：1

-- 5. scope 自动匹配（不传 secret 参数）
SELECT count(*) FROM nats_rw_scan('auth_test', url := 'nats://nats_userpass:4222');
-- 预期：1

-- 6. 错误密码 → 拒绝
CREATE SECRET bad_auth (
    TYPE nats_rw,
    URL 'nats://nats_userpass:4222',
    USER 'admin',
    PASSWORD 'wrong',
    SCOPE 'nats://bad'
);
SELECT count(*) FROM nats_rw_scan('auth_test', url := 'nats://nats_userpass:4222', secret := 'bad_auth');
-- 预期：报错（authorization violation）

-- 7. 验证脱敏（密码不得出现在 duckdb_secrets 输出中）
SELECT count(*) FROM duckdb_secrets()
WHERE name = 'my_auth' AND secret_string LIKE '%s3cret-pass%';
-- 预期：0（密码被脱敏为 ***）

-- 8. Token 认证
CREATE SECRET tok_auth (
    TYPE nats_rw,
    URL 'nats://nats_token:4222',
    TOKEN 'tok-12345'
);
COPY (SELECT 'token-ok' AS v)
TO 'tok_test' (FORMAT nats_rw, url 'nats://nats_token:4222',
    secret 'tok_auth', payload_column 'v', create_stream 'true');
SELECT count(*) FROM nats_rw_scan('tok_test', secret := 'tok_auth');
-- 预期：1
```

### 五、元数据

```sql
-- 1. 列出全部流
SELECT name, messages, last_seq
FROM nats_rw_streams('', url := 'nats://nats:4222')
ORDER BY name;
-- 预期：包含 seed_stream（10 条）、manual_rt、manual_cn、manual_json、manual_batch（100）等

-- 2. 单流详情
SELECT name, subjects, messages, first_seq, last_seq, consumer_count
FROM nats_rw_stream_info('seed_stream', url := 'nats://nats:4222');
-- 预期：seed_stream | [seed.>] | 10 | 1 | 10 | 0
```

### 六、错误路径

```sql
-- 1. 不存在的流
SELECT count(*) FROM nats_rw_scan('no_such_stream', url := 'nats://nats:4222');
-- 预期：报错，错误消息含 "no_such_stream"

-- 2. 服务器不可达（5 秒超时）
SELECT count(*) FROM nats_rw_scan('seed_stream', url := 'nats://10.255.255.1:4222');
-- 预期：约 5 秒后报连接超时

-- 3. 错误端口（立即拒绝）
SELECT count(*) FROM nats_rw_scan('seed_stream', url := 'nats://nats:4223');
-- 预期：立即报 connection refused

-- 4. 超大 payload（服务端 1MB 限制）
COPY (SELECT repeat('x', 1100000) AS v)
TO 'big_test' (FORMAT nats_rw, url 'nats://nats:4222',
    payload_column 'v', create_stream 'true');
-- 预期：报错 "max payload size exceeded"

-- 5. 非法流名
COPY (SELECT 1 AS v) TO 'bad stream!' (FORMAT nats_rw,
    url 'nats://nats:4222', create_stream 'true');
-- 预期：报错 "stream name cannot contain"

-- 6. 参数越界
SELECT count(*) FROM nats_rw_scan('seed_stream', url := 'nats://nats:4222', batch_size := 0);
-- 预期：Binder Error "batch_size must be within [1, 256]"
```

---

## 清理（每次手工测试结束后可选）

```sql
-- 删除手工测试流（重新播种也会清理）
-- 退出 DuckDB 后在容器内执行：
-- cd /workspace/rust && cargo run --bin seed -- nats://nats:4222
-- 这会删除全部测试流并重建种子流
```

## 常见问题

| 问题 | 解决 |
|------|------|
| `LOAD` 失败 | 确认在 builder 容器内且路径正确 |
| 连接超时 | 检查 `docker compose ps` 确认 nats 容器 Running |
| 中文乱码 | 使用 `decode(payload)` 而非 `CAST(payload AS VARCHAR)` |
| json_extract 全 NULL | payload 必须是有效 JSON 字符串 |
| 密码在错误消息中泄漏 | 已修复（URL 脱敏），如发现请记录 |
