#!/usr/bin/env bash
# 生成 nats-tls/nats-mtls 测试证书（core-publish 计划 A5；产物在 tmp/tls-certs，不入库）
# SAN 覆盖容器名（S2 高发点）：nats_tls / nats_mtls
set -euo pipefail
cd "$(dirname "$0")/../../tmp/tls-certs"
openssl genrsa -out ca-key.pem 2048 2>/dev/null
openssl req -x509 -new -nodes -key ca-key.pem -subj "/CN=nats-rw-test-ca" -days 30 -out ca.pem
openssl genrsa -out server-key.pem 2048 2>/dev/null
openssl req -new -key server-key.pem -subj "/CN=nats_tls" -out server.csr
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server.pem -days 30 -extfile <(printf "subjectAltName=DNS:nats_tls,DNS:nats_mtls,DNS:localhost") 2>/dev/null
openssl genrsa -out client-key.pem 2048 2>/dev/null
openssl req -new -key client-key.pem -subj "/CN=nats-rw-test-client" -out client.csr
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out client.pem -days 30 2>/dev/null
rm -f server.csr client.csr
echo "certs ready: $(ls *.pem)"
