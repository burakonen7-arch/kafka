#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/cloud-env.sh
mkdir -p artifacts
docker compose up -d --wait postgres kafka
docker compose up -d kafka-init
docker compose wait kafka-init
export ASPNETCORE_ENVIRONMENT=Development Database__Initialize=true Kafka__BootstrapServers=localhost:9092
export Logging__LogLevel__Microsoft_EntityFrameworkCore=Warning
for service in Product Order; do
  if [[ "$service" == Product ]]; then port=5001; database=products; else port=5002; database=orders; fi
  if curl -fsS "http://127.0.0.1:$port/swagger/v1/swagger.json" >/dev/null 2>&1; then
    echo "$service endpoint already responds on $port; leaving it running"
    continue
  fi
  ConnectionStrings__Database="Host=localhost;Database=$database;Username=app;Password=local-development-only" \
  ASPNETCORE_URLS="http://127.0.0.1:$port" \
  nohup dotnet "src/$service/$service.Api/bin/Debug/net10.0/$service.Api.dll" >"artifacts/$service.log" 2>&1 &
  echo $! >"artifacts/$service.pid"
  ready=false
  for attempt in {1..60}; do
    if curl -fsS "http://127.0.0.1:$port/swagger/v1/swagger.json" >/dev/null 2>&1; then ready=true; break; fi
    if ! kill -0 "$(cat "artifacts/$service.pid")" 2>/dev/null; then break; fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then tail -60 "artifacts/$service.log"; exit 1; fi
  echo "$service API ready on port $port"
done
