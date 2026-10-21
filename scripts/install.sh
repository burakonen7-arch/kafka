#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/cloud-env.sh
if ! command -v dotnet >/dev/null || [[ "$(dotnet --version)" != 10.0.402 ]]; then
  archive=$(mktemp /tmp/dotnet-sdk.XXXXXX.tar.gz)
  checksum=$(mktemp /tmp/dotnet-sdk.XXXXXX.sha512)
  trap 'rm -f "$archive" "$checksum"' EXIT
  base=https://builds.dotnet.microsoft.com/dotnet/Sdk/10.0.402/dotnet-sdk-10.0.402-linux-x64.tar.gz
  curl --fail --show-error --silent --location "$base" -o "$archive"
  curl --fail --show-error --silent --location "$base.sha512" -o "$checksum"
  printf '%s  %s\n' "$(tr -d '\r\n' < "$checksum")" "$archive" | sha512sum --check --status
  mkdir -p "$DOTNET_ROOT"
  tar -xzf "$archive" -C "$DOTNET_ROOT"
fi
dotnet restore KafkaLearning.sln --locked-mode
dotnet build KafkaLearning.sln --no-restore -m:2
