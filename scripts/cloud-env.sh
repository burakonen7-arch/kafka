#!/usr/bin/env bash
# Source this file before using the cloud SDK.
export DOTNET_ROOT=/workspace/.dotnet
export PATH="$DOTNET_ROOT:$PATH"
export DOTNET_CLI_HOME=/workspace/.dotnet-home
export NUGET_PACKAGES=/workspace/.nuget/packages
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export BUILDX_CONFIG=/workspace/.buildx
