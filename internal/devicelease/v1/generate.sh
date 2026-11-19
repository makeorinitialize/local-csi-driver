#!/bin/sh
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
set -eu
cd "$(dirname "$0")"
# libprotoc 3.12.4. Keep the plugins local and use only public dependencies.
tools="$PWD/.protoc-tools"
mkdir "$tools"
trap 'rm -rf "$tools"' EXIT
export GOBIN="$tools"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$tools:$PATH" protoc --go_out=paths=source_relative:. \
    --go-grpc_out=paths=source_relative:. device_provider.proto
