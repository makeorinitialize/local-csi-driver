#!/bin/sh
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
set -eu
cd "$(dirname "$0")"
test "$(protoc --version)" = "libprotoc 3.12.4"
test "$(protoc-gen-go --version)" = "protoc-gen-go v1.36.12"
test "$(protoc-gen-go-grpc --version)" = "protoc-gen-go-grpc 1.5.1"
exec protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. device_use.proto
