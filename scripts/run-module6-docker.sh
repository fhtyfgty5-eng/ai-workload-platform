#!/usr/bin/env bash
set -euo pipefail

# 先编译仓库内固定动作二进制；不接受命令行传入的任意源码、镜像或构建上下文。
mkdir -p .workload/module6-action
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o .workload/module6-action/workload-action ./cmd/workload-action

# 构建仓库内固定动作镜像；不访问 Docker Hub，也不接受任意镜像。
docker version >/dev/null
docker build --pull=false -f deploy/module6-action/Dockerfile -t workload-action:local .

# 直接运行一个确定性动作，验证非 root、只读根文件系统、无网络和资源上限可以被 Docker 接受。
docker run --rm \
  --network none \
  --read-only \
  --user 65532:65532 \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 32 \
  --memory 64m \
  --cpus 0.25 \
  --tmpfs /tmp:rw,noexec,nosuid,size=8m \
  workload-action:local \
  document.normalize '{"source":"  module   six  "}'

# 输出本地镜像 ID，便于验证报告记录实验使用的确切构建结果。
docker image inspect workload-action:local --format 'image={{.Id}}'
