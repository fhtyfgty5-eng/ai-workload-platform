#!/usr/bin/env bash
set -euo pipefail

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required; install it before running the Kubernetes experiment" >&2
  exit 2
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required; install it before running the Kubernetes experiment" >&2
  exit 2
fi
docker version >/dev/null

cluster_name="${WORKLOAD_KIND_CLUSTER:-workload-local}"
job_name="${WORKLOAD_MODULE6_JOB_NAME:-module6-action-demo}"
if [[ ! "$job_name" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]]; then
  echo "WORKLOAD_MODULE6_JOB_NAME must be a lowercase Kubernetes name" >&2
  exit 2
fi
if ! kind get clusters | rg -qx "$cluster_name"; then
  kind create cluster --name "$cluster_name"
fi
mkdir -p .workload/module6-action
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o .workload/module6-action/workload-action ./cmd/workload-action
docker build --pull=false -f deploy/module6-action/Dockerfile -t workload-action:local .
kind load docker-image workload-action:local --name "$cluster_name"
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/network-policy.yaml

# 使用固定清单运行一次 Job；实验脚本只删除自己创建且带模块 6 名称的 Job。
kubectl delete job "$job_name" -n workload-tasks --ignore-not-found
sed "s/name: module6-action-demo/name: ${job_name}/" deploy/k8s/action-job.yaml | kubectl apply -f -
kubectl wait --for=condition=complete "job/${job_name}" -n workload-tasks --timeout=90s
kubectl logs "job/${job_name}" -n workload-tasks
kubectl delete job "$job_name" -n workload-tasks --ignore-not-found
