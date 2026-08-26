# 模块 6 固定动作镜像：先由仓库脚本编译静态二进制，再复制进空白镜像。
# 使用 scratch 让本地实验不依赖 Docker Hub 的基础镜像；动作本身不需要系统命令或证书。
FROM scratch
COPY .workload/module6-action/workload-action /action
USER 65532:65532
ENTRYPOINT ["/action"]
