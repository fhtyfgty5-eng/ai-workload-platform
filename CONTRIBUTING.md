# 贡献指南

感谢你关注 AI Workload Platform。提交修改前请先阅读 README、架构基线和对应模块的需求与设计文档。

## 开发环境

- Go 1.26.x
- Node.js 24.x 和 npm
- PostgreSQL 16（自动化测试默认不要求本机数据库）

前端依赖使用 `web/package-lock.json` 固定，安装请使用 `npm ci`。

## 提交前检查

```bash
gofmt -l .
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
bash scripts/verify-docs.sh
bash scripts/verify-k8s-manifests.sh
bash scripts/check-secrets.sh
cd web && npm ci && npm test -- --run && npm run typecheck && npm run build
```

代码标识符保持英文；面向项目读者的关键 Go 注释和学习资料使用中文。请为行为变化补充测试，避免把真实 Token、密码、公司代码、业务数据或本机绝对路径提交到仓库。

## 边界

控制面是业务事实源，前端不得直连数据库或复制调度逻辑。Draft 必须经过服务端校验和人工确认。容器执行器不是绝对安全沙箱，不要提交任意宿主机路径、Docker Socket 或未审查的镜像。
