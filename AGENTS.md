# MagWorkspace 开发规则

面向个人文档与知识库的 Go 后端；Workspace、数据权限与会话由本项目持有，Agent 执行复用独立 Magent Harness。

- 开发前阅读 `docs/project-iteration/SKILL.md`、相关实现与当前任务；保留用户已有改动。
- 架构与模块边界：`docs/architecture.md`；编码要求：`docs/code-standards.md`；验证：`docs/testing.md`。
- 按迭代验收交付；权限、持久化、会话并发及外部 Agent 契约必须验证真实入口和失败路径。
- 用户身份取自认证状态，Workspace 角色从数据库实时读取，会话始终归用户私有。禁止信任请求中的身份或 Agent 自报权限。
- 数据库等基础设施使用 Docker Compose，不在宿主机安装服务。配置使用环境变量，禁止提交凭据。
- 并行任务按文件分工；共享接口由集成人维护。不要修改相邻的 Magent Harness 仓库。
- 真实命令以 `docs/testing.md` 为准；明确区分通过、失败、未运行与跳过。
- 未经授权不提交、推送、合并或部署。
