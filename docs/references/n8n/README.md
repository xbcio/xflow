# n8n 工作流自动化实现方案

## 目录

- [架构设计](./architecture.md) - n8n 整体架构和技术栈
- [核心组件](./core-components.md) - 详细的核心组件说明
- [工作流执行](./workflow-execution.md) - 工作流执行机制和生命周期
- [节点系统](./nodes-system.md) - 节点类型、开发和扩展
- [实现指南](./implementation-guide.md) - 基于 n8n 理念的实现建议

## 什么是 n8n

n8n 是一个开源的**工作流自动化工具**，允许用户通过可视化界面连接各种应用和服务，实现业务流程的自动化。

### 核心特点

- **开源免费** - 基于 Fair Code License，可自托管
- **可扩展** - 支持 400+ 集成节点
- **低代码/无代码** - 可视化工作流设计器
- **灵活执行** - 支持手动、定时、Webhook 等多种触发方式
- **强大的数据处理** - 内置 JavaScript 代码执行和表达式引擎
- **自托管优先** - 完全控制数据和执行环境

### GitHub 仓库

- 主仓库: [n8n-io/n8n](https://github.com/n8n-io/n8n)
- 官方文档: [docs.n8n.io](https://docs.n8n.io)
- 社区: [community.n8n.io](https://community.n8n.io)

## 技术栈

### 后端
- **Node.js** + **TypeScript** - 核心运行时
- **Express.js** - Web 服务框架
- **SQLite/PostgreSQL/MySQL** - 数据持久化
- **Redis** (可选) - 队列和缓存
- **Bull Queue** - 异步任务处理

### 前端
- **Vue.js 3** - UI 框架
- **Pinia** - 状态管理
- **N8N Design System** - 自定义组件库
- **JSPlumb** - 工作流可视化连接线

### 执行引擎
- **workflow-engine** - 核心执行引擎
- **n8n-nodes-base** - 内置节点包
- **n8n-core** - 核心功能库

## 快速开始

```bash
# 使用 Docker 运行
docker run -it --rm \
  --name n8n \
  -p 5678:5678 \
  -v ~/.n8n:/home/node/.n8n \
  n8nio/n8n

# 或使用 npm 安装
npm install n8n -g
n8n start

# 访问 http://localhost:5678
```

## 在 xflow 项目中的应用

本文档集旨在帮助理解 n8n 的核心理念和实现方式，为 xflow 项目提供参考：

1. **工作流定义模型** - 如何设计工作流的数据结构
2. **执行引擎架构** - 如何实现高效的工作流执行器
3. **节点系统设计** - 如何构建可扩展的节点生态
4. **API 接口设计** - RESTful API 最佳实践
5. **可视化编辑器** - 工作流设计器的实现思路

## 文档约定

- 代码示例使用 TypeScript
- 架构图使用 Mermaid 格式
- API 规范使用 OpenAPI 3.0

## 贡献

如有问题或建议，请联系 xflow 项目团队。

---

**最后更新**: 2026-01-04
