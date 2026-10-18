# modelgate

自托管 LLM 代理网关：管理多家服务商的真实 Key，对外发放虚拟 Key，按转发策略路由调用，并提供完整的请求日志、SSE 实时观测与费用统计。

内置 [modellink](https://github.com/goroutined/modellink) 服务商与模型价格数据（通过 [modellink-go](https://github.com/goroutined/modellink-go) 加载），开箱即用，无需手动维护服务商目录。

## 特性

- **密钥池管理**：39+ 服务商预设（含协议端点、模型与价格），支持自定义服务商；一个服务商可挂多个真实 Key
- **虚拟 Key**：对外只暴露本地 baseURL + 虚拟 Key，真实 Key 不出内网
- **转发策略**：v1 支持固定映射（虚拟 Key → 指定真实 Key），轮询/随机/比例规划中
- **多协议**：openai-compatible / anthropic-compatible / openai-responses
- **完整日志**：请求/响应原始内容、SSE 每包到达时间，viewer 式时间轴回放
- **实时观测**：进行中的对话逐包实时展示（WebSocket）
- **费用统计**：按虚拟 Key / 模型 / 天粒度统计 tokens 与费用（CNY/积分），无价格数据标记为未计价而非猜测
- **自动清理**：日志水位线 + 保留天数，统计独立存储

## 安装与启动

需要 Go 1.26+：

```bash
go install github.com/goroutined/modelgate/cmd/modelgate@latest
modelgate
```

或从源码运行：

```bash
git clone https://github.com/goroutined/modelgate.git
cd modelgate && go run ./cmd/modelgate
```

启动后打开管理台：<http://127.0.0.1:8080>

## 快速开始

1. **服务商与 Key**：给服务商添加真实 Key（有 Key 即视为启用），或创建自定义服务商
2. **转发策略**：创建 fixed 策略，映射到某个真实 Key
3. **虚拟 Key**：创建虚拟 Key 并绑定策略，客户端 baseURL 填 `http://127.0.0.1:8080/v1`，Key 填虚拟 Key

## 配置

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `-listen` | `MODELGATE_LISTEN` | `127.0.0.1:8080` | 监听地址 |
| `-db` | `MODELGATE_DB` | `~/.modelgate/modelgate.db` | SQLite 数据库路径 |
| `-admin-token` | `MODELGATE_ADMIN_TOKEN` | 空 | 管理台鉴权令牌；为空不鉴权（仅限本机使用） |

业务配置（服务商、Key、策略等）存于 SQLite，管理台修改即时生效，无需重启。

## 安全提示

- 默认仅监听本机；对外暴露前请务必设置 `-admin-token`
- 真实 Key 只存于本地 SQLite，不会发往 modellink 或任何第三方
- 日志会记录完整请求/响应内容，注意设置合理的清理策略

## 生态

- [modellink](https://github.com/goroutined/modellink)：多服务商模型与价格数据（在线浏览：[goroutined.github.io/modellink](https://goroutined.github.io/modellink/)）
- [modellink-go](https://github.com/goroutined/modellink-go)：Go 客户端与费用估算库
- modelgate：调用层（本项目）

## License

[MIT](LICENSE)
