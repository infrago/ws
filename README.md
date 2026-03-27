# ws

infrago 的 WebSocket 模块。

## 能力

- `http/web.Context.Upgrade()` 直接接入 `ws`
- `space` 隔离
- `ws.Hook`：`Open/Receive/Send/Close`
- `ws.Filter`：入站消息执行链
- `ws.Message`：客户端上行消息
- `ws.Command`：服务端下行命令
- `ctx.Reply/Push/Broadcast/Groupcast`
- `ctx.BindUser`、`PushUser`
- `PushResult/BroadcastResult/GroupcastResult`
- `ctx.Answer`
- `ws.Export()` / `ws.Metrics()`
- demo 首页会直接展示 `/ws/export` 和 `/ws/metrics`

## 接入模型

`http/web` 只负责：

- 路由
- 权鉴 / 参数
- `Upgrade`
- 把升级后的连接交给 `ws`

大多数项目直接这样用：

```go
infra.Register(".socket", web.Router{
    Uri: "/socket",
    Action: func(ctx *web.Context) {
        if err := ctx.Upgrade(); err != nil {
            ctx.Error(infra.Fail.With(err.Error()))
        }
    },
})
```

默认规则：

- `ctx.Upgrade()`：默认 `space = ctx.Name`
- 如果 `ctx.Name == ""`，则回退 `infra.DEFAULT`
- `ctx.Upgrade("name")`：显式使用指定 `space`

自定义空间示例：

```go
infra.Register(".socket.custom", web.Router{
    Uri: "/socket/custom",
    Action: func(ctx *web.Context) {
        _ = ctx.Upgrade("custom")
    },
})
```

## 配置

```toml
[ws]
format = "text"
codec = "json"
ping_interval = "30s"
read_timeout = "75s"
write_timeout = "10s"
max_message_size = 4194304
queue_size = 128
queue_policy = "close"
compression = false
compress_level = 0
observe_interval = "30s"
observe_log = false
observe_trace = false
```

默认输出结构：

```json
{"code":0,"name":"demo.notice","data":{"text":"hello"},"time":1770000000}
```

注意：

- `space` 是服务端内部隔离维度，不在 websocket 包体里传输
- `ws.Export()` 中每条协议会同时给出 `request` / `response` 示例
- `ws.Export()` 顶层带 `schema.name / schema.version / schema.generated`
- `ws.Export()` 顶层 `spaces` 会汇总每个空间的消息、命令、过滤器、处理器和钩子数量

接收端兼容：

- `msg` / `name`
- `args` / `data`
- 若没有 `args/data`，则把除消息名外的其它字段全部合并为参数

发送端固定结构：

- `name`
- `data`
- `code`
- `text`
- `time`

## Space

`ws` 的连接、用户、分组、广播都按 `space` 隔离。

默认情况下：

- `ctx.Upgrade()` 使用当前路由名 `ctx.Name` 作为 `space`
- 同一路由下的连接天然互通
- 不同路由的连接天然隔离

下列能力都按 `space` 生效：

- `Message / Command`
- `Hook / Filter / Handler`
- `BindUser / PushUser`
- `Join / Groupcast`
- `Broadcast`
- 节点间 `_ws.dispatch`

其中注册规则是：

- `Message / Command / Handler`：先查当前 `space`，再回退全局 `infra.DEFAULT`
- `Filter / Hook`：执行全局 `infra.DEFAULT` + 当前 `space`

如果不在 `ctx` 中、但又要显式指定空间，可以使用：

- `ws.PushUserIn(space, uid, msg, data)`
- `ws.BroadcastIn(space, msg, data)`
- `ws.GroupcastIn(space, gid, msg, data)`

## 队列优先级

`ws.Command.Setting` 支持：

- `priority = "high" | "normal" | "low"`
- `queue_policy = "block" | "close" | "drop"`

默认规则：

- `high`：优先保留，默认按 `block`
- `normal`：按模块级 `queue_policy`
- `low`：高压时自动丢弃，默认按 `drop`

## 示例

```go
infra.Register("demo.echo", ws.Message{
    Action: func(ctx *ws.Context) {
        _ = ctx.Reply("demo.echoed", Map{"text": ctx.Value["text"]})
    },
})
```
