# ws

infrago 的 WebSocket 模块。

## 能力

- `http/web.Context.Upgrade()` 默认接入已注册的默认接入点
- `http.Endpoint / web.Endpoint` 可插拔接入
- `ws.Hook`：`Open/Receive/Send/Close`
- `ws.Filter`：入站消息执行链
- `ws.Message`：客户端上行消息
- `ws.Command`：服务端下行命令
- `ctx.Reply/Push/Broadcast/Groupcast`
- `ctx.BindUser`、`PushUser`
- `PushResult/BroadcastResult/GroupcastResult`
- `ctx.Answer`
- `ws.Export()` / `ws.Metrics()`

## 接入模型

`http/web` 只负责：

- 路由
- 权鉴 / 参数
- `Upgrade`
- 把升级后的连接交给已注册的 `Endpoint` 或默认 Upgrade 接管器

`ws` 模块自己注册了默认 Upgrade 接管器，所以大多数项目直接这样用：

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

- `ctx.Upgrade()`：先找未命名默认 `Endpoint`，没有则走默认 Upgrade 接管器
- `ctx.Upgrade("name")`：显式使用指定接入点

自定义接入点示例：

```go
infra.Register("custom", web.Endpoint{
    Name: "custom",
    Desc: "自定义 ws 接入",
    Accept: func(ctx *web.Context, socket web.Socket) error {
        return ws.Accept(ws.AcceptOptions{
            Conn:       socket,
            Meta:       ctx.Meta,
            Name:       ctx.Name,
            Site:       ctx.Site,
            Host:       ctx.Host,
            Domain:     ctx.Domain,
            RootDomain: ctx.RootDomain,
            Path:       ctx.Path,
            Uri:        ctx.Uri,
            Setting:    ctx.Setting,
            Params:     ctx.Params,
            Query:      ctx.Query,
            Form:       ctx.Form,
            Value:      ctx.Value,
            Args:       ctx.Args,
            Locals:     ctx.Locals,
        })
    },
})

infra.Register(".custom.socket", web.Router{
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
message_key = "name"
payload_key = "data"
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

接收端兼容：

- `msg` / `name`
- `args` / `data`
- 若没有 `args/data`，则把除消息名外的其它字段全部合并为参数

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
