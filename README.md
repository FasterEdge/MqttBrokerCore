<div align="center">
  <img src="Logo.png" alt="MqttBrokerCore" width="120"/>
  <h2>MqttBrokerCore</h2>
  <h3>轻量级 MQTT 代理核心（基于 Hrotti）</h3>
</div>

### 一、项目简介
- 使用 Go 实现的 **MQTT 3.1.1 / 5.0 代理核心**（broker library），可作为库集成，也附带独立服务器程序。
- 支持 **TCP** 与 **WebSocket** 两种监听方式，可同时配置多个监听器。
- 内置 **QoS 0/1/2** 消息处理、**遗嘱消息**（Will）、**保留消息**（Retained）、**主题通配符**（`+`、`#`）。
- 提供默认的 **内存持久化**（`MemoryPersistence`），可通过 `Persistence` 接口扩展到 Redis / LevelDB 等外部存储。
- 内部 `BrokerStats` 记录连接数、吞吐、丢弃等指标，便于对接监控系统。

> 本仓库是对上游 [alsm/hrotti](https://github.com/alsm/hrotti) 的**增强与安全加固**分支，重点补齐了原项目在报文解析、并发与资源控制上的安全隐患。

### 二、快速开始

> **环境要求**：本项目在 `go.mod` 中声明 `toolchain go1.25.13`（已修复标准库 CVE）。Go 1.21+ 会自动下载并使用该工具链；如显式指定，可用 `GOTOOLCHAIN=go1.25.13 go build`。

```bash
go mod tidy
go build ./...

# 以独立服务器方式启动
go run . -key mySecret

# 或使用配置文件启动（多监听 / WebSocket）
go run . -conf config.json
```

### 三、作为库集成

以 `MqttBrokerCore` 作为依赖库，创建一个 MQTT 服务器：

```go
package main

import (
	"os"
	"os/signal"
	"syscall"

	hrotti "github.com/alsm/hrotti/broker"
)

func main() {
	h := hrotti.NewHrotti(100)
	hrotti.INFO = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)
	h.AddListener("test", hrotti.NewListenerConfig("tcp://0.0.0.0:1883"))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	h.Stop()
}
```

### 四、启动参数

| 参数      | 默认值 | 说明                                                       |
|-----------|--------|------------------------------------------------------------|
| `-key`    |（必填） | 访问密码，用于 HTTP / MQTT 管理接口鉴权                     |
| `-conf`   |（空）   | JSON 配置文件路径；若为空则使用环境变量 `HROTTI_URL`（单监听）|
| `-addr`   | `:1883` | 单监听模式下的监听地址（仅在未提供 `-conf` 时生效）           |
| `-log`    | `stdout`| 日志输出目标：`stdout` / `stderr` / `discard`               |

### 五、配置文件（JSON 示例）

```json
{
  "maxQueueDepth": 100,
  "listeners": {
    "tcp": { "url": "tcp://0.0.0.0:1883" },
    "ws":  { "url": "ws://0.0.0.0:2000/mqtt" }
  },
  "logging": {
    "info": "stdout",
    "protocol": "discard",
    "error": "stderr",
    "debug": "discard"
  }
}
```

- **`maxQueueDepth`**：每个客户端的待发送消息队列大小（默认 100）。
- **`listeners`**：键名任意，`url` 仅支持 `tcp://` 或 `ws://`（其它 scheme 会被拒绝）。
- **`logging`**：将 `info` / `protocol` / `error` / `debug` 分别定向到 `stdout`、`stderr` 或 `discard`。

### 六、常见使用场景

| 场景            | 示例                                             | 说明                                       |
|-----------------|--------------------------------------------------|--------------------------------------------|
| 单节点本地测试   | `go run . -key test`                             | 默认监听 `0.0.0.0:1883`，使用 `test` 鉴权。 |
| 多监听 + WebSocket | `go run . -conf config.json`                     | 同时开 TCP 与 WS 监听，共享同一主题树。     |
| 持久化会话       | `CleanSession=false` 且使用相同 `ClientIdentifier` | 断线重连后保留未确认的 QoS 1/2 消息与订阅。 |

### 七、内部实现概览

- **`Hrotti`**：broker 实例，持有客户端表、订阅树、持久化器与统计对象。
- **位图订阅存储**：`subscriptionMap.subBitmap` 实现 O(1) 主题匹配，支持 `+` / `#` 通配符。
- **消息 ID 池**：`messageIDs` 分配 1–65534，耗尽时返回 `ErrMsgIDsExhausted`，避免写入非法 `MessageID=0`。
- **持久化接口**：`Persistence`（`Init` / `Open` / `Add` / `GetAll` / `Delete` / `Close` / `Exists` / `Replace`）。

### 八、已修复的安全与可靠性问题

| 问题 | 修复 |
|------|------|
| `decodeLength` 无限读取字节导致协议失效 / OOM | 限制最多 4 字节并返回错误 |
| `ReadPacket` 未限制 `RemainingLength`，可分配大内存 | 新增 `MaxRemainingLength`（默认 64 MiB）上限 |
| 报文体短读未检测，导致协议同步失效 | 引入 `decodeReader` 捕获 truncation 错误 |
| `WillFlag=0` 时未检查 `WillQoS` / `WillRetain` | 在 `ConnectPacket.Validate` 中拒绝不一致包 |
| 慢/不再读取的客户端导致发送死锁 | 所有发送改为非阻塞 `select…default` 模式 |
| `NewListenerConfig` 在 URL 解析失败时返回 nil | 新增 `NewListenerConfigWithError` 并校验 scheme |
| `getClient` 返回 nil 导致空指针 panic | 在 `FindRetained` / `DeliverMessage` 中加空值保护 |
| 空 `ClientIdentifier` 且 `CleanSession=0` 未拒绝 | 按 MQTT 规范返回 `CONN_REF_ID_REJ` |

---

> 感谢上游项目 **[alsm/hrotti](https://github.com/alsm/hrotti)** 提供的原始实现与设计思路，本项目在其基础上进行兼容性增强与安全加固。