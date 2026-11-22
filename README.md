<div align="center">
  <img src="Logo.png" alt="MqttBrokerCore" width="120"/>
  <h2>MqttBrokerCore</h2>
  <h3>轻量级 MQTT 代理核心（基于 Hrotti）</h3>
</div>

### 一、项目简介
- 使用 Go 实现的 **MQTT 3.1.1 代理核心**（broker library），可作为库集成，也附带独立服务器程序。
- 支持 **TCP** 与 **WebSocket** 两种监听方式，可同时配置多个监听器。
- 内置 **QoS 0/1/2** 消息处理、**遗嘱消息**（Will）、**保留消息**（Retained）、**主题通配符**（`+`、`#`）。
- 提供默认的 **内存持久化**（`MemoryPersistence`），可通过 `Persistence` 接口扩展到 Redis / LevelDB 等外部存储。
- 内置 **访问控制**：CONNECT 用户名/密码校验 + 按身份的主题级 ACL（可限制各身份能发布/订阅的主题模式），默认 fail-closed。
- 内部 `BrokerStats` 记录连接数、吞吐、丢弃等指标，便于对接监控系统。

> 本仓库是对上游 [alsm/hrotti](https://github.com/alsm/hrotti) 的**增强与安全加固**分支，重点补齐了原项目在报文解析、并发、资源控制与**访问控制**上的安全隐患。

> **重要**：本 broker **暂不支持 TLS**。`tcp://` 与 `ws://` 两种监听器都是明文传输，用户名与密码以明文过网。生产部署必须放在可信网络内，或在前置反代/网关上终止 TLS（反向代理需自行处理证书与 `X-Forwarded-*`）。详见第九节「安全边界」。

### 二、快速开始

> **环境要求**：本项目在 `go.mod` 中声明 `toolchain go1.25.13`（已修复标准库 CVE）。Go 1.21+ 会自动下载并使用该工具链；如显式指定，可用 `GOTOOLCHAIN=go1.25.13 go build`。

```bash
go mod tidy
go build ./...

# 以独立服务器方式启动（默认监听 tcp://0.0.0.0:1883，可用 HROTTI_URL 覆盖）
# ⚠️ 不带 -conf 时没有 auth 段 → broker fail-closed，所有 CONNECT 被拒。
#    本地快速试用请显式关闭访问控制，例如：
#      go run . -conf config.json   # 其中 "auth": {"allowAnonymous": true}
go run .

# 或使用配置文件启动（多监听 / WebSocket / 访问控制）
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

	hrotti "github.com/FasterEdge/MqttBrokerCore/broker"
)

func main() {
	h := hrotti.NewHrotti(100, &hrotti.MemoryPersistence{})
	hrotti.INFO = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)

	// 访问控制: 不配置时 broker fail-closed(拒绝一切 CONNECT)。
	// 本地试用可显式关闭: h.SetAuthConfig(&hrotti.AuthConfig{AllowAnonymous: true})
	if err := h.SetAuthConfig(&hrotti.AuthConfig{AllowedCredentials: []hrotti.Credentials{{
		Username:  "sensor",
		Password:  "sensor-pw",
		PubTopics: []string{"site/+/reading"},
		SubTopics: []string{"site/+/reading"},
	}}}); err != nil {
		panic(err)
	}

	h.AddListener("test", hrotti.NewListenerConfig("tcp://0.0.0.0:1883"))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	h.Stop()
}
```

### 四、启动参数

| 参数       | 默认值 | 说明                                                      |
|------------|--------|-----------------------------------------------------------|
| `-conf`    | 空     | JSON 配置文件路径；提供后按配置启动多个 TCP / WebSocket 监听器，并从 `auth` 段加载凭据与 ACL |
| `HROTTI_URL` | `tcp://0.0.0.0:1883` | 未提供 `-conf` 时的单监听地址，格式为 `tcp://…` 或 `ws://…`；此路径下无 `auth` 配置，broker 为 fail-closed |

### 五、配置文件（JSON 示例）

```json
{
  "maxQueueDepth": 100,
  "listeners": {
    "tcp": { "url": "tcp://0.0.0.0:1883" },
    "ws":  { "url": "ws://0.0.0.0:2000/mqtt" }
  },
  "auth": {
    "allowed": [
      {
        "username": "sensor",
        "password": "sensor-pw",
        "pubTopics": ["site/+/reading"],
        "subTopics": ["site/+/reading"]
      },
      {
        "username": "ops",
        "password": "ops-pw",
        "pubTopics": ["site/#"],
        "subTopics": ["#"]
      }
    ]
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
- **`auth`**（可选，但**强烈建议配置**）：见下方「访问控制」一节。
- **`logging`**：将 `info` / `protocol` / `error` / `debug` 分别定向到 `stdout`、`stderr` 或 `discard`。

> **未配置 `auth` 时的行为**：broker 启动会打印 WARNING，并进入 **fail-closed** 状态——拒绝一切 CONNECT（`CONNACK 0x04`）。这是有意的："忘了配鉴权"的后果是所有人都连不上，而不是人人都能连。

### 六、访问控制

#### 6.1 身份校验（CONNECT）

`auth.allowed` 每项声明一个身份：用户名、密码、以及该身份允许 pub/sub 的主题过滤器列表。

- 密码用 `crypto/subtle.ConstantTimeCompare` **常量时间比较**，且用户名不存在时也走一次等长 dummy 比较，避免"账号枚举"时序侧信道。
- 用户名不存在、密码错误、未带凭据三种情况**统一返回 `CONNACK 0x04`**（`CONN_REF_BAD_USER_PASS`），客户端无法据此枚举合法用户名。
- 鉴权发生在任何会话状态（客户端表、持久化、订阅表）被触碰**之前**，未认证连接不能借"同 ClientID 重连"接管他人持久会话。
- 密码只与配置做比较，不写日志、不进持久化。

#### 6.2 主题级 ACL

每个身份声明 `pubTopics`（可发布的主题）与 `subTopics`（可订阅的主题过滤器）：

- **白名单语义**：列表为空 = 该身份不能发布/订阅任何主题（漏配的结果是拒绝，不是放行）。
- ACL 规则本身也是主题过滤器，因此 `site/+/reading` 的 ACL 覆盖 `site/1/reading`、`site/2/reading`，但不覆盖 `site/1/reading/extra`。
- **`#` 只对显式写了 `#` 的身份放行**：没有任何隐式通配提升。上例中 `sensor` 订阅 `#` 会被拒，`ops` 会被接受。

#### 6.3 越权时的行为

| 场景 | broker 行为 | MQTT 返回码 |
|------|-------------|-------------|
| 凭据错误 / 未带凭据 | 回 CONNACK 后立即关闭连接 | `0x04 CONN_REF_BAD_USER_PASS` |
| 订阅未授权主题 | 不注册订阅，回 SUBACK | `0x80`（SUBACK Failure） |
| 发布到未授权主题（QoS 0） | 静默丢弃，不下发、不入保留消息 | 无回包 |
| 发布到未授权主题（QoS 1/2） | 丢弃，但**仍回 PUBACK/PUBREC** | 正常 ack |

> 关于"不泄漏主题存在性"：未授权订阅的 `0x80` 与"写错了主题名"完全一致，越权发布仍照常 ack。客户端因此无法用返回码或 ack 时序判断某主题是否存在、有谁订阅。

#### 6.4 相关 API

```go
h := hrotti.NewHrotti(100, &hrotti.MemoryPersistence{})

// 配置访问控制(重复用户名/空用户名/匿名与凭据混用都会返回错误)
err := h.SetAuthConfig(&hrotti.AuthConfig{
    AllowedCredentials: []hrotti.Credentials{{
        Username:  "sensor",
        Password:  "sensor-pw",
        PubTopics: []string{"site/+/reading"},
        SubTopics: []string{"site/+/reading"},
    }},
})

// 显式关闭访问控制(必须写出来, 不能用"不配置"来隐式放开)
_ = h.SetAuthConfig(&hrotti.AuthConfig{AllowAnonymous: true})

hrotti.GetAuthStats() // 读取 ConnRejected/SubRejected/PubRejected/UnknownIdentity 计数
```

`SetAuthConfig(nil)` 会清空凭据表并回到 fail-closed（可用于配置热更新失败时的安全降级）。

### 七、常见使用场景

| 场景            | 示例                                             | 说明                                       |
|-----------------|--------------------------------------------------|--------------------------------------------|
| 单节点本地测试   | `go run . -conf config.json`（配 `allowAnonymous`）| 默认监听 `0.0.0.0:1883`（可通过 `HROTTI_URL` 覆盖）。无 `auth` 段时 fail-closed。 |
| 多监听 + WebSocket | `go run . -conf config.json`                     | 同时开 TCP 与 WS 监听，共享同一主题树。     |
| 端侧只读上报     | 身份 `pubTopics: ["site/+/reading"]`              | 端侧只能上报读数，不能订阅任何主题。        |
| 运维全量订阅     | 身份 `subTopics: ["#"]`                           | 需显式写 `#` 才被授予；未授权身份订阅 `#` 回 `0x80`。 |
| 持久化会话       | `CleanSession=false` 且使用相同 `ClientIdentifier` | 断线重连后保留未确认的 QoS 1/2 消息与订阅。 |

### 八、内部实现概览

- **`Hrotti`**：broker 实例，持有客户端表、订阅树、持久化器、访问控制器与统计对象。
- **位图订阅存储**：`subscriptionMap.subBitmap` 实现 O(1) 主题匹配，支持 `+` / `#` 通配符。
- **消息 ID 池**：`messageIDs` 分配 1–65534，耗尽时返回 `ErrMsgIDsExhausted`，避免写入非法 `MessageID=0`。
- **访问控制**：`AuthManager`（`broker/auth.go`）持有凭据表与 ACL，用 `RWMutex` 保护；ACL 裁决复用既有的 `match()` 主题匹配原语，`#` 命中必须来自显式规则。
- **持久化接口**：`Persistence`（`Init` / `Open` / `Add` / `GetAll` / `Delete` / `Close` / `Exists` / `Replace`）。

### 九、安全边界（支持 / 不支持）

**支持**

| 能力 | 说明 |
|------|------|
| CONNECT 用户名/密码校验 | 可配置凭据表，常量时间比较，拒绝时不区分用户名/密码错误 |
| 主题级 ACL | 按身份限制 pub/sub 的主题模式，`#` 需显式授权 |
| fail-closed 默认 | 未配置凭据表（且未显式 `allowAnonymous`）时拒绝一切连接 |
| 越权不泄漏主题存在性 | 未授权订阅 `0x80`，越权发布照常 ack 后丢弃 |
| 越权发布不写保留消息 | 保留消息不会被未授权客户端污染 |
| 拒绝计数 | `GetAuthStats()` 提供连接/订阅/发布拒绝次数 |

**暂不支持**

| 能力 | 现状与原因 |
|------|-----------|
| **TLS / MQTTS** | **暂不支持**。`tcp://` / `ws://` 均为明文，用户名密码明文过网。需要在前置反代/网关终止 TLS，或等待后续版本在 broker 内实现。 |
| 匿名访问 | 需显式配置 `allowAnonymous: true`；未配置时一律拒绝 |
| 动态主题过滤器（ACL 规则内的正则） | ACL 规则即 MQTT 主题过滤器（`+` / `#`），不支持正则或 `${username}` 类模板 |
| 用户/ACL 管理 API | 仅启动期通过配置文件或 `SetAuthConfig` 注入，无运行时增删用户的接口 |
| 令牌鉴权（JWT / OAuth2） | 仅用户名+密码静态凭据 |
| $SYS 主题的访问控制 | `$SYS/session_identifier` 等内部发布未纳入 ACL，见下条注意事项 |

> **已知注意点**：broker 向空 `ClientIdentifier` 的客户端投递 `$SYS/session_identifier` 的发布未经过 ACL 过滤（该发布由 `InitClient` 在会话建立后直接入队）。它只泄漏服务端生成的 UUID，不含其它客户端数据；若需严格化，应在订阅侧一并裁决。

### 十、已修复的安全与可靠性问题

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
| **`Username`/`Password` 只解析不使用，任意客户端可订阅 `#` 旁路全部端侧数据** | 新增 `AuthManager`：CONNECT 常量时间凭据校验 + 主题级 ACL，未授权订阅回 `0x80`、越权发布丢弃且照常 ack，默认 fail-closed |

---

> 感谢上游项目 **[alsm/hrotti](https://github.com/alsm/hrotti)** 提供的原始实现与设计思路，本项目在其基础上进行兼容性增强与安全加固。
