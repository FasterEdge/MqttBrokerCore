<div align="center">
  <img src="Logo.png" alt="MqttBrokerCore" width="120"/>
  <h2>MqttBrokerCore</h2>
  <h3>轻量级 MQTT 代理核心（基于 Hrotti）</h3>
</div>

### 一、项目简介
- 使用 Go 实现的 **MQTT 3.1.1 代理核心**（broker library），可作为库集成，也附带独立服务器程序。
- 支持 **TCP** 与 **WebSocket** 两种监听方式，可同时配置多个监听器。`tcp://` / `ws://` 之外的 scheme 在配置装载阶段即被拒绝。
- 内置 **QoS 0/1/2** 消息处理、**遗嘱消息**（Will）、**保留消息**（Retained）、**主题通配符**（`+`、`#`）。
- 提供默认的 **内存持久化**（`MemoryPersistence`），可通过 `Persistence` 接口扩展到 Redis / LevelDB 等外部存储。
- 内置 **访问控制**：CONNECT 用户名/密码校验 + 按身份的主题级 ACL（可限制各身份能发布/订阅的主题模式），默认 fail-closed。遗嘱主题同样受 ACL 约束。
- 运行期计数器 `BrokerStats` 已在连接/订阅/收发布/投递/丢弃各路径真实累加，通过 `(*Hrotti).Stats()` 取快照（连接数、收发布量、投递量、丢弃量、订阅数、运行时长）。

> 本仓库是对上游 [alsm/hrotti](https://github.com/alsm/hrotti) 的**增强与安全加固**分支，重点补齐了原项目在报文解析、并发、资源控制与**访问控制**上的安全隐患。

> **重要**：本 broker **暂不支持 TLS**。`tcp://` 与 `ws://` 两种监听器都是明文传输，用户名与密码以明文过网。生产部署必须放在可信网络内，或在前置反代/网关上终止 TLS（反向代理需自行处理证书与 `X-Forwarded-*`）。详见第九节「安全边界」。

### 二、快速开始

> **环境要求**：本项目在 `go.mod` 中声明 `go 1.27.4`（与 CI 的 `go-version: '1.27.4'`、Dockerfile 的 `golang:1.27.4-alpine` 一致）。未声明 `toolchain` 指令，因此不会自动下载其它工具链；如显式指定，可用 `GOTOOLCHAIN=go1.27.4 go build`。

```bash
go mod tidy
go build ./...

# 版本号(与仓库根目录 VERSION 文件一致)
go run . --version

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
| `-conf`    | 空     | JSON 配置文件路径；提供后按配置启动多个 TCP / WebSocket 监听器，并从 `auth` 段加载凭据与 ACL。**配置装载失败（含不支持的 listener scheme、非法 `auth` 段）会以退出码 2 终止启动，不会带着半份配置继续运行** |
| `--version` | —    | 打印 `MqttBrokerCore <版本>` 并退出；版本号与仓库根目录 `VERSION` 文件一致（当前 `1.0.20261123`），同一常量也会出现在启动日志里 |
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

- **`maxQueueDepth`**：每个客户端的待发送消息队列大小。**省略该键（或给出 0 / 负值）时兜底为 100**——0 会造出无缓冲 channel，导致全部投递被丢弃（表现为"能连上但收不到任何消息"）。显式给出正数时按给定值生效。
- **`listeners`**：键名任意，`url` 仅支持 `tcp://` 或 `ws://`（其它 scheme 在配置装载阶段直接报错并终止启动）。
- **`auth`**（可选，但**强烈建议配置**）：见下方「访问控制」一节。
- **`logging`**：将 `info` / `protocol` / `error` / `debug` 分别定向到 `stdout`、`stderr` 或 `discard`。

> **未配置 `auth` 时的行为**：broker 启动会打印 WARNING，并进入 **fail-closed** 状态——拒绝一切 CONNECT（`CONNACK 0x04`）。这是有意的："忘了配鉴权"的后果是所有人都连不上，而不是人人都能连。

### 六、访问控制

#### 6.1 身份校验（CONNECT）

`auth.allowed` 每项声明一个身份：用户名、密码、以及该身份允许 pub/sub 的主题过滤器列表。

- 密码用 `crypto/subtle.ConstantTimeCompare` **常量时间比较**。用户名不存在时也会走一次对定长（32 字节，`"dummy-password-for-timing-parity"`）字符串的比较，让"查不到用户"这条路径同样付出一次可观的比较代价。
  - 实测边界（如实说明，不夸大）：`subtle.ConstantTimeCompare` 在两侧长度不等时立即返回 false、不进入逐字节比较，因此**它抹平的只是"等长但内容不同"的时序差**；密码长度恰好等于真实密码长度、而用户名不存在的组合仍可被时长区分。账号枚举的主要防线是下面一条（三种失败统一返回 `0x04`，返回码不可区分），时序侧信道只是被削弱而非消除。
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
| **遗嘱主题越权**（`WillTopic` 不在本身份 `pubTopics` 范围内） | 回 CONNACK 后立即关闭连接，不建立会话 | `0x05 CONN_REF_NOT_AUTH` |
| 订阅未授权主题 | 不注册订阅，回 SUBACK | `0x80`（SUBACK Failure） |
| 发布到未授权主题（QoS 0） | 静默丢弃，不下发、不入保留消息 | 无回包 |
| 发布到未授权主题（QoS 1/2） | 丢弃，但**仍回 PUBACK/PUBREC** | 正常 ack |

> **为什么遗嘱要在 CONNECT 阶段裁决**：遗嘱消息在客户端异常断开时由 `client.Stop` → `DeliverMessage` **直接投递**，不经过 `client.Receive` 里那条唯一的发布 ACL 检查点。若不在此处校验，`WillTopic` 就是一条绕过全部发布 ACL 的通道——连上时声明任意主题的遗嘱，断线即由 broker 代身份发布。越权遗嘱计入 `GetAuthStats().PubRejected`。
>
> 关于"不泄漏主题存在性"：未授权订阅的 `0x80` 与"写错了主题名"完全一致，越权发布仍照常 ack。客户端因此无法用返回码或 ack 时序判断某主题是否存在、有谁订阅。（遗嘱拒绝返回 `0x05` 而非 `0x04`，是为了让客户端能区分"凭据问题"与"权限问题"，但这只在**已经通过身份校验**的连接上发生，不构成账号枚举。）

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

#### 6.5 运行期计数器

```go
s := h.Stats()            // 返回 StatsSnapshot, 字段为原子读的一致性快照
fmt.Println(s.ClientsConnected, s.ClientsTotal, s.ClientsMaximum)
fmt.Println(s.PublishReceived, s.PublishSent, s.PublishDropped)
fmt.Println(s.Subscriptions, s.UptimeSeconds)
```

| 字段 | 含义 | 采集点 |
|------|------|--------|
| `ClientsConnected` / `ClientsTotal` / `ClientsMaximum` / `ClientsDisconnected` | 当前连接数 / 累计连接数 / 并发峰值 / 累计断开数 | `InitClient` 会话建立后 `AddClient()`，会话结束（DISCONNECT / keepalive 超时 / 被接管）`RemoveClient()` |
| `PublishReceived` / `MessagesReceived` / `BytesReceived` | 通过 ACL 的入站发布条数与载荷字节 | `client.Receive` ACL 通过之后 |
| `PublishSent` / `MessagesSent` / `BytesSent` | 成功压入某客户端出站队列的投递条数与载荷字节 | `DeliverMessage` 入队成功分支 |
| `PublishDropped` | 因出站队列满而被丢弃的投递条数 | `DeliverMessage` 的 `select…default` |
| `Subscriptions` | 当前订阅条目数 | `AddSub` 增、`DeleteSub` / `DeleteSubAll` 减 |
| `UptimeSeconds` | 进程内 broker 实例运行秒数 | 由实例创建时刻推算 |

被 ACL 拒绝的发布**不**计入 `PublishReceived`，只计入 `GetAuthStats().PubRejected`——两个计数器的语义互不重叠。

### 七、常见使用场景

| 场景            | 示例                                             | 说明                                       |
|-----------------|--------------------------------------------------|--------------------------------------------|
| 单节点本地测试   | `go run . -conf config.json`（配 `allowAnonymous`）| 默认监听 `0.0.0.0:1883`（可通过 `HROTTI_URL` 覆盖）。无 `auth` 段时 fail-closed。 |
| 多监听 + WebSocket | `go run . -conf config.json`                     | 同时开 TCP 与 WS 监听，共享同一主题树。     |
| 端侧只读上报     | 身份 `pubTopics: ["site/+/reading"]`              | 端侧只能上报读数，不能订阅任何主题。        |
| 运维全量订阅     | 身份 `subTopics: ["#"]`                           | 需显式写 `#` 才被授予；未授权身份订阅 `#` 回 `0x80`。 |
| 持久化会话       | `CleanSession=false` 且使用相同 `ClientIdentifier` | 断线重连后保留未确认的 QoS 1/2 消息与订阅。 |

### 八、内部实现概览

- **`Hrotti`**：broker 实例，持有客户端表、订阅树、持久化器、访问控制器与统计对象（`BrokerStats`，经 `Stats()` 读快照）。
- **位图订阅存储**：`subscriptionMap.subBitmap` 实现 O(1) 主题匹配，支持 `+` / `#` 通配符。
- **消息 ID 池**：`messageIDs` 分配 1–65534，耗尽时返回 `ErrMsgIDsExhausted`，避免写入非法 `MessageID=0`。
- **访问控制**：`AuthManager`（`broker/auth.go`）持有凭据表与 ACL，用 `RWMutex` 保护；ACL 裁决复用既有的 `match()` 主题匹配原语，`#` 命中必须来自显式规则。CONNECT 阶段对 `WillTopic` 也执行 `CanPublish`，堵住遗嘱绕过发布 ACL 的通道。
- **监听器 scheme 校验**：`ParseConfig` 复用 `NewListenerConfigWithError`，只接受 `tcp://` / `ws://`；`Server.AddListener` 只做 `net.Listen("tcp", host)`，故任何"看起来像别的协议"的 URL 都不会被静默当成 TCP。
- **持久化接口**：`Persistence`（`Init` / `Open` / `Add` / `GetAll` / `Delete` / `Close` / `Exists` / `Replace`）。

### 九、安全边界（支持 / 不支持）

**支持**

| 能力 | 说明 |
|------|------|
| CONNECT 用户名/密码校验 | 可配置凭据表，常量时间比较，拒绝时不区分用户名/密码错误 |
| 主题级 ACL | 按身份限制 pub/sub 的主题模式，`#` 需显式授权 |
| 遗嘱主题 ACL | `WillTopic` 在 CONNECT 阶段按 `pubTopics` 裁决，越权回 `0x05` 并断开，堵住"遗嘱绕过发布 ACL" |
| fail-closed 默认 | 未配置凭据表（且未显式 `allowAnonymous`）时拒绝一切连接 |
| 越权不泄漏主题存在性 | 未授权订阅 `0x80`，越权发布照常 ack 后丢弃 |
| 越权发布不写保留消息 | 保留消息不会被未授权客户端污染 |
| 拒绝计数 | `GetAuthStats()` 提供连接/订阅/发布拒绝次数 |
| 运行期计数器 | `(*Hrotti).Stats()` 提供连接数、收发布量、投递量、丢弃量、订阅数、运行时长 |

**暂不支持**

| 能力 | 现状与原因 |
|------|-----------|
| **TLS / MQTTS** | **暂不支持**。`tcp://` / `ws://` 均为明文，用户名密码明文过网。需要在前置反代/网关终止 TLS，或等待后续版本在 broker 内实现。 |
| 匿名访问 | 需显式配置 `allowAnonymous: true`；未配置时一律拒绝 |
| 动态主题过滤器（ACL 规则内的正则） | ACL 规则即 MQTT 主题过滤器（`+` / `#`），不支持正则或 `${username}` 类模板 |
| 用户/ACL 管理 API | 仅启动期通过配置文件或 `SetAuthConfig` 注入，无运行时增删用户的接口 |
| 令牌鉴权（JWT / OAuth2） | 仅用户名+密码静态凭据 |
| $SYS 主题的访问控制 | `$SYS/session_identifier` 等内部发布已按身份 `pubTopics` 裁决；需授权 `$SYS/#`（或精确主题）才能收到 |

> **说明**：broker 向空 `ClientIdentifier` 的客户端投递的 `$SYS/session_identifier`，与 PUBLISH 报文、遗嘱主题走同一套发布 ACL（`CanPublish`）。无权身份连接照常成功（协议层合规），但该发布被抑制并计入 `PubRejected`；显式持有 `$SYS/#` 发布权的身份照常收到。订阅侧仍按 `subTopics` 独立裁决。

### 十、已修复的安全与可靠性问题

| 问题 | 修复 |
|------|------|
| `decodeLength` 无限读取字节导致协议失效 / OOM | 限制最多 4 字节并返回错误 |
| `ReadPacket` 未限制 `RemainingLength`，可分配大内存 | 新增 `MaxRemainingLength`（默认 64 MiB）上限 |
| 报文体短读未检测，导致协议同步失效 | 引入 `decodeReader` 捕获 truncation 错误 |
| `WillFlag=0` 时未检查 `WillQoS` / `WillRetain` | 在 `ConnectPacket.Validate` 中拒绝不一致包 |
| 慢/不再读取的客户端导致发送死锁 | 所有发送改为非阻塞 `select…default` 模式 |
| `NewListenerConfig` 在 URL 解析失败时返回 nil | 新增 `NewListenerConfigWithError` 并校验 scheme |
| **监听器 scheme 未校验：`udp://0.0.0.0:1883` 被静默开成 TCP 监听** | `ParseConfig` 复用 `NewListenerConfigWithError`，配置装载阶段即拒绝 `tcp`/`ws` 以外的 scheme，并终止启动（退出码 2） |
| **配置文件缺 `maxQueueDepth` 时取零值 → 无缓冲队列 → 全部投递被丢弃** | `ParseConfig` 与 `NewHrotti` 对 `<= 0` 兜底为 `DefaultMaxQueueDepth`（100），显式正数按给定值生效 |
| **遗嘱绕过发布 ACL：`WillTopic` 仅在 CONNECT 校验格式，投递路径不过 `CanPublish`** | CONNECT 阶段按身份 `pubTopics` 裁决 `WillTopic`，越权回 `0x05 CONN_REF_NOT_AUTH` 并断开 |
| **`$SYS/session_identifier` 绕过发布 ACL：`InitClient` 直接入队，未经 `CanPublish`** | 入队前按身份 `pubTopics` 裁决，无权则抑制该发布并计入 `PubRejected`；持有 `$SYS/#` 的身份照常收到 |
| **`BrokerStats` 从未实例化、字段全小写无访问器** | 接入连接/收发布/投递/丢弃/订阅各路径，新增 `StatsSnapshot` 与 `(*Hrotti).Stats()` |
| `getClient` 返回 nil 导致空指针 panic | 在 `FindRetained` / `DeliverMessage` 中加空值保护 |
| 空 `ClientIdentifier` 且 `CleanSession=0` 未拒绝 | 按 MQTT 规范返回 `CONN_REF_ID_REJ` |
| **`Username`/`Password` 只解析不使用，任意客户端可订阅 `#` 旁路全部端侧数据** | 新增 `AuthManager`：CONNECT 常量时间凭据校验 + 主题级 ACL，未授权订阅回 `0x80`、越权发布丢弃且照常 ack，默认 fail-closed |

---

> 感谢上游项目 **[alsm/hrotti](https://github.com/alsm/hrotti)** 提供的原始实现与设计思路，本项目在其基础上进行兼容性增强与安全加固。
