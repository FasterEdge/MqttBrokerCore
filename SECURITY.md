# 安全政策

## 支持的能力与安全边界

本节描述 MqttBrokerCore **已实现**与**未实现**的安全能力，与代码逐一对应；不写代码做不到的承诺。

### 已支持

| 能力 | 实现位置 | 边界说明 |
|------|----------|----------|
| CONNECT 用户名/密码校验 | `broker/auth.go` `AuthManager.Authenticate` | 凭据表在启动期由配置文件 `auth.allowed` 或 `SetAuthConfig` 注入。密码用 `crypto/subtle.ConstantTimeCompare` 常量时间比较。用户名不存在、密码错误、未带凭据三种失败**统一返回 `CONNACK 0x04`**，不可用返回码枚举合法用户名。 |
| 时序侧信道（部分缓解，非完全消除） | 同上 | 用户名不存在时会走一次对定长 32 字节串（`"dummy-password-for-timing-parity"`）的比较，使该路径也付出比较代价。但 `ConstantTimeCompare` 在长度不等时立即返回，**等长与不等长密码之间仍存在可区分的时长差异**。时序侧信道被削弱而非消除。 |
| 主题级 ACL（pub/sub 白名单） | `AuthManager.CanPublish` / `CanSubscribe` | 每个身份声明 `pubTopics` / `subTopics`，规则本身是 MQTT 主题过滤器，复用 `match()` 原语裁决。列表为空 = 一律拒绝（白名单语义）。`#` 只对显式写了 `#` 的规则放行，无隐式通配提升。 |
| 遗嘱主题 ACL | `broker/server.go` `InitClient` | `WillTopic` 在 CONNECT 阶段按身份 `pubTopics` 裁决，越权返回 `CONNACK 0x05 (CONN_REF_NOT_AUTH)` 并关闭连接。必要性：遗嘱由 `client.Stop` → `DeliverMessage` 直接投递，不经过 `client.Receive` 的发布 ACL，否则构成绕过全部发布 ACL 的通道。 |
| fail-closed 默认 | `NewAuthManager` / `SetAuthConfig` | 未配置任何凭据且未显式 `allowAnonymous` 时拒绝一切 CONNECT。"忘了配鉴权"的后果是所有人都连不上，不是人人都能连。 |
| 拒绝时不泄漏主题存在性 | `broker/client.go`、`broker/subscriptions.go` | 未授权订阅回 `SUBACK 0x80`，与"主题过滤器非法"完全一致；越权发布仍照常回 PUBACK/PUBREC 后丢弃。客户端无法用返回码或 ack 时序判断主题是否存在。 |
| 越权发布不写保留消息 | `broker/client.go` | 保留消息写入在 ACL 检查之后。 |
| 监听器 scheme 白名单 | `broker/config.go`、`config.go` `ParseConfig` | 只接受 `tcp://` 与 `ws://`。`Server.AddListener` 恒 `net.Listen("tcp", host)`，因此配置里写 `udp://` 之类不会开出一个行为与配置不符的监听器；装载阶段即报错并以退出码 2 终止。 |
| 客户端出站队列深度兜底 | `broker/server.go` `NewHrotti` | 队列深度 `<= 0` 时兜底为 100（`DefaultMaxQueueDepth`）。0 深度会造出无缓冲 channel，导致全部投递被静默丢弃。 |
| 单个客户端异常不拖垮进程 | `broker/client.go` `Receive` 的 `defer recover` | 单个客户端畸形数据触发 panic 时断开该客户端并继续服务其余客户端；根因仍依赖各边界检查。 |
| 拒绝计数 | `broker/auth.go` `GetAuthStats` | `ConnRejected` / `SubRejected` / `PubRejected` / `UnknownIdentity`，进程内单调累加，适合按差值算拦截率。 |
| 运行期计数器 | `broker/stats.go`、`(*Hrotti).Stats()` | `StatsSnapshot` 提供连接数（当前/累计/峰值/断开）、收发布量与字节、投递量与字节、丢弃量、订阅数、运行秒数。所有计数均挂在真实代码路径上。 |

### 暂不支持

| 能力 | 现状与原因 |
|------|-----------|
| **TLS / MQTTS** | **未实现**。`tcp://` 与 `ws://` 都是明文传输，用户名与密码以明文过网。生产部署必须置于可信网络内，或在前置反代/网关终止 TLS（反代需自行处理证书与 `X-Forwarded-*`）。 |
| 密码哈希存储 | 配置文件中密码为明文，只在内存中与用户输入比较，不写日志、不进持久化。 |
| 运行时用户/ACL 管理 | 仅启动期经配置文件或 `SetAuthConfig` 注入；`SetAuthConfig` 可整表替换（用于热更新失败时的安全降级），没有单用户增删接口。 |
| 令牌鉴权（JWT / OAuth2 / mTLS 客户端证书） | 仅静态用户名 + 密码。 |
| ACL 规则中的正则 / 模板 | ACL 规则即 MQTT 主题过滤器（`+` / `#`），不支持正则或 `${username}` 类模板。 |
| `$SYS` 主题的 ACL | `$SYS/session_identifier` 由 `InitClient` 在会话建立后直接入队，未经过 ACL 过滤。它只泄漏服务端生成的 UUID，不含其它客户端数据。 |
| 跨实例统计/监控端点 | `Stats()` 是进程内 API，没有内置 HTTP `$SYS` 或 Prometheus exporter。 |

## 支持的版本

| 版本 | 支持状态 |
| ---- | -------- |
| main (开发分支) | ✅ 支持 |
| 1.0.20261123 | ✅ 支持 |

## 报告安全漏洞

FasterEdge 项目重视安全。请**不要在公共渠道（Issue/讨论区/群聊）公开未修复的漏洞细节**。

请通过以下渠道私密报告：

- **GitHub Security Advisory**（推荐）：在本仓库的 *Security* → *Report a vulnerability* 提交，或直接访问
  `https://github.com/FasterEdge/MqttBrokerCore/security/advisories/new`
- **邮件**：`shun_@outlook.com`（主题请注明 `[FasterEdge Security]`）

### 报告中请包含

1. 漏洞类型与影响组件
2. 触发条件与复现步骤（尽量精简）
3. 影响范围（可读性 / 可用性 / 安全性）
4. 建议修复方向（可选）

### 响应承诺

- **48 小时内**：确认收到报告并评估严重性
- **7 天内**：给出修复计划或缓解措施
- **修复发版后**：在致谢中列入报告者（如同意）

## 致谢

我们感谢所有以负责任方式报告问题的安全研究人员。
