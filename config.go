// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	. "github.com/FasterEdge/MqttBrokerCore/broker"
)

type ListenerEntry struct {
	URL string `json:"url"`
}

// AuthEntry 是配置文件中一条身份(用户名/密码 + 该身份被允许 pub/sub
// 的主题过滤器)。字段名与 broker.Credentials 的 JSON tag 一致, 由
// ParseConfig 直接解码为 broker.AuthConfig。
type AuthEntry struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	PubTopics []string `json:"pubTopics"`
	SubTopics []string `json:"subTopics"`
}

// AuthSection 是配置文件的 "auth" 段。Allowed 与 AllowAnonymous 至少
// 给出一个; 两者都缺省时 broker 进入 fail-closed 模式(拒绝一切连接),
// 这是有意的——"忘了配鉴权"必须是连不上, 不能是人人都能连。
type AuthSection struct {
	Allowed        []AuthEntry `json:"allowed"`
	AllowAnonymous bool        `json:"allowAnonymous"`
}

// Current configuration struct, maxQueueDepth sets the maximum number of unacknowledged mesages
// for a client. Listeners is a slice of ListenerConfigs
type BrokerConfig struct {
	MaxQueueDepth   int                       `json:"maxQueueDepth"`
	ListenerEntries map[string]*ListenerEntry `json:"listeners"`
	Listeners       map[string]*ListenerConfig
	Auth            AuthSection `json:"auth"`
	Logging         struct {
		Info     string `json:"info"`
		Protocol string `json:"protocol"`
		Errlog   string `json:"error"`
		Debug    string `json:"debug"`
	}
}

// AuthConfigured 报告配置文件是否显式声明了 auth 段。用于 main() 在
// 无 auth 配置时打印醒目告警, 而不是静默跑成一个无鉴权 broker。
func (c *BrokerConfig) AuthConfigured() bool {
	return c.Auth.AllowAnonymous || len(c.Auth.Allowed) > 0
}

// ApplyAuth 把配置文件的 auth 段下发给 broker 的访问控制器。
// 未配置时传入 nil → fail-closed。返回的 error 只来自配置本身非法
// (重复用户名/空用户名/匿名与凭据混用), 此时必须终止启动。
func (c *BrokerConfig) ApplyAuth(h *Hrotti) error {
	if !c.AuthConfigured() {
		return h.SetAuthConfig(nil)
	}
	cfg := &AuthConfig{AllowAnonymous: c.Auth.AllowAnonymous}
	for _, e := range c.Auth.Allowed {
		cfg.AllowedCredentials = append(cfg.AllowedCredentials, Credentials{
			Username:  e.Username,
			Password:  e.Password,
			PubTopics: e.PubTopics,
			SubTopics: e.SubTopics,
		})
	}
	return h.SetAuthConfig(cfg)
}

var logTargets map[string]io.Writer = map[string]io.Writer{
	"stdout":  os.Stdout,
	"stderr":  os.Stderr,
	"discard": io.Discard,
}

func (c *BrokerConfig) SetLogTargets() {
	target, ok := logTargets[c.Logging.Info]
	if !ok {
		target = os.Stdout
	}
	INFO = log.New(target, "INFO: ", log.Ldate|log.Ltime)
	target, ok = logTargets[c.Logging.Protocol]
	if !ok {
		target = io.Discard
	}
	PROTOCOL = log.New(target, "PROTOCOL: ", log.Ldate|log.Ltime)
	target, ok = logTargets[c.Logging.Errlog]
	if !ok {
		target = os.Stderr
	}
	ERROR = log.New(target, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	target, ok = logTargets[c.Logging.Debug]
	if !ok {
		target = io.Discard
	}
	DEBUG = log.New(target, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func ParseConfig(confFile string, confVar *BrokerConfig) error {
	file, err := os.Open(confFile)
	if err != nil {
		return err
	}
	// 不关闭句柄会让调用方(尤其是测试的 t.TempDir)无法删除配置文件,
	// Windows 上表现为"进程仍在访问该文件"。旧实现泄漏该 fd。
	defer file.Close()
	decoder := json.NewDecoder(file)

	err = decoder.Decode(confVar)
	if err != nil {
		return err
	}

	// 监听器 URL 在装载阶段就校验 scheme: 旧实现只做 url.Parse,
	// server.AddListener 恒 net.Listen("tcp", ...) 且只在 Scheme=="ws" 时
	// 走 WebSocket 分支, 因此 "udp://0.0.0.0:1883" 会被静默开成 TCP 监听
	// (配置说是 UDP, 实际是 TCP)。这里复用 NewListenerConfigWithError 的
	// 校验逻辑, 让不支持的 scheme 在启动前就失败。
	if confVar.Listeners == nil {
		confVar.Listeners = make(map[string]*ListenerConfig)
	}
	for name, entry := range confVar.ListenerEntries {
		if entry == nil {
			return fmt.Errorf("listener %q: missing url", name)
		}
		lc, err := NewListenerConfigWithError(entry.URL)
		if err != nil {
			return fmt.Errorf("listener %q: %w", name, err)
		}
		confVar.Listeners[name] = lc
	}

	// maxQueueDepth 缺省(0)兜底: 0 会让 newClient 建出无缓冲 channel,
	// 投递全部走 select 的 default 分支被丢弃。负值同样无意义, 一并兜底。
	if confVar.MaxQueueDepth <= 0 {
		confVar.MaxQueueDepth = DefaultMaxQueueDepth
	}
	return nil
}

// validateAuthEntries 在解析后立刻校验 auth 段, 让配置错误在启动阶段
// 暴露(而不是等到某个客户端连上来才被拒绝)。只做结构性检查: 用户名非空、
// 密码非空、主题过滤器基本合法。真正的 ACL 裁决在 broker 内完成。
func (c *BrokerConfig) validateAuthEntries() error {
	if !c.AuthConfigured() {
		return nil
	}
	seen := make(map[string]bool, len(c.Auth.Allowed))
	for _, e := range c.Auth.Allowed {
		if e.Username == "" {
			return fmt.Errorf("auth: username must not be empty")
		}
		if e.Password == "" {
			return fmt.Errorf("auth: password for %q must not be empty", e.Username)
		}
		if seen[e.Username] {
			return fmt.Errorf("auth: duplicate username %q", e.Username)
		}
		seen[e.Username] = true
		for _, t := range append(append([]string(nil), e.PubTopics...), e.SubTopics...) {
			if t == "" {
				return fmt.Errorf("auth: empty topic filter for %q", e.Username)
			}
		}
	}
	return nil
}
