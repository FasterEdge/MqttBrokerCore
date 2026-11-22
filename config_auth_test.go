// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/FasterEdge/MqttBrokerCore/broker"
)

func writeConf(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

// TestParseConfigAuthSection 验证 auth 段能被解析并真正下发到 broker 的
// 访问控制器(而不是"读进结构体但没人用")。
func TestParseConfigAuthSection(t *testing.T) {
	p := writeConf(t, `{
  "maxQueueDepth": 32,
  "listeners": {"tcp": {"url": "tcp://127.0.0.1:1883"}},
  "auth": {
    "allowed": [
      {"username": "sensor", "password": "pw", "pubTopics": ["site/+/reading"], "subTopics": ["site/+/reading"]},
      {"username": "ops", "password": "pw2", "subTopics": ["#"]}
    ]
  }
}`)
	var cfg BrokerConfig
	cfg.Listeners = make(map[string]*ListenerConfig)
	if err := ParseConfig(p, &cfg); err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if !cfg.AuthConfigured() {
		t.Fatal("auth section parsed but AuthConfigured() == false")
	}
	if err := cfg.validateAuthEntries(); err != nil {
		t.Fatalf("validateAuthEntries: %v", err)
	}

	h := NewHrotti(cfg.MaxQueueDepth, &MemoryPersistence{})
	if err := cfg.ApplyAuth(h); err != nil {
		t.Fatalf("ApplyAuth: %v", err)
	}
	if got := h.AuthManager().Mode(); got != "strict" {
		t.Fatalf("mode = %q, want strict", got)
	}
	if id, ok := h.AuthManager().Authenticate("sensor", []byte("pw")); !ok || id != "sensor" {
		t.Fatalf("sensor auth = (%q,%v), want (sensor,true)", id, ok)
	}
	if _, ok := h.AuthManager().Authenticate("sensor", []byte("pw-wrong")); ok {
		t.Fatal("wrong password accepted")
	}
	// ACL 必须随配置生效: ops 有 '#' 订阅权, sensor 没有。
	if !h.AuthManager().CanSubscribe("ops", "#") {
		t.Fatal("ops lost explicitly granted '#'")
	}
	if h.AuthManager().CanSubscribe("sensor", "#") {
		t.Fatal("sensor granted '#' without explicit grant")
	}
	if !h.AuthManager().CanPublish("sensor", "site/7/reading") {
		t.Fatal("sensor lost granted publish topic")
	}
	if h.AuthManager().CanPublish("sensor", "site/7/other") {
		t.Fatal("sensor got unauthorised publish topic")
	}
}

// TestConfigWithoutAuthIsFailClosed 覆盖"配置文件里没有 auth 段": broker
// 必须进入 fail-closed, 不能因为漏配而默认敞开。
func TestConfigWithoutAuthIsFailClosed(t *testing.T) {
	p := writeConf(t, `{
  "maxQueueDepth": 16,
  "listeners": {"tcp": {"url": "tcp://127.0.0.1:1883"}}
}`)
	var cfg BrokerConfig
	cfg.Listeners = make(map[string]*ListenerConfig)
	if err := ParseConfig(p, &cfg); err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.AuthConfigured() {
		t.Fatal("AuthConfigured() == true without auth section")
	}
	h := NewHrotti(cfg.MaxQueueDepth, &MemoryPersistence{})
	if err := cfg.ApplyAuth(h); err != nil {
		t.Fatalf("ApplyAuth: %v", err)
	}
	if got := h.AuthManager().Mode(); got != "closed" {
		t.Fatalf("mode = %q, want closed", got)
	}
	if _, ok := h.AuthManager().Authenticate("anyone", []byte("anything")); ok {
		t.Fatal("fail-closed broker authenticated a client")
	}
}

// TestParseConfigRejectsUnsupportedScheme 覆盖"配置里写了 udp://"这一类
// 不支持的 scheme: 必须在配置装载阶段失败。旧实现只做 url.Parse, 而
// server.AddListener 恒 net.Listen("tcp", ...)、只在 Scheme=="ws" 时走
// WebSocket 分支, 因此 udp://0.0.0.0:1883 会被静默开成 TCP 监听
// (配置说是 UDP, 实际是 TCP)。
func TestParseConfigRejectsUnsupportedScheme(t *testing.T) {
	for _, raw := range []string{
		"udp://0.0.0.0:1883",
		"http://0.0.0.0:1883",
		"https://0.0.0.0:1883",
		"tls://0.0.0.0:1883",
		"mqtts://0.0.0.0:1883",
		"://no-scheme",
	} {
		t.Run(raw, func(t *testing.T) {
			var cfg BrokerConfig
			cfg.Listeners = make(map[string]*ListenerConfig)
			err := ParseConfig(writeConf(t, `{"listeners":{"bad":{"url":"`+raw+`"}}}`), &cfg)
			if err == nil {
				t.Fatalf("scheme %q accepted by ParseConfig", raw)
			}
			if len(cfg.Listeners) != 0 {
				t.Fatalf("rejected config still produced listeners: %v", cfg.Listeners)
			}
		})
	}
}

// TestParseConfigAcceptsTcpAndWs 是上面的正向对照: tcp:// 与 ws:// 必须
// 继续被接受(且 http.Handle 依赖的 Path 被保留), 不能被"一刀切"误伤。
func TestParseConfigAcceptsTcpAndWs(t *testing.T) {
	for _, raw := range []string{"tcp://127.0.0.1:1883", "ws://0.0.0.0:2000/mqtt"} {
		t.Run(raw, func(t *testing.T) {
			var cfg BrokerConfig
			cfg.Listeners = make(map[string]*ListenerConfig)
			if err := ParseConfig(writeConf(t, `{"listeners":{"ok":{"url":"`+raw+`"}}}`), &cfg); err != nil {
				t.Fatalf("ParseConfig(%q): %v", raw, err)
			}
			if cfg.Listeners["ok"] == nil || cfg.Listeners["ok"].URL == nil {
				t.Fatalf("listener for %q not built", raw)
			}
		})
	}
}

// TestParseConfigMaxQueueDepthFallsBack 覆盖缺省值: 配置里完全没有
// maxQueueDepth(以及给出 0 / 负值)时, 装载后必须是 100。旧实现在配置文件
// 路径下不赋值, JSON 零值 0 直接传到 NewHrotti → newClient 建出无缓冲
// channel → 所有投递走 select 的 default 被丢弃(能连上但收不到任何消息)。
func TestParseConfigMaxQueueDepthFallsBack(t *testing.T) {
	cases := map[string]string{
		"key absent":   `{"listeners":{"tcp":{"url":"tcp://127.0.0.1:1883"}}}`,
		"zero":         `{"maxQueueDepth": 0, "listeners":{"tcp":{"url":"tcp://127.0.0.1:1883"}}}`,
		"negative":     `{"maxQueueDepth": -5, "listeners":{"tcp":{"url":"tcp://127.0.0.1:1883"}}}`,
		"explicit 256": `{"maxQueueDepth": 256, "listeners":{"tcp":{"url":"tcp://127.0.0.1:1883"}}}`,
	}
	want := map[string]int{
		"key absent":   DefaultMaxQueueDepth,
		"zero":         DefaultMaxQueueDepth,
		"negative":     DefaultMaxQueueDepth,
		"explicit 256": 256,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var cfg BrokerConfig
			cfg.Listeners = make(map[string]*ListenerConfig)
			if err := ParseConfig(writeConf(t, body), &cfg); err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if cfg.MaxQueueDepth != want[name] {
				t.Fatalf("MaxQueueDepth = %d, want %d", cfg.MaxQueueDepth, want[name])
			}
		})
	}
	if DefaultMaxQueueDepth != 100 {
		t.Fatalf("DefaultMaxQueueDepth = %d, want 100 (README 声明值)", DefaultMaxQueueDepth)
	}
}

// TestVersionConstantMatchesVERSIONFile 钉住"VERSION 文件不再无人引用":
// main.Version 必须与仓库根目录 VERSION 文件内容一致。改动版本时只改一处
// 会让 --version 与 VERSION 文件各说各话, 答辩时这是最容易被问住的一处。
func TestVersionConstantMatchesVERSIONFile(t *testing.T) {
	// 包 main 位于仓库根目录, go test 的工作目录即该包目录, 故 VERSION 在此。
	body, err := os.ReadFile("VERSION")
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}
	if want := strings.TrimSpace(string(body)); want != Version {
		t.Fatalf("main.Version = %q, VERSION file = %q", Version, want)
	}
}

// TestConfigRejectsBadAuthEntries 覆盖明显错误的 auth 段: 空用户名、
// 空密码、重复用户名都必须让启动阶段失败(而非运行时静默丢身份)。
func TestConfigRejectsBadAuthEntries(t *testing.T) {
	cases := map[string]string{
		"empty username": `{"auth": {"allowed": [{"username": "", "password": "p"}]}}`,
		"empty password": `{"auth": {"allowed": [{"username": "u", "password": ""}]}}`,
		"duplicate user": `{"auth": {"allowed": [{"username": "u", "password": "p"}, {"username": "u", "password": "q"}]}}`,
		"empty topic":    `{"auth": {"allowed": [{"username": "u", "password": "p", "subTopics": [""]}]}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var cfg BrokerConfig
			cfg.Listeners = make(map[string]*ListenerConfig)
			if err := ParseConfig(writeConf(t, body), &cfg); err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if err := cfg.validateAuthEntries(); err == nil {
				t.Fatalf("invalid auth config accepted: %s", body)
			}
		})
	}
}

// TestApplyAuthAnonymous 覆盖显式匿名配置: 只有写了 allowAnonymous 才关闭
// 访问控制, 并且此时 '#' 订阅放行。
func TestApplyAuthAnonymous(t *testing.T) {
	p := writeConf(t, `{"auth": {"allowAnonymous": true}}`)
	var cfg BrokerConfig
	cfg.Listeners = make(map[string]*ListenerConfig)
	if err := ParseConfig(p, &cfg); err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	h := NewHrotti(8, &MemoryPersistence{})
	if err := cfg.ApplyAuth(h); err != nil {
		t.Fatalf("ApplyAuth: %v", err)
	}
	if got := h.AuthManager().Mode(); got != "anonymous" {
		t.Fatalf("mode = %q, want anonymous", got)
	}
	if !h.AuthManager().CanSubscribe(AnonymousIdentity, "#") {
		t.Fatal("anonymous mode denied '#'")
	}
}
