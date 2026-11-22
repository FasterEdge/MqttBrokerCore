// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package main

import (
	"os"
	"path/filepath"
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
