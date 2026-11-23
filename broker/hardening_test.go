// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import (
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/FasterEdge/MqttBrokerCore/packets"
)

// ── 遗嘱主题的发布 ACL ─────────────────────────────────────
//
// 遗嘱在客户端异常断开时由 client.Stop → DeliverMessage 直接投递, 不经过
// client.Receive 里的 PUBLISH ACL 裁决, 因此若 CONNECT 阶段不校验 WillTopic,
// 它就是一条绕过全部发布 ACL 的通道(声明任意主题的遗嘱, 断线即由 broker
// 代发布)。以下用例钉住该裁决。

// dialWill 发送一个带遗嘱的 CONNECT, 返回连接与 CONNACK 的返回码。
// willTopic 为空时不置 WillFlag(用于对照: 无遗嘱的连接不受影响)。
func dialWill(t *testing.T, addr, clientID, user string, pass []byte, willTopic string) (net.Conn, byte) {
	t.Helper()
	cp := connectPacket(clientID, user, pass)
	if willTopic != "" {
		cp.WillFlag = true
		cp.WillQos = 0
		cp.WillTopic = willTopic
		cp.WillMessage = []byte("gone")
	}
	conn, ca := dial(t, addr, cp)
	return conn, ca.ReturnCode
}

// TestWillTopicOutsideACLIsRejected 覆盖 Me 5: sensor 只有 site/+/reading 的
// 发布权, 声明遗嘱主题 admin/secret 必须被拒绝连接(0x05 NOT_AUTH), 且连接
// 随即关闭; 而落在已授权范围内的遗嘱主题必须照常接受。
func TestWillTopicOutsideACLIsRejected(t *testing.T) {
	b := startTestBroker(t, aclFixture())
	ResetAuthStatsForTest()

	conn, rc := dialWill(t, b.addr, "will-bad", "sensor", []byte("sensor-pw"), "admin/secret")
	defer conn.Close()
	if rc != CONN_REF_NOT_AUTH {
		t.Fatalf("out-of-ACL will topic: rc=0x%02x, want 0x%02x", rc, CONN_REF_NOT_AUTH)
	}
	// 拒绝后连接必须被关闭: 读应立刻失败(EOF/reset), 而不是挂着等。
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := ReadPacket(conn); err == nil {
		t.Fatal("connection still open after will-topic rejection")
	}
	if GetAuthStats().PubRejected == 0 {
		t.Fatal("PubRejected counter not incremented for will-topic rejection")
	}

	// 正向对照: 授权范围内的遗嘱主题必须被接受。
	okConn, rc2 := dialWill(t, b.addr, "will-good", "sensor", []byte("sensor-pw"), "site/9/reading")
	defer okConn.Close()
	if rc2 != CONN_ACCEPTED {
		t.Fatalf("in-ACL will topic rejected: rc=0x%02x", rc2)
	}

	// 无遗嘱的连接不受影响(不能因新增校验误伤普通连接)。
	plain, rc3 := dialWill(t, b.addr, "no-will", "sensor", []byte("sensor-pw"), "")
	defer plain.Close()
	if rc3 != CONN_ACCEPTED {
		t.Fatalf("will-less connection rejected: rc=0x%02x", rc3)
	}
}

// TestWillTopicAllowedInAnonymousMode 覆盖显式匿名模式: 访问控制整体关闭时
// 任何合法遗嘱主题都应放行, 不能被新校验误拒。
func TestWillTopicAllowedInAnonymousMode(t *testing.T) {
	b := startTestBroker(t, &AuthConfig{AllowAnonymous: true})
	conn, rc := dialWill(t, b.addr, "anon-will", "", nil, "anything/at/all")
	defer conn.Close()
	if rc != CONN_ACCEPTED {
		t.Fatalf("anonymous mode rejected will topic: rc=0x%02x", rc)
	}
}

// ── maxQueueDepth 兜底 ────────────────────────────────────

// TestQueueDepthFallbackAppliesToChannels 证明"兜底 100"不是只改了一个整
// 数字段: 传入 0 时 NewHrotti 必须把客户端出站队列建成容量 100 的缓冲
// channel。容量为 0 即无缓冲, select 的 default 分支会让每条投递被丢弃。
func TestQueueDepthFallbackAppliesToChannels(t *testing.T) {
	for _, in := range []int{0, -1} {
		h := NewHrotti(in, &MemoryPersistence{})
		if h.maxQueueDepth != DefaultMaxQueueDepth {
			t.Fatalf("NewHrotti(%d): maxQueueDepth=%d, want %d", in, h.maxQueueDepth, DefaultMaxQueueDepth)
		}
		c := newClient(nil, "c", "id", h.maxQueueDepth)
		if got := cap(c.outboundMessages); got != DefaultMaxQueueDepth {
			t.Fatalf("NewHrotti(%d): cap(outboundMessages)=%d, want %d", in, got, DefaultMaxQueueDepth)
		}
		if got := cap(c.outboundPriority); got != DefaultMaxQueueDepth {
			t.Fatalf("NewHrotti(%d): cap(outboundPriority)=%d, want %d", in, got, DefaultMaxQueueDepth)
		}
	}
	// 显式给出正数时必须尊重调用方, 不能被兜底覆盖。
	h := NewHrotti(3, &MemoryPersistence{})
	if c := newClient(nil, "c", "id", h.maxQueueDepth); cap(c.outboundMessages) != 3 {
		t.Fatalf("explicit depth ignored: cap=%d, want 3", cap(c.outboundMessages))
	}
}

// ── BrokerStats 接线 ──────────────────────────────────────

// TestBrokerStatsCountsRealTraffic 走真实 TCP 连接验证连接数/订阅数/入站
// 发布计数确实被累加, 断开后连接数回落(而不是只增不减)。
func TestBrokerStatsCountsRealTraffic(t *testing.T) {
	b := startTestBroker(t, aclFixture())

	conn := mustConnect(t, b.addr, "stats-1", "sensor", []byte("sensor-pw"))
	if s := waitForStats(t, b.h, func(s StatsSnapshot) bool { return s.ClientsConnected == 1 }); s.ClientsTotal < 1 {
		t.Fatalf("ClientsTotal = %d, want >= 1", s.ClientsTotal)
	}

	if gq := subscribe(t, conn, 1, "site/+/reading"); gq[0] != 0 {
		t.Fatalf("subscribe granted %v, want [0]", gq)
	}
	if s := waitForStats(t, b.h, func(s StatsSnapshot) bool { return s.Subscriptions >= 1 }); s.Subscriptions < 1 {
		t.Fatalf("Subscriptions = %d, want >= 1", s.Subscriptions)
	}

	publish(t, conn, "site/7/reading", 0, 0, "42.0")
	if s := waitForStats(t, b.h, func(s StatsSnapshot) bool { return s.PublishReceived >= 1 }); s.PublishReceived < 1 {
		t.Fatalf("PublishReceived = %d, want >= 1", s.PublishReceived)
	} else if s.BytesReceived < int64(len("42.0")) {
		t.Fatalf("BytesReceived = %d, want >= %d", s.BytesReceived, len("42.0"))
	}

	// 自订阅会收到一条投递: PublishSent 必须相应增加(证明投递侧也接了线)。
	if s := waitForStats(t, b.h, func(s StatsSnapshot) bool { return s.PublishSent >= 1 }); s.PublishSent < 1 {
		t.Fatalf("PublishSent = %d, want >= 1", s.PublishSent)
	}

	_ = conn.Close()
	if s := waitForStats(t, b.h, func(s StatsSnapshot) bool { return s.ClientsConnected == 0 }); s.ClientsConnected != 0 {
		t.Fatalf("ClientsConnected = %d after disconnect, want 0", s.ClientsConnected)
	} else if s.ClientsDisconnected < 1 {
		t.Fatalf("ClientsDisconnected = %d, want >= 1", s.ClientsDisconnected)
	}
	if b.h.Stats().ClientsTotal < 1 {
		t.Fatal("ClientsTotal lost after disconnect")
	}
}

// TestBrokerStatsDroppedIsWired 证明"丢弃"计数挂在真实的投递路径上: 把某
// 客户端的出站队列填满后投递, DeliverMessage 会走 select 的 default 分支,
// PublishDropped 必须 +1; 队列空闲时同一路径走 PublishSent。
func TestBrokerStatsDroppedIsWired(t *testing.T) {
	h := NewHrotti(1, &MemoryPersistence{})
	if err := h.SetAuthConfig(&AuthConfig{AllowAnonymous: true}); err != nil {
		t.Fatalf("SetAuthConfig: %v", err)
	}
	c := newClient(nil, "drop-1", AnonymousIdentity, h.maxQueueDepth)
	c.state.SetValue(CONNECTED)
	h.AddSub("drop-1", "t/1", 0)
	h.clients.Lock()
	h.clients.list["drop-1"] = c
	h.clients.Unlock()
	// 填满队列(容量 1)。
	c.outboundMessages <- NewControlPacket(PUBLISH).(*PublishPacket)

	msg := NewControlPacket(PUBLISH).(*PublishPacket)
	msg.TopicName = "t/1"
	msg.Qos = 0
	msg.Payload = []byte("x")
	h.DeliverMessage("t/1", msg)
	h.DeliverMessage("t/1", msg) // 队列仍满, 第二条同样被丢弃

	if got := h.Stats().PublishDropped; got != 2 {
		t.Fatalf("PublishDropped = %d, want 2", got)
	}
	if got := h.Stats().PublishSent; got != 0 {
		t.Fatalf("PublishSent = %d, want 0 (queue was full)", got)
	}
	// 排空队列后再投递, 必须计为已发送。
	<-c.outboundMessages
	h.DeliverMessage("t/1", msg)
	if got := h.Stats().PublishSent; got != 1 {
		t.Fatalf("PublishSent = %d, want 1", got)
	}
}

// TestZeroValueHrottiStatsIsSafe 覆盖直接 &Hrotti{} 构造的实例: Stats() 与
// 计数路径都不能 panic(库使用者可能不经 NewHrotti)。
func TestZeroValueHrottiStatsIsSafe(t *testing.T) {
	h := &Hrotti{}
	if got := h.Stats(); got != (StatsSnapshot{}) {
		t.Fatalf("zero-value Hrotti Stats() = %+v, want zero", got)
	}
	h.noteSent(1)
	h.noteDropped()
	h.noteSubscribed()
	h.noteUnsubscribed()
}

// ── $SYS/session_identifier 的发布 ACL ────────────────────
//
// 该发布由 InitClient 在会话建立后直接入队, 与 PUBLISH 报文、遗嘱主题
// 并列构成"broker 代身份发布"的三条通道。历史上它未经任何 ACL 裁决,
// 于是一个对 $SYS 毫无权限的身份连上即由 broker 代其向 $SYS 发布——
// 与遗嘱主题越权同一类绕过。以下用例钉住"裁决真的接在发布路径上"。

// sysFixture 在 aclFixture 之外补一个持有 $SYS/# 发布权的身份: 既要
// 证明未授权身份被拒, 也要证明已授权身份照常收到, 缺任一侧都只能证明
// "什么都不发", 证明不了"按 ACL 发"。
func sysFixture() *AuthConfig {
	return &AuthConfig{AllowedCredentials: []Credentials{
		{
			Username:  "plain",
			Password:  "plain-pw",
			PubTopics: []string{"only/here"},
			SubTopics: []string{"only/here"},
		},
		{
			Username:  "syspub",
			Password:  "syspub-pw",
			PubTopics: []string{"$SYS/#"},
			SubTopics: []string{"$SYS/#"},
		},
	}}
}

// connectNoClientID 发一个 ClientIdentifier 为空的 CONNECT(MQTT 3.1.1
// §3.1.3.1 要求此时 CleanSession=1), broker 会自行生成 UUID 并通过
// $SYS/session_identifier 回给客户端, 这是该内部发布的唯一触发条件。
func connectNoClientID(t *testing.T, addr, user string, pass []byte) net.Conn {
	t.Helper()
	conn, ca := dial(t, addr, connectPacket("", user, pass))
	if ca.ReturnCode != CONN_ACCEPTED {
		_ = conn.Close()
		t.Fatalf("connect %q rejected: rc=0x%02x", user, ca.ReturnCode)
	}
	return conn
}

// TestSessionIdentifierPublishIsACLChecked 覆盖修复前的缺口: InitClient
// 把 $SYS/session_identifier 塞进出站队列时没有过 CanPublish, 无 $SYS
// 发布权(= 全部身份, 因为既有 ACL 夹具都不含 $SYS)也能让 broker 代其
// 发布。修复后它按 PubTopics 裁决: 无权者收不到且计入 PubRejected,
// 显式持有 $SYS/# 者必须照常收到(否则就是过度收紧)。
func TestSessionIdentifierPublishIsACLChecked(t *testing.T) {
	b := startTestBroker(t, sysFixture())
	ResetAuthStatsForTest()

	// 未授权身份: 空 ClientID 连接被接受(协议层合规), 但不得收到 $SYS 发布。
	denied := connectNoClientID(t, b.addr, "plain", []byte("plain-pw"))
	defer denied.Close()
	if got := drainTopics(t, denied, 500*time.Millisecond); len(got) != 0 {
		t.Fatalf("unauthorized identity received internal publish: %v", got)
	}
	if GetAuthStats().PubRejected == 0 {
		t.Fatal("PubRejected not incremented for suppressed $SYS publish")
	}

	// 已授权身份: 同一条发布必须真的到达, 证明裁决不会误伤。
	allowed := connectNoClientID(t, b.addr, "syspub", []byte("syspub-pw"))
	defer allowed.Close()
	seen := false
	for _, topic := range drainTopics(t, allowed, 2*time.Second) {
		if topic == "$SYS/session_identifier" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("authorized identity missed $SYS/session_identifier")
	}

	// 匿名模式(显式关闭访问控制)语义与 PUBLISH / 遗嘱一致: 放行。
	anon := startTestBroker(t, &AuthConfig{AllowAnonymous: true})
	anonConn := connectNoClientID(t, anon.addr, "", nil)
	defer anonConn.Close()
	anonSeen := false
	for _, topic := range drainTopics(t, anonConn, 2*time.Second) {
		if topic == "$SYS/session_identifier" {
			anonSeen = true
		}
	}
	if !anonSeen {
		t.Fatal("anonymous mode suppressed $SYS/session_identifier")
	}
}

func waitForStats(t *testing.T, h *Hrotti, cond func(StatsSnapshot) bool) StatsSnapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var s StatsSnapshot
	for time.Now().Before(deadline) {
		s = h.Stats()
		if cond(s) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic(fmt.Sprintf("stats condition not met within 3s, last snapshot: %+v", s))
}
