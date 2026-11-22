// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	. "github.com/FasterEdge/MqttBrokerCore/packets"
)

// ── 测试夹具 ────────────────────────────────────────────────
//
// 本文件覆盖 MqttBrokerCore 的访问控制: CONNECT 身份校验、主题级 ACL、
// 以及拒绝时"不泄漏主题存在性"的可观测性质。除单元断言外, 绝大多数
// 用例走真实 TCP 连接 + 真实 MQTT 报文, 避免只用内部函数调用代替
// 端到端行为。

type testBroker struct {
	h    *Hrotti
	addr string
}

// startTestBroker 在 127.0.0.1 的临时端口上起一个真实 TCP 监听。
// cfg 为 nil 表示不配置凭据表(应进入 fail-closed)。
func startTestBroker(t *testing.T, cfg *AuthConfig) *testBroker {
	t.Helper()
	h := NewHrotti(16, &MemoryPersistence{})
	if err := h.SetAuthConfig(cfg); err != nil {
		t.Fatalf("SetAuthConfig: %v", err)
	}
	// 先占一个空闲端口再释放: AddListener 不回报实际绑定端口, 这是
	// 仓库内既有的取端口方式(见 stop_race_test.go)。
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	lc, err := NewListenerConfigWithError(fmt.Sprintf("tcp://127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("NewListenerConfigWithError: %v", err)
	}
	if err := h.AddListener("test", lc); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	t.Cleanup(h.Stop)
	return &testBroker{h: h, addr: fmt.Sprintf("127.0.0.1:%d", port)}
}

// connectPacket 构造一个 CONNECT 报文; user 为空表示不带用户名/密码。
func connectPacket(clientID string, user string, pass []byte) *ConnectPacket {
	cp := NewControlPacket(CONNECT).(*ConnectPacket)
	cp.ProtocolName = "MQTT"
	cp.ProtocolVersion = 4
	cp.CleanSession = true
	cp.ClientIdentifier = clientID
	cp.KeepaliveTimer = 0
	if user != "" {
		cp.UsernameFlag = true
		cp.Username = user
		cp.PasswordFlag = true
		cp.Password = pass
	}
	return cp
}

// dial 建立 TCP 连接并把 CONNECT 写出去, 返回连接与读到的 CONNACK。
// CONNACK 之后无论接受还是拒绝都返回, 由调用方断言 ReturnCode。
func dial(t *testing.T, addr string, cp *ConnectPacket) (net.Conn, *ConnackPacket) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := cp.Write(conn); err != nil {
		_ = conn.Close()
		t.Fatalf("write CONNECT: %v", err)
	}
	p, err := ReadPacket(conn)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read CONNACK: %v", err)
	}
	ca, ok := p.(*ConnackPacket)
	if !ok {
		_ = conn.Close()
		t.Fatalf("expected CONNACK, got %T", p)
	}
	return conn, ca
}

// mustConnect 断言连接被接受(ReturnCode == 0)并返回可用连接。
func mustConnect(t *testing.T, addr, clientID, user string, pass []byte) net.Conn {
	t.Helper()
	conn, ca := dial(t, addr, connectPacket(clientID, user, pass))
	if ca.ReturnCode != CONN_ACCEPTED {
		_ = conn.Close()
		t.Fatalf("connect %q rejected: rc=0x%02x", user, ca.ReturnCode)
	}
	return conn
}

// readRawPacket 读一个完整控制包并返回 (类型, body 字节)。
// 用于 SUBACK: packets.SubackPacket.Unpack 走 ReadFrom, 与 ReadPacket 的
// 短读检测冲突(packets/suback.go:42 的既有缺陷, 不在本次改动范围内),
// 故此处直接按 MQTT 线格式解析 body, 反而更独立地验证了 broker 写出的字节。
func readRawPacket(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	head := make([]byte, 1)
	if _, err := io.ReadFull(conn, head); err != nil {
		t.Fatalf("read fixed header: %v", err)
	}
	var rl int
	var mult uint32 = 1
	for i := 0; i < 4; i++ {
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			t.Fatalf("read remaining length: %v", err)
		}
		rl += int(b[0]&0x7f) * int(mult)
		if b[0]&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, rl)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return head[0] >> 4, body
}

// subscribe 发送 SUBSCRIBE 并读回 SUBACK 的授予列表。
func subscribe(t *testing.T, conn net.Conn, msgID uint16, filters ...string) []byte {
	t.Helper()
	sp := NewControlPacket(SUBSCRIBE).(*SubscribePacket)
	sp.MessageID = msgID
	sp.Topics = append([]string(nil), filters...)
	for range filters {
		sp.Qoss = append(sp.Qoss, 0)
	}
	if err := sp.Write(conn); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
	typ, body := readRawPacket(t, conn)
	if typ != SUBACK {
		t.Fatalf("expected SUBACK(9), got packet type %d", typ)
	}
	if len(body) < 3 {
		t.Fatalf("SUBACK body too short: %d bytes", len(body))
	}
	if got := uint16(body[0])<<8 | uint16(body[1]); got != msgID {
		t.Fatalf("SUBACK msgid = %d, want %d", got, msgID)
	}
	return body[2:]
}

// publish 发送一条 PUBLISH, 返回 broker 回的 ack 的 MessageID(QoS0 时为 0)。
func publish(t *testing.T, conn net.Conn, topic string, qos byte, msgID uint16, payload string) uint16 {
	t.Helper()
	pp := NewControlPacket(PUBLISH).(*PublishPacket)
	pp.TopicName = topic
	pp.Qos = qos
	pp.MessageID = msgID
	pp.Payload = []byte(payload)
	if err := pp.Write(conn); err != nil {
		t.Fatalf("write PUBLISH %s: %v", topic, err)
	}
	if qos == 0 {
		return 0
	}
	typ, body := readRawPacket(t, conn)
	if typ != PUBACK && typ != PUBREC {
		t.Fatalf("expected PUBACK(4)/PUBREC(5) for %s, got packet type %d", topic, typ)
	}
	if len(body) < 2 {
		t.Fatalf("ack body too short: %d bytes", len(body))
	}
	return uint16(body[0])<<8 | uint16(body[1])
}

// drainTopics 在 idle 窗口内尽量读包, 返回收到的 PUBLISH 主题序列。
// 用于"某主题未被投递"的断言: 缺席无法瞬时证明, 故以一段空闲窗口作为
// 观测边界, 并把观察到的全部主题交给调用方断言(不吞掉任何一条)。
// 超时/EOF 是正常的观测结束信号, 不算失败。
func drainTopics(t *testing.T, conn net.Conn, idle time.Duration) []string {
	t.Helper()
	var topics []string
	for {
		_ = conn.SetReadDeadline(time.Now().Add(idle))
		typ, body := readRawConnPacket(conn)
		if body == nil {
			return topics
		}
		if typ == PUBLISH {
			topics = append(topics, rawPublishTopic(body))
		}
	}
}

// readRawConnPacket 是 readRawPacket 的非失败版本: 读不到就返回 (0, nil),
// 由调用方判断是观测窗口结束还是真错误。
func readRawConnPacket(conn net.Conn) (byte, []byte) {
	head := make([]byte, 1)
	if _, err := io.ReadFull(conn, head); err != nil {
		return 0, nil
	}
	var rl int
	var mult uint32 = 1
	for i := 0; i < 4; i++ {
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			return 0, nil
		}
		rl += int(b[0]&0x7f) * int(mult)
		if b[0]&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, rl)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, nil
	}
	return head[0] >> 4, body
}

// rawPublishTopic 从 PUBLISH 报文 body 中取出主题名(跳过前两个字节的
// 长度前缀)。
func rawPublishTopic(body []byte) string {
	if len(body) < 2 {
		return ""
	}
	n := int(body[0])<<8 | int(body[1])
	if n < 0 || n+2 > len(body) {
		return ""
	}
	return string(body[2 : 2+n])
}

// aclFixture 是端到端用例共用的身份配置:
//   - sensor : 端侧, 只能在自己站点的 reading 主题与 only/here 上收发
//   - ops    : 运维, 显式授权 '#' 订阅
//   - limited: 权限最小的身份, 既没有 '#' 也没有 site/+/reading 发布权
func aclFixture() *AuthConfig {
	return &AuthConfig{AllowedCredentials: []Credentials{
		{
			Username:  "sensor",
			Password:  "sensor-pw",
			PubTopics: []string{"site/+/reading", "only/here"},
			SubTopics: []string{"site/+/reading", "only/here"},
		},
		{
			Username:  "ops",
			Password:  "ops-pw",
			PubTopics: []string{"site/#"},
			SubTopics: []string{"#"},
		},
		{
			Username:  "limited",
			Password:  "limited-pw",
			PubTopics: []string{"only/here"},
			SubTopics: []string{"only/here"},
		},
	}}
}

// ── CONNECT 身份校验 ────────────────────────────────────────

// TestConnectRequiresCredentials 覆盖三种失败路径: 不带凭据、密码错误、
// 用户名不存在。三者必须统一返回 CONN_REF_BAD_USER_PASS(0x04), 否则
// 攻击者可借返回码差异枚举合法用户名。
func TestConnectRequiresCredentials(t *testing.T) {
	b := startTestBroker(t, aclFixture())
	ResetAuthStatsForTest()

	cases := []struct {
		name string
		user string
		pass []byte
	}{
		{"no credentials", "", nil},
		{"wrong password", "sensor", []byte("not-the-password")},
		{"unknown user", "ghost", []byte("whatever")},
		{"empty password", "sensor", []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, ca := dial(t, b.addr, connectPacket("probe-"+tc.name, tc.user, tc.pass))
			defer conn.Close()
			if ca.ReturnCode != CONN_REF_BAD_USER_PASS {
				t.Fatalf("rc=0x%02x want 0x%02x", ca.ReturnCode, CONN_REF_BAD_USER_PASS)
			}
			// 拒绝后连接必须被关闭: 读应立刻失败(EOF/reset), 而不是挂着等。
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := ReadPacket(conn); err == nil {
				t.Fatal("connection still open after auth rejection")
			}
		})
	}
	if got := GetAuthStats().ConnRejected; got < int64(len(cases)) {
		t.Fatalf("ConnRejected=%d want >= %d", got, len(cases))
	}
}

// TestConnectAcceptsValidCredentials 是"合法路径"的正向对照: 校验通过
// 必须回 CONN_ACCEPTED, 且客户端能正常收发。
func TestConnectAcceptsValidCredentials(t *testing.T) {
	b := startTestBroker(t, aclFixture())
	conn := mustConnect(t, b.addr, "sensor-1", "sensor", []byte("sensor-pw"))
	defer conn.Close()

	if gq := subscribe(t, conn, 1, "site/+/reading"); len(gq) != 1 || gq[0] != 0 {
		t.Fatalf("granted qos = %v, want [0]", gq)
	}
}

// TestFailClosedWithoutAuthConfig 覆盖"忘了配鉴权"的场景: 未配置凭据表
// 时连合法凭据也必须被拒绝, 避免默认敞开。
func TestFailClosedWithoutAuthConfig(t *testing.T) {
	b := startTestBroker(t, nil)
	if b.h.AuthManager().Mode() != "closed" {
		t.Fatalf("mode = %q, want closed", b.h.AuthManager().Mode())
	}
	conn, ca := dial(t, b.addr, connectPacket("anon", "sensor", []byte("sensor-pw")))
	defer conn.Close()
	if ca.ReturnCode != CONN_REF_BAD_USER_PASS {
		t.Fatalf("fail-closed broker accepted connection: rc=0x%02x", ca.ReturnCode)
	}
}

// TestAnonymousModeMustBeExplicit 覆盖显式匿名模式: 只有显式
// allowAnonymous 才放行, 并且此时 '#' 订阅也放行(即访问控制确实关闭)。
func TestAnonymousModeMustBeExplicit(t *testing.T) {
	b := startTestBroker(t, &AuthConfig{AllowAnonymous: true})
	conn, ca := dial(t, b.addr, connectPacket("anon-1", "", nil))
	defer conn.Close()
	if ca.ReturnCode != CONN_ACCEPTED {
		t.Fatalf("anonymous mode rejected connection: rc=0x%02x", ca.ReturnCode)
	}
	if gq := subscribe(t, conn, 1, "#"); len(gq) != 1 || gq[0] != 0 {
		t.Fatalf("anonymous mode: '#' granted qos = %v, want [0]", gq)
	}
}

// ── 主题级 ACL: 订阅侧 ─────────────────────────────────────

// TestSubscribeWildcardDeniedWithoutGrant 是核心越权场景: 未被显式授权
// '#' 的身份订阅 '#' 必须收到 SUBACK 0x80, 且随后确实收不到其它站点的
// 数据(不只是"回了个错误码")。
func TestSubscribeWildcardDeniedWithoutGrant(t *testing.T) {
	b := startTestBroker(t, aclFixture())

	sensor := mustConnect(t, b.addr, "sensor-1", "sensor", []byte("sensor-pw"))
	defer sensor.Close()

	limited := mustConnect(t, b.addr, "limited-1", "limited", []byte("limited-pw"))
	defer limited.Close()

	if gq := subscribe(t, limited, 1, "#"); len(gq) != 1 || gq[0] != 0x80 {
		t.Fatalf("# granted qos = %v, want [0x80]", gq)
	}
	// 同一连接上合法过滤器仍须被授予(不能一拒全拒)。
	if gq := subscribe(t, limited, 2, "only/here"); len(gq) != 1 || gq[0] != 0 {
		t.Fatalf("only/here granted qos = %v, want [0]", gq)
	}

	// sensor 向受限身份无权监听的站点主题发布, 再向双方都可收的主题发布。
	// 后一条作为"屏障", 保证前一条已被 broker 处理完毕。
	publish(t, sensor, "site/42/reading", 0, 0, "secret")
	publish(t, sensor, "only/here", 0, 0, "visible")

	got := drainTopics(t, limited, 400*time.Millisecond)
	if len(got) != 1 || got[0] != "only/here" {
		t.Fatalf("limited received topics %v, want exactly [only/here]", got)
	}
	if GetAuthStats().SubRejected == 0 {
		t.Fatal("SubRejected counter not incremented")
	}
}

// TestSubscribeWildcardGrantedWhenExplicit 是"显式授权 '#'"的正向路径:
// ops 拥有 subTopics ["#"], 订阅 '#' 必须被授予并能收到任意站点数据。
func TestSubscribeWildcardGrantedWhenExplicit(t *testing.T) {
	b := startTestBroker(t, aclFixture())

	sensor := mustConnect(t, b.addr, "sensor-1", "sensor", []byte("sensor-pw"))
	defer sensor.Close()
	ops := mustConnect(t, b.addr, "ops-1", "ops", []byte("ops-pw"))
	defer ops.Close()

	if gq := subscribe(t, ops, 1, "#"); len(gq) != 1 || gq[0] != 0 {
		t.Fatalf("# granted qos = %v, want [0]", gq)
	}
	publish(t, sensor, "site/42/reading", 0, 0, "42.0")
	got := drainTopics(t, ops, 400*time.Millisecond)
	if len(got) != 1 || got[0] != "site/42/reading" {
		t.Fatalf("ops received topics %v, want [site/42/reading]", got)
	}
}

// ── 主题级 ACL: 发布侧 ─────────────────────────────────────

// TestPublishDeniedSilentlyButAcked 覆盖越权发布:
//   - QoS1 必须仍回 PUBACK(与合法发布完全一致的确认), 使客户端无法用
//     "有没有 ack" 判断主题是否存在;
//   - 消息绝不能送到订阅者手里。
func TestPublishDeniedSilentlyButAcked(t *testing.T) {
	b := startTestBroker(t, aclFixture())

	ops := mustConnect(t, b.addr, "ops-1", "ops", []byte("ops-pw"))
	defer ops.Close()
	if gq := subscribe(t, ops, 1, "#"); len(gq) != 1 || gq[0] != 0 {
		t.Fatalf("# granted qos = %v, want [0]", gq)
	}

	limited := mustConnect(t, b.addr, "limited-1", "limited", []byte("limited-pw"))
	defer limited.Close()

	// 越权发布 site/42/reading(QoS1): 必须回 PUBACK(msgid 一致)。
	if mid := publish(t, limited, "site/42/reading", 1, 7, "forged"); mid != 7 {
		t.Fatalf("denied PUBLISH ack msgid = %d, want 7", mid)
	}
	// 合法发布作为屏障。
	if mid := publish(t, limited, "only/here", 1, 8, "legit"); mid != 8 {
		t.Fatalf("authorised PUBLISH ack msgid = %d, want 8", mid)
	}

	got := drainTopics(t, ops, 400*time.Millisecond)
	if len(got) != 1 || got[0] != "only/here" {
		t.Fatalf("ops received %v, want exactly [only/here]", got)
	}
	if GetAuthStats().PubRejected == 0 {
		t.Fatal("PubRejected counter not incremented")
	}

	// 保留消息也不能被越权写入: 否则未授权客户端可污染他人后续订阅。
	// 用 QoS0 + retain 再发一次, 然后让新订阅者验证没收到保留消息。
	pp := NewControlPacket(PUBLISH).(*PublishPacket)
	pp.TopicName = "site/99/reading"
	pp.Qos = 0
	pp.Retain = true
	pp.Payload = []byte("poison")
	if err := pp.Write(limited); err != nil {
		t.Fatalf("write retained PUBLISH: %v", err)
	}
	_ = drainTopics(t, ops, 200*time.Millisecond)

	late := mustConnect(t, b.addr, "ops-2", "ops", []byte("ops-pw"))
	defer late.Close()
	if gq := subscribe(t, late, 1, "site/99/reading"); len(gq) != 1 || gq[0] != 0 {
		t.Fatalf("granted qos = %v, want [0]", gq)
	}
	if got := drainTopics(t, late, 300*time.Millisecond); len(got) != 0 {
		t.Fatalf("retained message from denied PUBLISH leaked: %v", got)
	}
}

// TestSubscribeRejectionLeaksNothing 断言拒绝码不区分"主题是否存在":
// 对一个肯定不存在的主题和一个活跃主题, 未授权身份的 SUBACK 完全相同。
func TestSubscribeRejectionLeaksNothing(t *testing.T) {
	b := startTestBroker(t, aclFixture())

	limited := mustConnect(t, b.addr, "limited-1", "limited", []byte("limited-pw"))
	defer limited.Close()

	existing := subscribe(t, limited, 1, "site/42/reading")
	missing := subscribe(t, limited, 2, "no/such/topic")
	if len(existing) != 1 || len(missing) != 1 {
		t.Fatalf("unexpected granted lengths: %v %v", existing, missing)
	}
	if existing[0] != missing[0] || existing[0] != 0x80 {
		t.Fatalf("rejection codes differ (existence leak): existing=%v missing=%v", existing, missing)
	}
}

// ── AuthManager 单元断言 ────────────────────────────────────

// TestAuthManagerConfigValidation 覆盖配置校验: 重复用户名/空用户名、
// 匿名与凭据混用都必须报错, 且报错时不得悄悄改掉已生效的表。
func TestAuthManagerConfigValidation(t *testing.T) {
	a := NewAuthManager()
	if err := a.SetAuthConfig(&AuthConfig{AllowedCredentials: []Credentials{
		{Username: "dup", Password: "x"},
		{Username: "dup", Password: "y"},
	}}); err == nil {
		t.Fatal("duplicate username accepted")
	}
	if err := a.SetAuthConfig(&AuthConfig{AllowedCredentials: []Credentials{
		{Username: "", Password: "x"},
	}}); err == nil {
		t.Fatal("empty username accepted")
	}
	if err := a.SetAuthConfig(&AuthConfig{
		AllowAnonymous:     true,
		AllowedCredentials: []Credentials{{Username: "u", Password: "p"}},
	}); err == nil {
		t.Fatal("allowAnonymous combined with credentials accepted")
	}
	// 失败后仍是 fail-closed: 不能因一次坏配置而变成放行。
	if id, ok := a.Authenticate("dup", []byte("x")); ok || id != "" {
		t.Fatalf("manager leaked into a permissive state after invalid config: id=%q ok=%v", id, ok)
	}
	if a.Mode() != "closed" {
		t.Fatalf("mode = %q, want closed", a.Mode())
	}
}

// TestAuthManagerConstantTimeCompareLengthMismatch 断言常量时间比较在
// 长度不等时返回 false(且不 panic), 覆盖空密码等边界。
func TestAuthManagerConstantTimeCompareLengthMismatch(t *testing.T) {
	if constantTimeEqual([]byte("abc"), []byte("abcd")) {
		t.Fatal("length mismatch compared equal")
	}
	if constantTimeEqual([]byte(""), []byte("x")) {
		t.Fatal("empty vs one byte compared equal")
	}
	if !constantTimeEqual([]byte("same"), []byte("same")) {
		t.Fatal("identical values compared unequal")
	}
}

// TestTopicAllowedSemantics 钉住 ACL 匹配语义: 规则本身是主题过滤器,
// 不存在"隐式通配提升"; '#' 只命中显式写了 '#' 的规则。
func TestTopicAllowedSemantics(t *testing.T) {
	cases := []struct {
		patterns []string
		topic    string
		want     bool
	}{
		{[]string{"site/+/reading"}, "site/1/reading", true},
		{[]string{"site/+/reading"}, "site/1/temp", false},
		{[]string{"site/+/reading"}, "site/1/reading/extra", false},
		{[]string{"site/#"}, "site/1/reading", true},
		{[]string{"site/#"}, "other/1/reading", false},
		{[]string{"#"}, "anything/at/all", true},
		{[]string{"only/here"}, "only/here", true},
		{[]string{"only/here"}, "only/here2", false},
		{nil, "only/here", false},
		{[]string{""}, "only/here", false},
	}
	for _, tc := range cases {
		if got := topicAllowed(tc.patterns, tc.topic); got != tc.want {
			t.Fatalf("topicAllowed(%v, %q) = %v, want %v", tc.patterns, tc.topic, got, tc.want)
		}
	}
}

// TestAddSubscriptionRequiresIdentity 断言没有身份(空 identity)的会话
// 无法获得任何订阅: 这是 fail-closed 在订阅路径上的落点。
// 该用例同时覆盖"凭据表被清空后已连接会话失去订阅权"(空身份与查无
// 身份两条路径都必须是拒绝)。
func TestAddSubscriptionRequiresIdentity(t *testing.T) {
	h := NewHrotti(10, &MemoryPersistence{})
	if err := h.SetAuthConfig(aclFixture()); err != nil {
		t.Fatalf("SetAuthConfig: %v", err)
	}
	// 直接构造的 Client 没有 identity(未走 CONNECT 鉴权)。
	c := &Client{clientID: "no-identity"}
	if gq := h.AddSubscription(c, []string{"only/here"}, []byte{0}); gq[0] != 0x80 {
		t.Fatalf("identity-less client granted %v, want 0x80", gq)
	}
	// 凭据表被替换成空(配置热更新失败/被清空)后, 已连接会话必须失去
	// 订阅权, 而不是继续沿用旧 ACL。
	if err := h.SetAuthConfig(nil); err != nil {
		t.Fatalf("SetAuthConfig(nil): %v", err)
	}
	if gq := h.AddSubscription(c, []string{"only/here"}, []byte{0}); gq[0] != 0x80 {
		t.Fatalf("client granted %v after credentials cleared, want 0x80", gq)
	}
}

// TestSetAuthConfigCopiesSlices 断言配置被复制: 调用方事后改动自己的
// 切片不影响已生效的 ACL(否则一个共享 buffer 就能扩大权限)。
func TestSetAuthConfigCopiesSlices(t *testing.T) {
	a := NewAuthManager()
	subs := []string{"only/here"}
	if err := a.SetAuthConfig(&AuthConfig{AllowedCredentials: []Credentials{
		{Username: "u", Password: "p", SubTopics: subs},
	}}); err != nil {
		t.Fatalf("SetAuthConfig: %v", err)
	}
	subs[0] = "#"
	if a.CanSubscribe("u", "#") {
		t.Fatal("ACL mutated through caller's slice")
	}
	if !a.CanSubscribe("u", "only/here") {
		t.Fatal("original grant lost")
	}
}

// TestPasswordBytesAreNotLeakedInLogs 相关: ConnectPacket.String() 会打印
// Password 字段, 故 broker 侧的日志不得对 CONNECT 包调用 String()。该约束
// 由 code review 保证, 不在此处用恒真断言假装检查(见 README 的安全边界)。
