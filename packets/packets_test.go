package packets

import (
	"bytes"
	"testing"
)

func TestPacketNames(t *testing.T) {
	if PacketNames[1] != "CONNECT" {
		t.Errorf("PacketNames[1] is %s, should be %s", PacketNames[1], "CONNECT")
	}
	if PacketNames[2] != "CONNACK" {
		t.Errorf("PacketNames[2] is %s, should be %s", PacketNames[2], "CONNACK")
	}
	if PacketNames[3] != "PUBLISH" {
		t.Errorf("PacketNames[3] is %s, should be %s", PacketNames[3], "PUBLISH")
	}
	if PacketNames[4] != "PUBACK" {
		t.Errorf("PacketNames[4] is %s, should be %s", PacketNames[4], "PUBACK")
	}
	if PacketNames[5] != "PUBREC" {
		t.Errorf("PacketNames[5] is %s, should be %s", PacketNames[5], "PUBREC")
	}
	if PacketNames[6] != "PUBREL" {
		t.Errorf("PacketNames[6] is %s, should be %s", PacketNames[6], "PUBREL")
	}
	if PacketNames[7] != "PUBCOMP" {
		t.Errorf("PacketNames[7] is %s, should be %s", PacketNames[7], "PUBCOMP")
	}
	if PacketNames[8] != "SUBSCRIBE" {
		t.Errorf("PacketNames[8] is %s, should be %s", PacketNames[8], "SUBSCRIBE")
	}
	if PacketNames[9] != "SUBACK" {
		t.Errorf("PacketNames[9] is %s, should be %s", PacketNames[9], "SUBACK")
	}
	if PacketNames[10] != "UNSUBSCRIBE" {
		t.Errorf("PacketNames[10] is %s, should be %s", PacketNames[10], "UNSUBSCRIBE")
	}
	if PacketNames[11] != "UNSUBACK" {
		t.Errorf("PacketNames[11] is %s, should be %s", PacketNames[11], "UNSUBACK")
	}
	if PacketNames[12] != "PINGREQ" {
		t.Errorf("PacketNames[12] is %s, should be %s", PacketNames[12], "PINGREQ")
	}
	if PacketNames[13] != "PINGRESP" {
		t.Errorf("PacketNames[13] is %s, should be %s", PacketNames[13], "PINGRESP")
	}
	if PacketNames[14] != "DISCONNECT" {
		t.Errorf("PacketNames[14] is %s, should be %s", PacketNames[14], "DISCONNECT")
	}
}

func TestPacketConsts(t *testing.T) {
	if CONNECT != 1 {
		t.Errorf("Const for CONNECT is %d, should be %d", CONNECT, 1)
	}
	if CONNACK != 2 {
		t.Errorf("Const for CONNACK is %d, should be %d", CONNACK, 2)
	}
	if PUBLISH != 3 {
		t.Errorf("Const for PUBLISH is %d, should be %d", PUBLISH, 3)
	}
	if PUBACK != 4 {
		t.Errorf("Const for PUBACK is %d, should be %d", PUBACK, 4)
	}
	if PUBREC != 5 {
		t.Errorf("Const for PUBREC is %d, should be %d", PUBREC, 5)
	}
	if PUBREL != 6 {
		t.Errorf("Const for PUBREL is %d, should be %d", PUBREL, 6)
	}
	if PUBCOMP != 7 {
		t.Errorf("Const for PUBCOMP is %d, should be %d", PUBCOMP, 7)
	}
	if SUBSCRIBE != 8 {
		t.Errorf("Const for SUBSCRIBE is %d, should be %d", SUBSCRIBE, 8)
	}
	if SUBACK != 9 {
		t.Errorf("Const for SUBACK is %d, should be %d", SUBACK, 9)
	}
	if UNSUBSCRIBE != 10 {
		t.Errorf("Const for UNSUBSCRIBE is %d, should be %d", UNSUBSCRIBE, 10)
	}
	if UNSUBACK != 11 {
		t.Errorf("Const for UNSUBACK is %d, should be %d", UNSUBACK, 11)
	}
	if PINGREQ != 12 {
		t.Errorf("Const for PINGREQ is %d, should be %d", PINGREQ, 12)
	}
	if PINGRESP != 13 {
		t.Errorf("Const for PINGRESP is %d, should be %d", PINGRESP, 13)
	}
	if DISCONNECT != 14 {
		t.Errorf("Const for DISCONNECT is %d, should be %d", DISCONNECT, 14)
	}
}

func TestConnackConsts(t *testing.T) {
	if CONN_ACCEPTED != 0x00 {
		t.Errorf("Const for CONN_ACCEPTED is %d, should be %d", CONN_ACCEPTED, 0)
	}
	if CONN_REF_BAD_PROTO_VER != 0x01 {
		t.Errorf("Const for CONN_REF_BAD_PROTO_VER is %d, should be %d", CONN_REF_BAD_PROTO_VER, 1)
	}
	if CONN_REF_ID_REJ != 0x02 {
		t.Errorf("Const for CONN_REF_ID_REJ is %d, should be %d", CONN_REF_ID_REJ, 2)
	}
	if CONN_REF_SERV_UNAVAIL != 0x03 {
		t.Errorf("Const for CONN_REF_SERV_UNAVAIL is %d, should be %d", CONN_REF_SERV_UNAVAIL, 3)
	}
	if CONN_REF_BAD_USER_PASS != 0x04 {
		t.Errorf("Const for CONN_REF_BAD_USER_PASS is %d, should be %d", CONN_REF_BAD_USER_PASS, 4)
	}
	if CONN_REF_NOT_AUTH != 0x05 {
		t.Errorf("Const for CONN_REF_NOT_AUTH is %d, should be %d", CONN_REF_NOT_AUTH, 5)
	}
}

func TestConnectPacket(t *testing.T) {
	connectPacketBytes := bytes.NewBuffer([]byte{16, 52, 0, 4, 77, 81, 84, 84, 4, 204, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 12, 84, 101, 115, 116, 32, 80, 97, 121, 108, 111, 97, 100, 0, 8, 116, 101, 115, 116, 117, 115, 101, 114, 0, 8, 116, 101, 115, 116, 112, 97, 115, 115})
	packet, err := ReadPacket(connectPacketBytes)
	if err != nil {
		t.Fatalf("Error reading packet: %s", err.Error())
	}
	cp := packet.(*ConnectPacket)
	if cp.ProtocolName != "MQTT" {
		t.Errorf("Connect Packet ProtocolName is %s, should be %s", cp.ProtocolName, "MQTT")
	}
	if cp.ProtocolVersion != 4 {
		t.Errorf("Connect Packet ProtocolVersion is %d, should be %d", cp.ProtocolVersion, 4)
	}
	if cp.UsernameFlag != true {
		t.Errorf("Connect Packet UsernameFlag is %t, should be %t", cp.UsernameFlag, true)
	}
	if cp.Username != "testuser" {
		t.Errorf("Connect Packet Username is %s, should be %s", cp.Username, "testuser")
	}
	if cp.PasswordFlag != true {
		t.Errorf("Connect Packet PasswordFlag is %t, should be %t", cp.PasswordFlag, true)
	}
	if string(cp.Password) != "testpass" {
		t.Errorf("Connect Packet Password is %s, should be %s", string(cp.Password), "testpass")
	}
	if cp.WillFlag != true {
		t.Errorf("Connect Packet WillFlag is %t, should be %t", cp.WillFlag, true)
	}
	if cp.WillTopic != "test" {
		t.Errorf("Connect Packet WillTopic is %s, should be %s", cp.WillTopic, "test")
	}
	if cp.WillQos != 1 {
		t.Errorf("Connect Packet WillQos is %d, should be %d", cp.WillQos, 1)
	}
	if cp.WillRetain != false {
		t.Errorf("Connect Packet WillRetain is %t, should be %t", cp.WillRetain, false)
	}
	if string(cp.WillMessage) != "Test Payload" {
		t.Errorf("Connect Packet WillMessage is %s, should be %s", string(cp.WillMessage), "Test Payload")
	}
}

func TestConnectValidateClientID(t *testing.T) {
	mk := func(id string) *ConnectPacket {
		return &ConnectPacket{ProtocolName: "MQTT", ProtocolVersion: 4, ClientIdentifier: id}
	}
	if rc := mk("client-1").Validate(); rc != CONN_ACCEPTED {
		t.Fatalf("valid client id rejected: rc=%d", rc)
	}
	// 控制字符(换行/ANSI 转义)可注入日志行——须拒绝。
	if rc := mk("client\nid").Validate(); rc != CONN_REF_ID_REJ {
		t.Fatalf("newline client id accepted: rc=%d", rc)
	}
	if rc := mk("client\x1b[31m").Validate(); rc != CONN_REF_ID_REJ {
		t.Fatalf("ansi client id accepted: rc=%d", rc)
	}
	// 超长 client id 放大日志/持久化/订阅表——须拒绝(>128)。
	long := string(bytes.Repeat([]byte("a"), 129))
	if rc := mk(long).Validate(); rc != CONN_REF_ID_REJ {
		t.Fatalf("overlong client id accepted: rc=%d", rc)
	}
	// 空 ID 在 packets 层放行,由 server 按 MQTT 3.1.1 §3.1.3.1 处理。
	if rc := mk("").Validate(); rc != CONN_ACCEPTED {
		t.Fatalf("empty client id rejected at packets layer: rc=%d", rc)
	}
}

func TestConnectValidateWillTopic(t *testing.T) {
	mk := func(topic string) *ConnectPacket {
		return &ConnectPacket{ProtocolName: "MQTT", ProtocolVersion: 4, ClientIdentifier: "c", WillFlag: true, WillTopic: topic}
	}
	if rc := mk("a/b").Validate(); rc != CONN_ACCEPTED {
		t.Fatalf("valid will topic rejected: rc=%d", rc)
	}
	// 主题名禁含通配符(与 PUBLISH 2529 同源)
	if rc := mk("a/#").Validate(); rc != CONN_PROTOCOL_VIOLATION {
		t.Fatalf("wildcard will topic accepted: rc=%d", rc)
	}
	if rc := mk("a/+").Validate(); rc != CONN_PROTOCOL_VIOLATION {
		t.Fatalf("plus will topic accepted: rc=%d", rc)
	}
	// WillFlag=1 时 WillTopic 非空
	if rc := mk("").Validate(); rc != CONN_PROTOCOL_VIOLATION {
		t.Fatalf("empty will topic accepted: rc=%d", rc)
	}
	// 空字符
	if rc := mk("a\x00b").Validate(); rc != CONN_PROTOCOL_VIOLATION {
		t.Fatalf("NUL will topic accepted: rc=%d", rc)
	}
}

func TestIsValidTopicName(t *testing.T) {
	for _, ok := range []string{"a/b", "a//b", "$SYS/broker", "sensor 1/x"} {
		if !IsValidTopicName(ok) {
			t.Fatalf("valid topic name %q rejected", ok)
		}
	}
	for _, bad := range []string{"", "#", "+", "a/#", "a/+", "a#", "a+b", "a\x00b"} {
		if IsValidTopicName(bad) {
			t.Fatalf("invalid topic name %q accepted", bad)
		}
	}
	// 段数上限: ≤31 段通过, 32 段拒绝(超出会在订阅 bitmap 越界 panic)
	if !IsValidTopicName(string(bytes.Repeat([]byte("a/"), 30)) + "a") {
		t.Fatal("31-segment topic rejected")
	}
	if IsValidTopicName(string(bytes.Repeat([]byte("a/"), 31)) + "a") {
		t.Fatal("32-segment topic accepted")
	}
}

func TestUnsubscribePacketUnpack(t *testing.T) {
	// 单 topic: UNSUBSCRIBE(10) flags=0x02(QoS1), RL=7, msgid=1, "a/b"
	single := bytes.NewBuffer([]byte{0xA2, 7, 0, 1, 0, 3, 'a', '/', 'b'})
	sp, err := ReadPacket(single)
	if err != nil {
		t.Fatalf("single-topic UNSUBSCRIBE rejected: %v", err)
	}
	if up := sp.(*UnsubscribePacket); len(up.Topics) != 1 || up.Topics[0] != "a/b" {
		t.Fatalf("single topics = %v", up.Topics)
	}
	// 双 topic: RL=12, "a/b" + "c/d"(MQTT 3.1.1 §3.10.3 允许多个 filter)
	double := bytes.NewBuffer([]byte{0xA2, 12, 0, 1, 0, 3, 'a', '/', 'b', 0, 3, 'c', '/', 'd'})
	dp, err := ReadPacket(double)
	if err != nil {
		t.Fatalf("double-topic UNSUBSCRIBE rejected: %v", err)
	}
	if up := dp.(*UnsubscribePacket); len(up.Topics) != 2 || up.Topics[0] != "a/b" || up.Topics[1] != "c/d" {
		t.Fatalf("double topics = %v", up.Topics)
	}
}
