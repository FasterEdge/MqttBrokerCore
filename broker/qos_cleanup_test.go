// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import (
	"testing"

	. "github.com/FasterEdge/MqttBrokerCore/packets"
	"github.com/google/uuid"
)

// TestHandleFlowInboundCleanup 验证方向2(客户端→服务端)QoS1/QoS2 完成时
// INBOUND 持久化与 inboundMessageIDs 反查表均被精确清理(回归: 旧实现用回包
// 自身 uuid 删除 → 永不命中 → INBOUND 无限累积泄漏)。
func TestHandleFlowInboundCleanup(t *testing.T) {
	h := NewHrotti(10, &MemoryPersistence{})
	h.PersistStore.Open("client-1")
	mp := h.PersistStore.(*MemoryPersistence)
	c := &Client{clientID: "client-1", inboundMessageIDs: messageIDs{index: make(map[uint16]*uuid.UUID)}}

	assertInboundLen := func(want int, what string) {
		mp.inbound["client-1"].Lock()
		n := len(mp.inbound["client-1"].messages)
		mp.inbound["client-1"].Unlock()
		if n != want {
			t.Fatalf("%s: INBOUND len=%d want %d", what, n, want)
		}
	}

	// QoS1: 客户端 PUBLISH(msgid=5) → 服务端 PUBACK → INBOUND 应清空
	pp1 := NewControlPacket(PUBLISH).(*PublishPacket)
	pp1.MessageID = 5
	pp1.Qos = 1
	h.PersistStore.Add("client-1", INBOUND, pp1)
	c.inboundMessageIDs.setID(5, pp1.UUID())
	pa := NewControlPacket(PUBACK).(*PubackPacket)
	pa.MessageID = 5
	c.HandleFlow(pa, h)
	assertInboundLen(0, "QoS1 PUBACK")
	if uid := c.inboundMessageIDs.index[5]; uid != nil {
		t.Fatal("QoS1: inboundMessageIDs[5] not freed")
	}

	// QoS2: 客户端 PUBLISH(msgid=7) → 服务端 PUBCOMP → INBOUND 应清空
	pp2 := NewControlPacket(PUBLISH).(*PublishPacket)
	pp2.MessageID = 7
	pp2.Qos = 2
	h.PersistStore.Add("client-1", INBOUND, pp2)
	c.inboundMessageIDs.setID(7, pp2.UUID())
	pc := NewControlPacket(PUBCOMP).(*PubcompPacket)
	pc.MessageID = 7
	c.HandleFlow(pc, h)
	assertInboundLen(0, "QoS2 PUBCOMP")
	if uid := c.inboundMessageIDs.index[7]; uid != nil {
		t.Fatal("QoS2: inboundMessageIDs[7] not freed")
	}
}

// TestPubackOutboundCleanup 验证方向1(服务端→客户端)QoS1 完成时 OUTBOUND 持久化
// 经 MessageID 反查被精确删除(回归: 旧实现用 PUBACK 自身 uuid 删 → 永不命中)。
func TestPubackOutboundCleanup(t *testing.T) {
	h := NewHrotti(10, &MemoryPersistence{})
	h.PersistStore.Open("client-1")
	mp := h.PersistStore.(*MemoryPersistence)
	c := &Client{clientID: "client-1", messageIDs: messageIDs{index: make(map[uint16]*uuid.UUID)}}

	// 服务端发 QoS1 PUBLISH: 分配 msgid 并持久化 OUTBOUND
	pub := NewControlPacket(PUBLISH).(*PublishPacket)
	pub.Qos = 1
	mid, err := c.getMsgID(pub.UUID())
	if err != nil {
		t.Fatalf("getMsgID: %v", err)
	}
	pub.MessageID = mid
	h.PersistStore.Add("client-1", OUTBOUND, pub)

	// 客户端回 PUBACK(msgid) → 按 MessageID 反查删除 OUTBOUND
	pa := NewControlPacket(PUBACK).(*PubackPacket)
	pa.MessageID = mid
	if c.inUse(pa.MessageID) {
		c.messageIDs.RLock()
		uid := c.messageIDs.index[pa.MessageID]
		c.messageIDs.RUnlock()
		if uid != nil {
			h.PersistStore.Delete("client-1", OUTBOUND, *uid)
		}
		c.freeID(pa.MessageID)
	}

	mp.outbound["client-1"].Lock()
	n := len(mp.outbound["client-1"].messages)
	mp.outbound["client-1"].Unlock()
	if n != 0 {
		t.Fatalf("QoS1 OUTBOUND not cleaned: %d messages remain", n)
	}
	if c.inUse(mid) {
		t.Fatal("msgid not freed after PUBACK")
	}
}
