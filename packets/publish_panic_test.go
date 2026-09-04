package packets

import (
	"bytes"
	"testing"
)

// 回归: PublishPacket.Unpack 在 topic 长度声明超出实际剩余 body 时,
// payloadLength 为负, make([]byte, 负数) panic (远程 DoS)。
// 修复后不得 panic, 且 ReadPacket 应返回错误而非崩溃。
func TestPublishUnpackNegativePayloadNoPanic(t *testing.T) {
	cases := []struct {
		name string
		body []byte // 已含 RemainingLength 之后的完整 body
		qos  byte
	}{
		// QoS=0: topic 前缀声明 1 字节但 body 只有 2 字节前缀, 无 topic 数据
		{"qos0_short_topic", []byte{0x00, 0x01}, 0},
		// QoS=1: topic 前缀声明 1 字节, body 剩 1 字节 topic, msgid 缺失
		{"qos1_short_msgid", []byte{0x00, 0x01, 0x41}, 1},
		// topic 前缀声明巨大 (0xFFFF)
		{"huge_topic_decl", []byte{0xFF, 0xFF, 0x41, 0x42}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 构造完整帧: 固定头 (PUBLISH + qos) + 变长长度 + body
			var frame bytes.Buffer
			frame.WriteByte(0x30 | (tc.qos << 1)) // PUBLISH
			frame.Write(encodeLength(len(tc.body)))
			frame.Write(tc.body)

			// 核心断言: 不得 panic。ReadPacket 返回错误或包皆可,
			// 但绝不能崩溃 (旧实现 make([]byte, 负数) panic)。
			_, _ = ReadPacket(&frame)
		})
	}
}
