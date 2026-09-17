// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package packets

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"io"
)

//UNSUBSCRIBE packet

type UnsubscribePacket struct {
	FixedHeader
	MessageID uint16
	Topics    []string
	uuid      uuid.UUID
}

func (u *UnsubscribePacket) String() string {
	str := fmt.Sprintf("%s\n", u.FixedHeader)
	str += fmt.Sprintf("MessageID: %d", u.MessageID)
	return str
}

func (u *UnsubscribePacket) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(encodeUint16(u.MessageID))
	for _, topic := range u.Topics {
		body.Write(encodeString(topic))
	}
	u.FixedHeader.RemainingLength = body.Len()
	packet := u.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

func (u *UnsubscribePacket) Unpack(b io.Reader) {
	u.MessageID = decodeUint16(b)
	// 循环边界按 RemainingLength 递减(与 SubscribePacket.Unpack 一致):
	// MQTT 3.1.1 §3.10.3 payload 无空字符串结束符, 旧实现以 topic != "" 为界——
	// 正常单 topic 的 UNSUBSCRIBE 读超 body 被 decodeReader 记录 truncated,
	// ReadPacket 整体拒绝(功能损坏)。
	payloadLength := u.FixedHeader.RemainingLength - 2
	for payloadLength > 0 {
		topic := decodeString(b)
		u.Topics = append(u.Topics, topic)
		payloadLength -= 2 + len(topic)
	}
}

func (u *UnsubscribePacket) Details() Details {
	return Details{Qos: 1, MessageID: u.MessageID}
}

func (u *UnsubscribePacket) UUID() uuid.UUID {
	return u.uuid
}
