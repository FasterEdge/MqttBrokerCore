// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
package packets

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"io"
)

//PUBLISH packet

type PublishPacket struct {
	FixedHeader
	TopicName string
	MessageID uint16
	Payload   []byte
	uuid      uuid.UUID
}

func (p *PublishPacket) String() string {
	str := fmt.Sprintf("%s\n", p.FixedHeader)
	str += fmt.Sprintf("topicName: %s MessageID: %d\n", p.TopicName, p.MessageID)
	str += fmt.Sprintf("payload: %s\n", string(p.Payload))
	return str
}

func (p *PublishPacket) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(encodeString(p.TopicName))
	if p.Qos > 0 {
		body.Write(encodeUint16(p.MessageID))
	}
	p.FixedHeader.RemainingLength = body.Len() + len(p.Payload)
	packet := p.FixedHeader.pack()
	packet.Write(body.Bytes())
	packet.Write(p.Payload)
	_, err = w.Write(packet.Bytes())

	return err
}

func (p *PublishPacket) Unpack(b io.Reader) {
	var payloadLength = p.FixedHeader.RemainingLength
	p.TopicName = decodeString(b)
	if p.Qos > 0 {
		p.MessageID = decodeUint16(b)
		payloadLength -= len(p.TopicName) + 4
	} else {
		payloadLength -= len(p.TopicName) + 2
	}
	// 恶意/损坏帧可声明 topic 长度超出实际剩余 body: decodeString 返回全零
	// topic (len=N) 后 payloadLength 变负, make([]byte, 负数) 直接 panic,
	// 未认证客户端可借此远程打崩 broker。截断本身已由 decodeReader 记录,
	// ReadPacket 会以错误拒绝; 这里 clamp 到 0 防止 panic。
	if payloadLength < 0 {
		payloadLength = 0
	}
	p.Payload = make([]byte, payloadLength)
	b.Read(p.Payload)
}

func (p *PublishPacket) Copy() *PublishPacket {
	newP := NewControlPacket(PUBLISH).(*PublishPacket)
	newP.TopicName = p.TopicName
	newP.Payload = p.Payload

	return newP
}

func (p *PublishPacket) Details() Details {
	return Details{Qos: p.Qos, MessageID: p.MessageID}
}

func (p *PublishPacket) UUID() uuid.UUID {
	return p.uuid
}
