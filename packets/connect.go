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

//CONNECT packet

type ConnectPacket struct {
	FixedHeader
	ProtocolName    string
	ProtocolVersion byte
	CleanSession    bool
	WillFlag        bool
	WillQos         byte
	WillRetain      bool
	UsernameFlag    bool
	PasswordFlag    bool
	ReservedBit     byte
	KeepaliveTimer  uint16

	ClientIdentifier string
	WillTopic        string
	WillMessage      []byte
	Username         string
	Password         []byte
	uuid             uuid.UUID
}

func (c *ConnectPacket) String() string {
	str := fmt.Sprintf("%s\n", c.FixedHeader)
	str += fmt.Sprintf("protocolversion: %d protocolname: %s cleansession: %t willflag: %t WillQos: %d WillRetain: %t Usernameflag: %t Passwordflag: %t keepalivetimer: %d\nclientId: %s\nwilltopic: %s\nwillmessage: %s\nUsername: %s\nPassword: %s\n", c.ProtocolVersion, c.ProtocolName, c.CleanSession, c.WillFlag, c.WillQos, c.WillRetain, c.UsernameFlag, c.PasswordFlag, c.KeepaliveTimer, c.ClientIdentifier, c.WillTopic, c.WillMessage, c.Username, c.Password)
	return str
}

func (c *ConnectPacket) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.Write(encodeString(c.ProtocolName))
	body.WriteByte(c.ProtocolVersion)
	body.WriteByte(boolToByte(c.CleanSession)<<1 | boolToByte(c.WillFlag)<<2 | c.WillQos<<3 | boolToByte(c.WillRetain)<<5 | boolToByte(c.PasswordFlag)<<6 | boolToByte(c.UsernameFlag)<<7)
	body.Write(encodeUint16(c.KeepaliveTimer))
	body.Write(encodeString(c.ClientIdentifier))
	if c.WillFlag {
		body.Write(encodeString(c.WillTopic))
		body.Write(encodeBytes(c.WillMessage))
	}
	if c.UsernameFlag {
		body.Write(encodeString(c.Username))
	}
	if c.PasswordFlag {
		body.Write(encodeBytes(c.Password))
	}
	c.FixedHeader.RemainingLength = body.Len()
	packet := c.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

func (c *ConnectPacket) Unpack(b io.Reader) {
	c.ProtocolName = decodeString(b)
	c.ProtocolVersion = decodeByte(b)
	options := decodeByte(b)
	c.ReservedBit = 1 & options
	c.CleanSession = 1&(options>>1) > 0
	c.WillFlag = 1&(options>>2) > 0
	c.WillQos = 3 & (options >> 3)
	c.WillRetain = 1&(options>>5) > 0
	c.PasswordFlag = 1&(options>>6) > 0
	c.UsernameFlag = 1&(options>>7) > 0
	c.KeepaliveTimer = decodeUint16(b)
	c.ClientIdentifier = decodeString(b)
	if c.WillFlag {
		c.WillTopic = decodeString(b)
		c.WillMessage = decodeBytes(b)
	}
	if c.UsernameFlag {
		c.Username = decodeString(b)
	}
	if c.PasswordFlag {
		c.Password = decodeBytes(b)
	}
}

func (c *ConnectPacket) Validate() byte {
	if c.PasswordFlag && !c.UsernameFlag {
		return CONN_REF_BAD_USER_PASS
	}
	// MQTT 3.1.1 §3.1.2.10: Will QoS 值 3 是保留值, 必须按协议违规拒绝
	// (旧实现接受 WillQos=3, 后续 Will 消息按非法 QoS 处理)。
	if c.WillQos > 2 {
		fmt.Println("Will QoS reserved value 3")
		return CONN_PROTOCOL_VIOLATION
	}
	// MQTT 3.1.1 §3.1.3.3/§4.7.3: WillFlag=1 时 WillTopic 必须为合法主题名
	// (非空、无通配符、无空字符)。遗嘱消息在客户端异常断开时投递, 非法主题
	// 名污染日志/保留消息(与 2529 PUBLISH 主题名同源)。
	if c.WillFlag && !IsValidTopicName(c.WillTopic) {
		fmt.Println("Bad will topic")
		return CONN_PROTOCOL_VIOLATION
	}
	// MQTT 3.1.1 §3.1.2.3: if WillFlag is 0, WillQoS MUST be 0 and WillRetain MUST be 0.
	if !c.WillFlag {
		if c.WillQos != 0 {
			fmt.Println("Will flag not set but WillQoS non-zero")
			return CONN_PROTOCOL_VIOLATION
		}
		if c.WillRetain {
			fmt.Println("Will flag not set but WillRetain true")
			return CONN_PROTOCOL_VIOLATION
		}
	}
	if c.ReservedBit != 0 {
		fmt.Println("Bad reserved bit")
		return CONN_PROTOCOL_VIOLATION
	}
	if (c.ProtocolName == "MQIsdp" && c.ProtocolVersion != 3) || (c.ProtocolName == "MQTT" && c.ProtocolVersion != 4) {
		return CONN_REF_BAD_PROTO_VER
	}
	if c.ProtocolName != "MQIsdp" && c.ProtocolName != "MQTT" {
		fmt.Println("Bad protocol name")
		return CONN_PROTOCOL_VIOLATION
	}
	if len(c.ClientIdentifier) > 65535 || len(c.Username) > 65535 || len(c.Password) > 65535 {
		fmt.Println("Bad size field")
		return CONN_PROTOCOL_VIOLATION
	}
	// 非空 client ID 拒绝控制字符(拼进日志/持久化可注入伪造行)与超长值(日志/
	// 持久化/订阅表放大)。空 ID 由 server 按 MQTT 3.1.1 §3.1.3.1 处理(分配
	// UUID 或拒绝), 故此处仅校验非空值。旧实现无字符集/长度校验(validateclientID
	// 恒 return true 且无调用点)。
	if len(c.ClientIdentifier) > 0 {
		if len(c.ClientIdentifier) > 128 {
			fmt.Println("Bad client id length")
			return CONN_REF_ID_REJ
		}
		for _, r := range c.ClientIdentifier {
			if r < 0x20 || r == 0x7f {
				fmt.Println("Bad client id character")
				return CONN_REF_ID_REJ
			}
		}
	}
	return CONN_ACCEPTED
}

func (c *ConnectPacket) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}

func (c *ConnectPacket) UUID() uuid.UUID {
	return c.uuid
}
