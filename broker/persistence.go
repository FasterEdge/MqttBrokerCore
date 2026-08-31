// FasterEdge 开源项目 · https://github.com/FasterEdge · https://gitee.com/FasterEdge
package hrotti

import (
	. "github.com/FasterEdge/MqttBrokerCore/packets"
	"github.com/google/uuid"
)

type dirFlag byte

const (
	INBOUND  = 1
	OUTBOUND = 2
)

type Persistence interface {
	Init() error
	Open(string)
	Close(string)
	Add(string, dirFlag, ControlPacket) bool
	Replace(string, dirFlag, ControlPacket) bool
	AddBatch(map[string]*PublishPacket)
	Delete(string, dirFlag, uuid.UUID) bool
	GetAll(string) []ControlPacket
	Exists(string) bool
}
