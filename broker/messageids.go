// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import (
	"errors"
	"github.com/google/uuid"
	"sync"
)

type messageIDs struct {
	sync.RWMutex
	//idChan chan uint16
	index map[uint16]*uuid.UUID
}

const (
	msgIDMax uint16 = 65535
	msgIDMin uint16 = 1
)

/*func (c *Client) genMsgIDs() {
	defer c.Done()
	m := &c.messageIDs
	for {
		m.Lock()
		for i := msgIDMin; i < msgIDMax; i++ {
			if m.index[i] == nil {
				m.index[i] =
				m.Unlock()
				select {
				case m.idChan <- i:
				case <-c.stop:
					return
				}
				break
			}
		}
	}
}*/

// ErrMsgIDsExhausted is returned when the in-use 1..65534 message-id pool is
// exhausted. Callers must wait for in-flight messages to be acknowledged
// before sending further QoS1/QoS2 messages.
var ErrMsgIDsExhausted = errors.New("message ids exhausted")

func (m *messageIDs) getMsgID(id uuid.UUID) (uint16, error) {
	m.Lock()
	defer m.Unlock()
	for i := msgIDMin; i < msgIDMax; i++ {
		if m.index[i] == nil {
			m.index[i] = &id
			return i, nil
		}
	}
	return 0, ErrMsgIDsExhausted
}

func (m *messageIDs) inUse(id uint16) bool {
	m.RLock()
	defer m.RUnlock()
	return m.index[id] != nil
}

func (m *messageIDs) freeID(id uint16) {
	m.Lock()
	defer m.Unlock()
	m.index[id] = nil
}

// setID 记录某个 MessageID 对应的消息 uuid(用于 INBOUND 方向: 客户端分配的
// MessageID → 原始 PUBLISH uuid 的反查, 见 Client.recordInboundID)。
func (m *messageIDs) setID(id uint16, u uuid.UUID) {
	m.Lock()
	defer m.Unlock()
	m.index[id] = &u
}
