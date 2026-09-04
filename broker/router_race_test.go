package hrotti

import (
	. "github.com/FasterEdge/MqttBrokerCore/packets"
	"sync"
	"testing"
)

// TestRouterConcurrentAccess 并发访问订阅/保留消息结构, 用 race 检测器实证锁语义。
func TestRouterConcurrentAccess(t *testing.T) {
	h := NewHrotti(100, &MemoryPersistence{})

	// 1. SetRetained 并发写: 当前实现用 RLock 持有期间写 map。
	//    多个写者同时持 RLock 并发 → 数据竞争 (race 检测器应报 WARNING)。
	t.Run("SetRetained_concurrent_writes", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					msg := NewControlPacket(PUBLISH).(*PublishPacket)
					msg.TopicName = "fe/race"
					msg.Payload = []byte{byte(j)}
					h.subs.SetRetained("fe/race", msg)
				}
			}(i)
		}
		wg.Wait()
	})

	// 2. DeliverMessage 在 RUnlock 之后访问 subMap (无锁读),
	//    与 AddSub/DeleteSub (写) 并发 → 潜在竞争。
	t.Run("Deliver_vs_Subscribe", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				h.AddSub("c1", "fe/topic", 1)
				h.DeleteSub("c1", "fe/topic")
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				msg := NewControlPacket(PUBLISH).(*PublishPacket)
				msg.TopicName = "fe/topic"
				msg.Payload = []byte("x")
				h.DeliverMessage("fe/topic", msg)
			}
		}()
		wg.Wait()
	})
}
