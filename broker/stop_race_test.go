package hrotti

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

// TestStopConcurrentWithStopListenerNoPanic 覆盖 Stop 与 StopListener 并发
// 调用对同一 listener 双 close(stop) 的竞态: 修复前 Stop 用 RLock 遍历并
// close, StopListener 可并发再 close 同一 channel 触发 panic; 修复后两者
// 互斥, 每个 stop channel 恰好 close 一次。
func TestStopConcurrentWithStopListenerNoPanic(t *testing.T) {
	h := NewHrotti(10, &MemoryPersistence{})
	names := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		lc, err := NewListenerConfigWithError(fmt.Sprintf("tcp://127.0.0.1:%d", port))
		if err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("l%d", i)
		if err := h.AddListener(name, lc); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			h.Stop()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			// StopListener 对已删除/未删除 listener 混合调用, not found 是合法结果
			_ = h.StopListener(names[i%len(names)])
		}
	}()
	wg.Wait()

	// 修复后重复 Stop 必须仍是安全 no-op(清空后的 map 不再有 stop channel)
	h.Stop()
	h.Stop()
}
