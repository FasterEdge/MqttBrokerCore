package hrotti

import (
	"strconv"
	"testing"
)

// Test_AddSub_Bitmap verifies that the subscription bitmap implementation can
// accept a wide variety of topic filters (including wildcards) without panic.
// It mimics the original intent of the legacy tree-based test but uses the
// current bitmap data structures.
func Test_AddSub_Bitmap(t *testing.T) {
	h := NewHrotti(10, &MemoryPersistence{})
	if h.subs == nil {
		t.Fatal("subs must be initialised by NewHrotti")
	}
	topics := []string{"a", "b", "c", "d", "e", "+", "#"}
	for i := 0; i < 50; i++ {
		cid := "testClientId" + strconv.Itoa(i%10)
		sub := ""
		depth := i % 7
		for j := 0; j <= depth; j++ {
			sub += topics[(i+j)%len(topics)]
			if sub[len(sub)-1] == '#' || j == depth {
				break
			}
			sub += "/"
		}
		h.AddSub(cid, sub, 1)
	}
}

func TestAddSubscriptionValidation(t *testing.T) {
	h := NewHrotti(10, &MemoryPersistence{})
	c := &Client{clientID: "test"}

	// 合法主题 + QoS 0/1/2 通过
	if rq := h.AddSubscription(c, []string{"a/b", "a/#", "a/+"}, []byte{0, 1, 2}); rq[0] != 0 || rq[1] != 1 || rq[2] != 2 {
		t.Fatalf("valid topics rejected: %v", rq)
	}
	// 非法 QoS(3) → SUBACK 0x80
	if rq := h.AddSubscription(c, []string{"a/b"}, []byte{3}); rq[0] != 0x80 {
		t.Fatalf("invalid qos not rejected: %v", rq)
	}
	// 非法主题过滤器 → 0x80
	for _, bad := range []string{"#/a", "a#", "a//b", "/a", "a/", "a+/b", ""} {
		if rq := h.AddSubscription(c, []string{bad}, []byte{0}); rq[0] != 0x80 {
			t.Fatalf("invalid topic filter %q not rejected: %v", bad, rq)
		}
	}
}
