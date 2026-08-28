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
