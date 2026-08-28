package hrotti

import "testing"

// TestNewListenerConfigRejectsInvalidScheme ensures that the safer
// two-return constructor surfaces scheme errors instead of returning a
// ListenerConfig with an unparseable URL.
func TestNewListenerConfigRejectsInvalidScheme(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"http scheme", "http://0.0.0.0:1883"},
		{"https scheme", "https://0.0.0.0:1883"},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lc, err := NewListenerConfigWithError(tc.raw)
			if err == nil {
				t.Fatalf("expected error for raw=%q, got %+v", tc.raw, lc)
			}
		})
	}
}

func TestNewListenerConfigAcceptsTcpAndWs(t *testing.T) {
	for _, raw := range []string{"tcp://0.0.0.0:1883", "ws://0.0.0.0:2000/mqtt"} {
		lc, err := NewListenerConfigWithError(raw)
		if err != nil {
			t.Fatalf("raw=%q rejected: %v", raw, err)
		}
		if lc == nil || lc.URL == nil {
			t.Fatalf("raw=%q returned nil listener", raw)
		}
	}
}
