package main

import (
	"testing"

	. "github.com/FasterEdge/MqttBrokerCore/broker"
)

// TestResolveListenerFallsBackToDefault verifies the HROTTI_URL bootstrap
// cannot produce a nil listener: with no env var set, main would previously
// dereference listener.URL and panic (nil pointer dereference at startup,
// exit code 2 in the container).
func TestResolveListenerFallsBackToDefault(t *testing.T) {
	t.Setenv("HROTTI_URL", "")
	lc := resolveListener()
	if lc == nil {
		t.Fatal("resolveListener returned nil, main would panic")
	}
	if got := lc.URL.Host; got != "0.0.0.0:1883" {
		t.Fatalf("default listener host = %q, want 0.0.0.0:1883", got)
	}
}

// TestResolveListenerKeepsValidCustomURL ensures a real HROTTI_URL is honored.
func TestResolveListenerKeepsValidCustomURL(t *testing.T) {
	t.Setenv("HROTTI_URL", "tcp://0.0.0.0:2883")
	lc := resolveListener()
	if lc == nil || lc.URL.Host != "0.0.0.0:2883" {
		t.Fatalf("custom HROTTI_URL not honored: %+v", lc)
	}
}

// TestResolveListenerRejectsGarbageURL pins the behavior for an unparsable
// env value: fall back to the default instead of crashing.
func TestResolveListenerRejectsGarbageURL(t *testing.T) {
	t.Setenv("HROTTI_URL", "::not-a-valid-url::")
	lc := resolveListener()
	if lc == nil {
		t.Fatal("resolveListener returned nil for garbage URL")
	}
	if got := lc.URL.Host; got != "0.0.0.0:1883" {
		t.Fatalf("garbage URL listener host = %q, want fallback 0.0.0.0:1883", got)
	}
}

// TestNewListenerConfigEmptyIsSafe documents that the legacy one-return
// constructor returns nil (not a crash) for empty input — callers must
// nil-check, which resolveListener does.
func TestNewListenerConfigEmptyIsSafe(t *testing.T) {
	t.Setenv("HROTTI_URL", "")
	if lc := NewListenerConfig(""); lc == nil {
		t.Log("empty rawURL -> nil ListenerConfig (expected)")
	} else {
		t.Fatalf("expected nil for empty rawURL, got %+v", lc)
	}
}