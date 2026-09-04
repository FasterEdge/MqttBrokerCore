package packets

import (
	"bytes"
	"testing"
)

// FuzzReadPacket ensures the MQTT frame parser never panics on arbitrary
// input bytes. Handles the full fixed-header + remaining-length decode path
// plus per-type Unpack bodies (CONNECT/PUBLISH/SUBSCRIBE/...): malformed
// frames must be rejected (or produce packets) without crashing, so a remote
// unauthenticated peer cannot take the broker down via malformed frames.
// The 64MiB remaining-length cap means a supplied input is always bounded.
func FuzzReadPacket(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x10})                                     // CONNECT, len 0
	f.Add([]byte{0x30, 0x03, 0x00, 0x03, 'a', 'b', 'c'})    // PUBLISH 1 byte payload missing
	f.Add([]byte{0x30, 0x05, 0x00, 0x02, 'a', 'b', 'c'})    // PUBLISH tiny
	f.Add([]byte{0x82, 0x0a, 0x00, 0x02, 'a', 'b', 0x00})   // SUBSCRIBE with topic
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff})             // invalid remaining length
	f.Add([]byte{0xC0, 0x00})                               // valid PINGREQ
	f.Fuzz(func(t *testing.T, data []byte) {
		// Never panic, whatever the input.
		_, _ = ReadPacket(bytes.NewReader(data))
	})
}