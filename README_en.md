<div align="center">
  <img src="Logo.png" alt="MqttBrokerCore" width="120"/>
  <h2>MqttBrokerCore</h2>
  <h3>Lightweight MQTT Broker Core (Based on Hrotti)</h3>
</div>

### 1. Introduction
- An **MQTT 3.1.1 / 5.0 broker core** implemented in Go (broker library), usable as a library and shipped with a standalone server program.
- Supports both **TCP** and **WebSocket** listeners, with multiple listeners configurable at the same time.
- Built-in **QoS 0/1/2** message handling, **will messages** (Will), **retained messages** (Retained), and **topic wildcards** (`+`, `#`).
- Provides default **in-memory persistence** (`MemoryPersistence`), extensible to external storage such as Redis / LevelDB via the `Persistence` interface.
- The internal `BrokerStats` records metrics such as connection count, throughput and drops, for integration with monitoring systems.

> This repository is an **enhanced and security-hardened fork** of the upstream [alsm/hrotti](https://github.com/alsm/hrotti), focusing on fixing the security issues in the original project around packet parsing, concurrency and resource control.

### 2. Quick Start

> **Environment requirement**: This project declares `toolchain go1.25.13` in `go.mod` (with standard-library CVEs fixed). Go 1.21+ will automatically download and use this toolchain; to specify explicitly, use `GOTOOLCHAIN=go1.25.13 go build`.

```bash
go mod tidy
go build ./...

# Start as a standalone server
go run . -key mySecret

# Or start with a config file (multiple listeners / WebSocket)
go run . -conf config.json
```

### 3. Integration as a Library

Use `MqttBrokerCore` as a dependency to create an MQTT server:

```go
package main

import (
	"os"
	"os/signal"
	"syscall"

	hrotti "github.com/alsm/hrotti/broker"
)

func main() {
	h := hrotti.NewHrotti(100)
	hrotti.INFO = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)
	h.AddListener("test", hrotti.NewListenerConfig("tcp://0.0.0.0:1883"))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	h.Stop()
}
```

### 4. Startup Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `-key`   | (required) | Access password, used to authenticate the HTTP / MQTT management endpoints |
| `-conf`  | (empty) | Path to a JSON config file; if empty, the `HROTTI_URL` environment variable is used (single listener) |
| `-addr`  | `:1883` | Listening address in single-listener mode (only takes effect when `-conf` is not provided) |
| `-log`   | `stdout`| Log output target: `stdout` / `stderr` / `discard` |

### 5. Config File (JSON Example)

```json
{
  "maxQueueDepth": 100,
  "listeners": {
    "tcp": { "url": "tcp://0.0.0.0:1883" },
    "ws":  { "url": "ws://0.0.0.0:2000/mqtt" }
  },
  "logging": {
    "info": "stdout",
    "protocol": "discard",
    "error": "stderr",
    "debug": "discard"
  }
}
```

- **`maxQueueDepth`**: the size of each client's pending message queue (default 100).
- **`listeners`**: keys are arbitrary; `url` supports only `tcp://` or `ws://` (other schemes are rejected).
- **`logging`**: routes `info` / `protocol` / `error` / `debug` to `stdout`, `stderr` or `discard` respectively.

### 6. Common Use Cases

| Scenario | Example | Description |
|----------|---------|-------------|
| Single-node local testing | `go run . -key test` | Listens on `0.0.0.0:1883` by default, using `test` for auth. |
| Multiple listeners + WebSocket | `go run . -conf config.json` | Opens TCP and WS listeners simultaneously, sharing the same topic tree. |
| Persistent sessions | `CleanSession=false` with the same `ClientIdentifier` | Unacknowledged QoS 1/2 messages and subscriptions are kept after reconnect. |

### 7. Internal Implementation Overview

- **`Hrotti`**: the broker instance, holding the client table, subscription tree, persister and statistics objects.
- **Bitmap subscription storage**: `subscriptionMap.subBitmap` implements O(1) topic matching, supporting `+` / `#` wildcards.
- **Message ID pool**: `messageIDs` allocates 1–65534; when exhausted it returns `ErrMsgIDsExhausted` to avoid writing an invalid `MessageID=0`.
- **Persistence interface**: `Persistence` (`Init` / `Open` / `Add` / `GetAll` / `Delete` / `Close` / `Exists` / `Replace`).

### 8. Security and Reliability Issues Fixed

| Issue | Fix |
|-------|-----|
| `decodeLength` reading bytes unboundedly causes protocol failure / OOM | Limit to at most 4 bytes and return an error |
| `ReadPacket` did not limit `RemainingLength`, allowing large memory allocation | Added a `MaxRemainingLength` cap (default 64 MiB) |
| Short reads of the packet body were undetected, breaking protocol sync | Introduced `decodeReader` to catch truncation errors |
| `WillQoS` / `WillRetain` were not checked when `WillFlag=0` | Reject inconsistent packets in `ConnectPacket.Validate` |
| Slow / non-reading clients caused send deadlocks | All sends changed to non-blocking `select…default` mode |
| `NewListenerConfig` returned nil on URL parse failure | Added `NewListenerConfigWithError` and validates the scheme |
| `getClient` returning nil caused nil pointer panics | Added nil guards in `FindRetained` / `DeliverMessage` |
| Empty `ClientIdentifier` with `CleanSession=0` was not rejected | Return `CONN_REF_ID_REJ` per the MQTT spec |

---

> Thanks to the upstream project **[alsm/hrotti](https://github.com/alsm/hrotti)** for the original implementation and design ideas; this project builds compatibility enhancements and security hardening on top of it.