<div align="center">
  <img src="Logo.png" alt="MqttBrokerCore" width="120"/>
  <h2>MqttBrokerCore</h2>
  <h3>Lightweight MQTT Broker Core (Based on Hrotti)</h3>
</div>

### 1. Introduction
- An **MQTT 3.1.1 broker core** implemented in Go (broker library), usable as a library and shipped with a standalone server program.
- Supports both **TCP** and **WebSocket** listeners, with multiple listeners configurable at the same time.
- Built-in **QoS 0/1/2** message handling, **will messages** (Will), **retained messages** (Retained), and **topic wildcards** (`+`, `#`).
- Provides default **in-memory persistence** (`MemoryPersistence`), extensible to external storage such as Redis / LevelDB via the `Persistence` interface.
- Built-in **access control**: CONNECT username/password verification plus per-identity topic-level ACL (restricts which topic patterns each identity may publish/subscribe); fail-closed by default.
- The internal `BrokerStats` records metrics such as connection count, throughput and drops, for integration with monitoring systems.

> This repository is an **enhanced and security-hardened fork** of the upstream [alsm/hrotti](https://github.com/alsm/hrotti), focusing on fixing the security issues in the original project around packet parsing, concurrency, resource control and **access control**.

> **Important**: this broker **does not yet support TLS**. Both the `tcp://` and `ws://` listeners are plaintext, so usernames and passwords travel in the clear. Deploy inside a trusted network or terminate TLS on a front proxy/gateway. See section 9, "Security Boundary".

### 2. Quick Start

> **Environment requirement**: This project declares `toolchain go1.25.13` in `go.mod` (with standard-library CVEs fixed). Go 1.21+ will automatically download and use this toolchain; to specify explicitly, use `GOTOOLCHAIN=go1.25.13 go build`.

```bash
go mod tidy
go build ./...

# Start as a standalone server (defaults to tcp://0.0.0.0:1883; HROTTI_URL overrides it)
go run .

# Or start with a config file (multiple listeners / WebSocket / access control)
go run . -conf config.json

# WARNING: without -conf there is no auth section, so the broker is fail-closed
# and refuses every CONNECT. For a quick local try-out, disable access control
# explicitly, e.g. go run . -conf config.json with "auth": {"allowAnonymous": true}
```

### 3. Integration as a Library

Use `MqttBrokerCore` as a dependency to create an MQTT server:

```go
package main

import (
	"os"
	"os/signal"
	"syscall"

	hrotti "github.com/FasterEdge/MqttBrokerCore/broker"
)

func main() {
	h := hrotti.NewHrotti(100, &hrotti.MemoryPersistence{})
	hrotti.INFO = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)

	// Access control: unconfigured means fail-closed (all CONNECTs refused).
	// For a local try-out, disable explicitly:
	//   h.SetAuthConfig(&hrotti.AuthConfig{AllowAnonymous: true})
	if err := h.SetAuthConfig(&hrotti.AuthConfig{AllowedCredentials: []hrotti.Credentials{{
		Username:  "sensor",
		Password:  "sensor-pw",
		PubTopics: []string{"site/+/reading"},
		SubTopics: []string{"site/+/reading"},
	}}}); err != nil {
		panic(err)
	}

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
| `-conf`  | (empty) | Path to a JSON config file; when provided, starts the listeners described in the config and loads credentials/ACL from its `auth` section |
| `HROTTI_URL` | `tcp://0.0.0.0:1883` | Single-listener address in `tcp://…` or `ws://…` form when `-conf` is not provided; on this path there is no `auth` config and the broker is fail-closed |

### 5. Config File (JSON Example)

```json
{
  "maxQueueDepth": 100,
  "listeners": {
    "tcp": { "url": "tcp://0.0.0.0:1883" },
    "ws":  { "url": "ws://0.0.0.0:2000/mqtt" }
  },
  "auth": {
    "allowed": [
      {
        "username": "sensor",
        "password": "sensor-pw",
        "pubTopics": ["site/+/reading"],
        "subTopics": ["site/+/reading"]
      },
      {
        "username": "ops",
        "password": "ops-pw",
        "pubTopics": ["site/#"],
        "subTopics": ["#"]
      }
    ]
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
- **`auth`** (optional but **strongly recommended**): see "Access Control" below.
- **`logging`**: routes `info` / `protocol` / `error` / `debug` to `stdout`, `stderr` or `discard` respectively.

> **Behaviour when `auth` is absent**: the broker prints a WARNING at startup and enters **fail-closed** mode — every CONNECT is refused (`CONNACK 0x04`). This is deliberate: forgetting to configure authentication should mean nobody can connect, not that everyone can.

### 6. Access Control

#### 6.1 Identity verification (CONNECT)

Each entry of `auth.allowed` declares one identity: username, password, and the topic filters that identity may publish/subscribe.

- Passwords are compared with `crypto/subtle.ConstantTimeCompare` (**constant time**); an unknown username also runs an equal-length dummy comparison, removing the account-enumeration timing side channel.
- Unknown username, wrong password and missing credentials **all return the same `CONNACK 0x04`** (`CONN_REF_BAD_USER_PASS`), so a client cannot enumerate valid usernames from the return code.
- Verification happens **before** any session state (client table, persistence, subscription tree) is touched, so an unauthenticated connection cannot take over someone else's durable session via a "same ClientIdentifier reconnect".
- Passwords are only compared against the config — never logged, never persisted.

#### 6.2 Topic-level ACL

Each identity declares `pubTopics` (topics it may publish to) and `subTopics` (topic filters it may subscribe to):

- **Whitelist semantics**: an empty list means the identity may publish/subscribe nothing at all (a mis-configuration results in denial, not in access).
- ACL rules are themselves MQTT topic filters, so a rule of `site/+/reading` covers `site/1/reading` and `site/2/reading` but not `site/1/reading/extra`.
- **`#` is granted only to identities that explicitly list it**: there is no implicit wildcard promotion. In the example above, `sensor` subscribing to `#` is refused while `ops` is accepted.

#### 6.3 Behaviour on violation

| Scenario | Broker behaviour | MQTT return code |
|----------|------------------|------------------|
| Bad / missing credentials | CONNACK, then close the connection immediately | `0x04 CONN_REF_BAD_USER_PASS` |
| Subscribing to an unauthorised topic | Subscription not registered, SUBACK returned | `0x80` (SUBACK Failure) |
| Publishing to an unauthorised topic (QoS 0) | Silently dropped; not delivered, not retained | none |
| Publishing to an unauthorised topic (QoS 1/2) | Dropped, but **PUBACK/PUBREC still sent** | normal ack |

> On "not leaking topic existence": the `0x80` for an unauthorised subscription is identical to the code for a malformed topic filter, and an unauthorised publish is acked as usual. A client therefore cannot tell from return codes or ack timing whether a topic exists or who subscribes to it.

#### 6.4 Relevant API

```go
h := hrotti.NewHrotti(100, &hrotti.MemoryPersistence{})

// Configure access control (duplicate/empty usernames, or mixing anonymous
// with credentials, all return an error)
err := h.SetAuthConfig(&hrotti.AuthConfig{
    AllowedCredentials: []hrotti.Credentials{{
        Username:  "sensor",
        Password:  "sensor-pw",
        PubTopics: []string{"site/+/reading"},
        SubTopics: []string{"site/+/reading"},
    }},
})

// Explicitly disabling access control (must be written out; you cannot
// disable it implicitly by "not configuring")
_ = h.SetAuthConfig(&hrotti.AuthConfig{AllowAnonymous: true})

hrotti.GetAuthStats() // ConnRejected/SubRejected/PubRejected/UnknownIdentity
```

`SetAuthConfig(nil)` clears the credential table and returns to fail-closed (usable as a safe fallback when a config hot-reload fails).

### 7. Common Use Cases

| Scenario | Example | Description |
|----------|---------|-------------|
| Single-node local testing | `go run . -conf config.json` (with `allowAnonymous`) | Listens on `0.0.0.0:1883` by default (overridable via `HROTTI_URL`). Without an `auth` section the broker is fail-closed. |
| Multiple listeners + WebSocket | `go run . -conf config.json` | Opens TCP and WS listeners simultaneously, sharing the same topic tree. |
| Edge devices reporting readings only | identity with `pubTopics: ["site/+/reading"]` | The device may only publish readings and cannot subscribe to anything. |
| Operations full subscription | identity with `subTopics: ["#"]` | Granted only when `#` is written explicitly; unauthorised identities get `0x80`. |
| Persistent sessions | `CleanSession=false` with the same `ClientIdentifier` | Unacknowledged QoS 1/2 messages and subscriptions are kept after reconnect. |

### 8. Internal Implementation Overview

- **`Hrotti`**: the broker instance, holding the client table, subscription tree, persister, access controller and statistics objects.
- **Bitmap subscription storage**: `subscriptionMap.subBitmap` implements O(1) topic matching, supporting `+` / `#` wildcards.
- **Message ID pool**: `messageIDs` allocates 1–65534; when exhausted it returns `ErrMsgIDsExhausted` to avoid writing an invalid `MessageID=0`.
- **Access control**: `AuthManager` (`broker/auth.go`) holds the credential table and ACL behind an `RWMutex`; ACL decisions reuse the existing `match()` topic-matching primitive, so a `#` hit must come from an explicit rule.
- **Persistence interface**: `Persistence` (`Init` / `Open` / `Add` / `GetAll` / `Delete` / `Close` / `Exists` / `Replace`).

### 9. Security Boundary (supported / not supported)

**Supported**

| Capability | Notes |
|------------|-------|
| CONNECT username/password verification | Configurable credential table, constant-time comparison, no distinction between unknown user and wrong password on rejection |
| Topic-level ACL | Per-identity pub/sub topic patterns; `#` requires an explicit grant |
| Fail-closed by default | Every connection is refused when no credential table is configured (and `allowAnonymous` is not set explicitly) |
| Violations do not leak topic existence | Unauthorised subscription gets `0x80`; unauthorised publish is acked then dropped |
| Unauthorised publish does not write retained messages | Retained messages cannot be poisoned by an unauthorised client |
| Rejection counters | `GetAuthStats()` exposes connection/subscribe/publish rejection counts |

**Not yet supported**

| Capability | Status and reason |
|------------|-------------------|
| **TLS / MQTTS** | **Not yet supported.** `tcp://` / `ws://` are plaintext, so usernames and passwords travel in the clear. Terminate TLS on a front proxy/gateway, or wait for a future version that implements TLS inside the broker. |
| Anonymous access | Requires explicit `allowAnonymous: true`; unconfigured means always refused |
| Dynamic topic filters (regex in ACL rules) | ACL rules are MQTT topic filters (`+` / `#`); no regex and no `${username}`-style templates |
| User / ACL management API | Injected at startup via config file or `SetAuthConfig` only; no runtime add/remove-user interface |
| Token authentication (JWT / OAuth2) | Static username + password credentials only |
| Access control for `$SYS` topics | Internal publishes such as `$SYS/session_identifier` are not ACL-filtered — see the note below |

> **Known caveat**: the broker's `$SYS/session_identifier` publish to clients with an empty `ClientIdentifier` is not ACL-filtered (it is queued directly by `InitClient` after the session is established). It only leaks the server-generated UUID and no other client's data; decide on the subscribe side as well if strictness is required.

### 10. Security and Reliability Issues Fixed

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
| **`Username`/`Password` were parsed but never used; any client could subscribe to `#` and bypass all edge-side data** | Added `AuthManager`: constant-time CONNECT credential verification plus topic-level ACL; unauthorised subscription gets `0x80`, unauthorised publish is dropped but still acked; fail-closed by default |

---

> Thanks to the upstream project **[alsm/hrotti](https://github.com/alsm/hrotti)** for the original implementation and design ideas; this project builds compatibility enhancements and security hardening on top of it.
