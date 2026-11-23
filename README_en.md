<div align="center">
  <img src="Logo.png" alt="MqttBrokerCore" width="120"/>
  <h2>MqttBrokerCore</h2>
  <h3>Lightweight MQTT Broker Core (Based on Hrotti)</h3>
</div>

### 1. Introduction
- An **MQTT 3.1.1 broker core** implemented in Go (broker library), usable as a library and shipped with a standalone server program.
- Supports both **TCP** and **WebSocket** listeners, with multiple listeners configurable at the same time. Schemes other than `tcp://` / `ws://` are rejected during config loading.
- Built-in **QoS 0/1/2** message handling, **will messages** (Will), **retained messages** (Retained), and **topic wildcards** (`+`, `#`).
- Provides default **in-memory persistence** (`MemoryPersistence`), extensible to external storage such as Redis / LevelDB via the `Persistence` interface.
- Built-in **access control**: CONNECT username/password verification plus per-identity topic-level ACL (restricts which topic patterns each identity may publish/subscribe); fail-closed by default. Will topics are ACL-checked as well.
- The runtime counters in `BrokerStats` are accumulated on the real connection / subscription / publish / delivery / drop paths and read through `(*Hrotti).Stats()` (connection counts, received publishes, delivered publishes, drops, subscriptions, uptime).

> This repository is an **enhanced and security-hardened fork** of the upstream [alsm/hrotti](https://github.com/alsm/hrotti), focusing on fixing the security issues in the original project around packet parsing, concurrency, resource control and **access control**.

> **Important**: this broker **does not yet support TLS**. Both the `tcp://` and `ws://` listeners are plaintext, so usernames and passwords travel in the clear. Deploy inside a trusted network or terminate TLS on a front proxy/gateway. See section 9, "Security Boundary".

### 2. Quick Start

> **Environment requirement**: this project declares `go 1.27.4` in `go.mod` (matching the CI `go-version: '1.27.4'` and the Dockerfile's `golang:1.27.4-alpine`). No `toolchain` directive is declared, so no other toolchain is downloaded automatically; to specify explicitly, use `GOTOOLCHAIN=go1.27.4 go build`.

```bash
go mod tidy
go build ./...

# Version (matches the VERSION file in the repository root)
go run . --version

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
| `-conf`  | (empty) | Path to a JSON config file; when provided, starts the listeners described in the config and loads credentials/ACL from its `auth` section. **A config load failure — including an unsupported listener scheme or an invalid `auth` section — exits with status 2 instead of running with a half-loaded config** |
| `--version` | — | Prints `MqttBrokerCore <version>` and exits; the version matches the `VERSION` file in the repository root (currently `1.0.20261123`) and is also the first line of the startup log |
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

- **`maxQueueDepth`**: the size of each client's pending message queue. **Omitting the key (or giving `0` / a negative value) falls back to 100** — `0` creates an unbuffered channel, so every delivery is dropped (the symptom is "connects fine but never receives anything"). A positive value is used as given.
- **`listeners`**: keys are arbitrary; `url` supports only `tcp://` or `ws://` (other schemes fail config loading and abort startup).
- **`auth`** (optional but **strongly recommended**): see "Access Control" below.
- **`logging`**: routes `info` / `protocol` / `error` / `debug` to `stdout`, `stderr` or `discard` respectively.

> **Behaviour when `auth` is absent**: the broker prints a WARNING at startup and enters **fail-closed** mode — every CONNECT is refused (`CONNACK 0x04`). This is deliberate: forgetting to configure authentication should mean nobody can connect, not that everyone can.

### 6. Access Control

#### 6.1 Identity verification (CONNECT)

Each entry of `auth.allowed` declares one identity: username, password, and the topic filters that identity may publish/subscribe.

- Passwords are compared with `crypto/subtle.ConstantTimeCompare` (**constant time**); an unknown username also runs a comparison against a fixed-length (32-byte, `"dummy-password-for-timing-parity"`) string, so the "user not found" path pays a comparable cost too.
  - Measured boundary (stated plainly, not exaggerated): `subtle.ConstantTimeCompare` returns false immediately when the lengths differ and never enters the per-byte loop, so **what it evens out is only the timing of "same length, different content"**. The combination "username does not exist, and the supplied password happens to be the same length as the real one" can still be distinguished by timing. The primary account-enumeration defence is the next bullet (all three failures return the same `0x04`, indistinguishable by return code); the timing side channel is weakened, not eliminated.
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
| **Unauthorised will topic** (`WillTopic` not inside the identity's `pubTopics`) | CONNACK, then close the connection immediately; no session is created | `0x05 CONN_REF_NOT_AUTH` |
| Subscribing to an unauthorised topic | Subscription not registered, SUBACK returned | `0x80` (SUBACK Failure) |
| Publishing to an unauthorised topic (QoS 0) | Silently dropped; not delivered, not retained | none |
| Publishing to an unauthorised topic (QoS 1/2) | Dropped, but **PUBACK/PUBREC still sent** | normal ack |

> **Why the will topic is decided at CONNECT time**: a will message is delivered directly by `client.Stop` → `DeliverMessage` when the client drops, and never passes through the single publish-ACL check point in `client.Receive`. Without this check, `WillTopic` is a channel that bypasses every publish ACL — declare a will on any topic, drop the connection, and the broker publishes on your identity's behalf. A rejected will counts towards `GetAuthStats().PubRejected`.
>
> On "not leaking topic existence": the `0x80` for an unauthorised subscription is identical to the code for a malformed topic filter, and an unauthorised publish is acked as usual. A client therefore cannot tell from return codes or ack timing whether a topic exists or who subscribes to it. (A will rejection returns `0x05` rather than `0x04` so a client can tell "credential problem" from "permission problem"; this only ever happens on a connection that already passed identity verification, so it is not an account-enumeration vector.)

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

#### 6.5 Runtime counters

```go
s := h.Stats()            // StatsSnapshot, assembled from atomic reads
fmt.Println(s.ClientsConnected, s.ClientsTotal, s.ClientsMaximum)
fmt.Println(s.PublishReceived, s.PublishSent, s.PublishDropped)
fmt.Println(s.Subscriptions, s.UptimeSeconds)
```

| Field | Meaning | Collection point |
|-------|---------|------------------|
| `ClientsConnected` / `ClientsTotal` / `ClientsMaximum` / `ClientsDisconnected` | current connections / cumulative connections / peak concurrency / cumulative disconnects | `AddClient()` after `InitClient` establishes the session; `RemoveClient()` when the session ends (DISCONNECT / keepalive timeout / takeover) |
| `PublishReceived` / `MessagesReceived` / `BytesReceived` | inbound publishes that passed the ACL, and their payload bytes | `client.Receive`, after the ACL check |
| `PublishSent` / `MessagesSent` / `BytesSent` | deliveries successfully queued to a client, and their payload bytes | the success branch of `DeliverMessage` |
| `PublishDropped` | deliveries discarded because the outbound queue was full | the `select…default` in `DeliverMessage` |
| `Subscriptions` | current subscription entries | incremented by `AddSub`, decremented by `DeleteSub` / `DeleteSubAll` |
| `UptimeSeconds` | seconds the broker instance has been running | derived from instance creation time |

Publishes rejected by the ACL are **not** counted in `PublishReceived`; they only increment `GetAuthStats().PubRejected`. The two counter sets have disjoint semantics.

### 7. Common Use Cases

| Scenario | Example | Description |
|----------|---------|-------------|
| Single-node local testing | `go run . -conf config.json` (with `allowAnonymous`) | Listens on `0.0.0.0:1883` by default (overridable via `HROTTI_URL`). Without an `auth` section the broker is fail-closed. |
| Multiple listeners + WebSocket | `go run . -conf config.json` | Opens TCP and WS listeners simultaneously, sharing the same topic tree. |
| Edge devices reporting readings only | identity with `pubTopics: ["site/+/reading"]` | The device may only publish readings and cannot subscribe to anything. |
| Operations full subscription | identity with `subTopics: ["#"]` | Granted only when `#` is written explicitly; unauthorised identities get `0x80`. |
| Persistent sessions | `CleanSession=false` with the same `ClientIdentifier` | Unacknowledged QoS 1/2 messages and subscriptions are kept after reconnect. |

### 8. Internal Implementation Overview

- **`Hrotti`**: the broker instance, holding the client table, subscription tree, persister, access controller and statistics object (`BrokerStats`, read via `Stats()`).
- **Bitmap subscription storage**: `subscriptionMap.subBitmap` implements O(1) topic matching, supporting `+` / `#` wildcards.
- **Message ID pool**: `messageIDs` allocates 1–65534; when exhausted it returns `ErrMsgIDsExhausted` to avoid writing an invalid `MessageID=0`.
- **Access control**: `AuthManager` (`broker/auth.go`) holds the credential table and ACL behind an `RWMutex`; ACL decisions reuse the existing `match()` topic-matching primitive, so a `#` hit must come from an explicit rule. `WillTopic` is also checked with `CanPublish` at CONNECT time, closing the will-bypasses-publish-ACL channel.
- **Listener scheme validation**: `ParseConfig` reuses `NewListenerConfigWithError` and accepts only `tcp://` / `ws://`. `Server.AddListener` always does `net.Listen("tcp", host)`, so a URL that looks like another protocol is never silently served over TCP.
- **Persistence interface**: `Persistence` (`Init` / `Open` / `Add` / `GetAll` / `Delete` / `Close` / `Exists` / `Replace`).

### 9. Security Boundary (supported / not supported)

**Supported**

| Capability | Notes |
|------------|-------|
| CONNECT username/password verification | Configurable credential table, constant-time comparison, no distinction between unknown user and wrong password on rejection |
| Topic-level ACL | Per-identity pub/sub topic patterns; `#` requires an explicit grant |
| Will topic ACL | `WillTopic` is decided against `pubTopics` at CONNECT time; a violation gets `0x05` and the connection is closed, closing the "will bypasses publish ACL" hole |
| Fail-closed by default | Every connection is refused when no credential table is configured (and `allowAnonymous` is not set explicitly) |
| Violations do not leak topic existence | Unauthorised subscription gets `0x80`; unauthorised publish is acked then dropped |
| Unauthorised publish does not write retained messages | Retained messages cannot be poisoned by an unauthorised client |
| Rejection counters | `GetAuthStats()` exposes connection/subscribe/publish rejection counts |
| Runtime counters | `(*Hrotti).Stats()` exposes connection counts, received/delivered publishes, drops, subscriptions and uptime |

**Not yet supported**

| Capability | Status and reason |
|------------|-------------------|
| **TLS / MQTTS** | **Not yet supported.** `tcp://` / `ws://` are plaintext, so usernames and passwords travel in the clear. Terminate TLS on a front proxy/gateway, or wait for a future version that implements TLS inside the broker. |
| Anonymous access | Requires explicit `allowAnonymous: true`; unconfigured means always refused |
| Dynamic topic filters (regex in ACL rules) | ACL rules are MQTT topic filters (`+` / `#`); no regex and no `${username}`-style templates |
| User / ACL management API | Injected at startup via config file or `SetAuthConfig` only; no runtime add/remove-user interface |
| Token authentication (JWT / OAuth2) | Static username + password credentials only |
| Access control for `$SYS` topics | Internal publishes such as `$SYS/session_identifier` are now decided by the identity's `pubTopics`; grant `$SYS/#` (or the exact topic) to receive them |

> **Note**: the `$SYS/session_identifier` publish the broker sends to clients with an empty `ClientIdentifier` goes through the same publish ACL (`CanPublish`) as PUBLISH packets and the will topic. Unauthorized identities still connect successfully (protocol compliance), but the publish is suppressed and counted in `PubRejected`; identities explicitly holding `$SYS/#` publish rights still receive it. The subscribe side remains independently decided by `subTopics`.

### 10. Security and Reliability Issues Fixed

| Issue | Fix |
|-------|-----|
| `decodeLength` reading bytes unboundedly causes protocol failure / OOM | Limit to at most 4 bytes and return an error |
| `ReadPacket` did not limit `RemainingLength`, allowing large memory allocation | Added a `MaxRemainingLength` cap (default 64 MiB) |
| Short reads of the packet body were undetected, breaking protocol sync | Introduced `decodeReader` to catch truncation errors |
| `WillQoS` / `WillRetain` were not checked when `WillFlag=0` | Reject inconsistent packets in `ConnectPacket.Validate` |
| Slow / non-reading clients caused send deadlocks | All sends changed to non-blocking `select…default` mode |
| `NewListenerConfig` returned nil on URL parse failure | Added `NewListenerConfigWithError` and validates the scheme |
| **Listener scheme was unchecked: `udp://0.0.0.0:1883` was silently opened as a TCP listener** | `ParseConfig` reuses `NewListenerConfigWithError` and rejects any scheme other than `tcp`/`ws` at load time, aborting startup (exit status 2) |
| **A config without `maxQueueDepth` fell back to the zero value → unbuffered queue → every delivery dropped** | `ParseConfig` and `NewHrotti` clamp `<= 0` to `DefaultMaxQueueDepth` (100); an explicit positive value is honoured |
| **Will bypassed the publish ACL: `WillTopic` was only format-checked at CONNECT, and the delivery path never called `CanPublish`** | `WillTopic` is decided against the identity's `pubTopics` at CONNECT time; violations get `0x05 CONN_REF_NOT_AUTH` and the connection is closed |
| **`$SYS/session_identifier` bypassed the publish ACL: `InitClient` queued it directly without calling `CanPublish`** | The topic is decided against the identity's `pubTopics` before queuing; unauthorised identities have the publish suppressed and counted in `PubRejected`; identities holding `$SYS/#` still receive it |
| **`BrokerStats` was never instantiated, all fields were unexported with no accessors** | Wired into the connection / publish / delivery / drop / subscription paths; added `StatsSnapshot` and `(*Hrotti).Stats()` |
| `getClient` returning nil caused nil pointer panics | Added nil guards in `FindRetained` / `DeliverMessage` |
| Empty `ClientIdentifier` with `CleanSession=0` was not rejected | Return `CONN_REF_ID_REJ` per the MQTT spec |
| **`Username`/`Password` were parsed but never used; any client could subscribe to `#` and bypass all edge-side data** | Added `AuthManager`: constant-time CONNECT credential verification plus topic-level ACL; unauthorised subscription gets `0x80`, unauthorised publish is dropped but still acked; fail-closed by default |

---

> Thanks to the upstream project **[alsm/hrotti](https://github.com/alsm/hrotti)** for the original implementation and design ideas; this project builds compatibility enhancements and security hardening on top of it.
