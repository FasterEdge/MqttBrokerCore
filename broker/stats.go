// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import (
	"sync/atomic"
	"time"
)

// BrokerStats 是 broker 的运行期计数器集合。所有字段用 atomic 读写, 可在
// 任意 goroutine 中累加, 由 Snapshot() 取一致性快照。每个字段都在下面的
// 采集点被真实累加, 不存在"声明了但没人写"的字段。
//
// 采集点:
//   - clientsConnected / clientsTotal / clientsMaximum / clientsDisconnected:
//     server.InitClient 在 CONNECT 通过鉴权与遗嘱校验、会话建立后
//     AddClient(); 该连接结束时(正常 DISCONNECT / keepalive 超时 / 被接管)
//     RemoveClient()。
//   - publishMessagesReceived / messagesReceived / bytesReceived:
//     client.Receive 收到并通过 ACL 的 PUBLISH(被 ACL 拒绝的发布只计入
//     auth.GetAuthStats 的 PubRejected, 不计入这里)。
//   - publishMessagesSent / messagesSent / bytesSent: router.DeliverMessage
//     把一条投递成功压入某客户端出站队列时。
//   - publishMessagesDropped: 该入队因队列满而走 select 的 default 分支时
//     (即"丢弃"计数)。
//   - subscriptions: AddSub 增、DeleteSub / DeleteSubAll 减。
//   - brokerTime / brokerUptime: 由 startedAt 推算, 见 Snapshot。
type BrokerStats struct {
	bytesReceived           int64
	bytesSent               int64
	clientsConnected        int64
	clientsDisconnected     int64
	clientsMaximum          int64
	clientsTotal            int64
	messagesReceived        int64
	messagesSent            int64
	publishMessagesDropped  int64
	publishMessagesReceived int64
	publishMessagesSent     int64
	subscriptions           int64
	brokerTime              int64
	brokerUptime            int64
	startedAt               time.Time
}

// StatsSnapshot 是 BrokerStats 的一次性快照(各字段取自同一时刻的原子读,
// 调用方不需要再加锁)。单位: 计数为条/个, 字节为字节, Uptime 为秒。
type StatsSnapshot struct {
	ClientsConnected    int64
	ClientsDisconnected int64
	ClientsTotal        int64
	ClientsMaximum      int64
	PublishReceived     int64
	PublishSent         int64
	PublishDropped      int64
	MessagesReceived    int64
	MessagesSent        int64
	BytesReceived       int64
	BytesSent           int64
	Subscriptions       int64
	UptimeSeconds       int64
}

func newBrokerStats() *BrokerStats {
	return &BrokerStats{startedAt: time.Now()}
}

func (b *BrokerStats) AddClient() {
	atomic.AddInt64(&b.clientsConnected, 1)
	atomic.AddInt64(&b.clientsTotal, 1)
	// clientsMaximum 记录历史并发连接峰值: CAS 循环而不是简单的
	// "读-比较-写", 否则并发接入时峰值会被后写者覆盖成较小值。
	for {
		cur := atomic.LoadInt64(&b.clientsMaximum)
		now := atomic.LoadInt64(&b.clientsConnected)
		if now <= cur {
			return
		}
		if atomic.CompareAndSwapInt64(&b.clientsMaximum, cur, now) {
			return
		}
	}
}

func (b *BrokerStats) RemoveClient() {
	atomic.AddInt64(&b.clientsConnected, -1)
	atomic.AddInt64(&b.clientsDisconnected, 1)
}

// AddPublishReceived 记录一条通过 ACL 的入站 PUBLISH。
func (b *BrokerStats) AddPublishReceived(payloadBytes int) {
	atomic.AddInt64(&b.publishMessagesReceived, 1)
	atomic.AddInt64(&b.messagesReceived, 1)
	atomic.AddInt64(&b.bytesReceived, int64(payloadBytes))
}

// AddPublishSent 记录一条成功入队的投递。
func (b *BrokerStats) AddPublishSent(payloadBytes int) {
	atomic.AddInt64(&b.publishMessagesSent, 1)
	atomic.AddInt64(&b.messagesSent, 1)
	atomic.AddInt64(&b.bytesSent, int64(payloadBytes))
}

// AddPublishDropped 记录一条因出站队列满而丢弃的投递。
func (b *BrokerStats) AddPublishDropped() {
	atomic.AddInt64(&b.publishMessagesDropped, 1)
}

func (b *BrokerStats) AddSubscription()    { atomic.AddInt64(&b.subscriptions, 1) }
func (b *BrokerStats) RemoveSubscription() { atomic.AddInt64(&b.subscriptions, -1) }

func (b *BrokerStats) Snapshot() StatsSnapshot {
	var up int64
	if !b.startedAt.IsZero() {
		up = int64(time.Since(b.startedAt) / time.Second)
		atomic.StoreInt64(&b.brokerUptime, up)
	}
	atomic.StoreInt64(&b.brokerTime, time.Now().Unix())
	return StatsSnapshot{
		ClientsConnected:    atomic.LoadInt64(&b.clientsConnected),
		ClientsDisconnected: atomic.LoadInt64(&b.clientsDisconnected),
		ClientsTotal:        atomic.LoadInt64(&b.clientsTotal),
		ClientsMaximum:      atomic.LoadInt64(&b.clientsMaximum),
		PublishReceived:     atomic.LoadInt64(&b.publishMessagesReceived),
		PublishSent:         atomic.LoadInt64(&b.publishMessagesSent),
		PublishDropped:      atomic.LoadInt64(&b.publishMessagesDropped),
		MessagesReceived:    atomic.LoadInt64(&b.messagesReceived),
		MessagesSent:        atomic.LoadInt64(&b.messagesSent),
		BytesReceived:       atomic.LoadInt64(&b.bytesReceived),
		BytesSent:           atomic.LoadInt64(&b.bytesSent),
		Subscriptions:       atomic.LoadInt64(&b.subscriptions),
		UptimeSeconds:       up,
	}
}
