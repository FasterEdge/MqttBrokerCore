// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
package hrotti

import (
	"crypto/subtle"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

// 本文件是 MqttBrokerCore 的访问控制实现, 覆盖三件事:
//
//  1. CONNECT 阶段的身份校验: 凭据表(用户名 → 密码)逐条比对, 密码用
//     crypto/subtle.ConstantTimeCompare 做常量时间比较, 避免按字节泄露
//     "前缀正确"的时序侧信道。
//  2. 主题级 ACL: 每个身份声明允许 publish / subscribe 的主题过滤器列表,
//     按 MQTT 主题匹配语义裁决; '#' 只对显式写了 '#' 的规则放行(没有任何
//     隐式通配提升), 因此"任意客户端订阅 '#'" 在配置凭据后不再成立。
//  3. 拒绝计数: 供运维/论文度量"越权尝试被拦截次数"。
//
// 三条有意的设计约束:
//   - fail-closed: 未配置任何凭据(且未显式允许匿名)时拒绝一切 CONNECT,
//     而不是"默认放开"。忘了配置的后果是所有人都连不上, 不是所有人都能连。
//   - 不泄漏主题存在性: 未授权订阅回 SUBACK 0x80, 未授权发布按 MQTT 3.1.1
//     §4.5 的两种允许做法之一处理(正面确认后丢弃), 两种情形都不含任何
//     "该主题是否存在/有谁订阅"的信息。
//   - 只复用 broker 包内既有的 match() 主题匹配原语, 不反向依赖 packets,
//     避免 import cycle。

// authCounters 记录访问控制的拒绝次数, 与 BrokerStats 的既有风格
// (atomic.AddInt64) 保持一致。
var authCounters struct {
	connRejected    int64 // CONNECT 凭据校验失败
	subRejected     int64 // SUBSCRIBE 被 ACL 拒绝
	pubRejected     int64 // PUBLISH 被 ACL 拒绝
	unknownIdentity int64 // 凭据表里查不到该身份(保守拒绝)
}

// AuthStats 是 authCounters 的可读快照。
type AuthStats struct {
	ConnRejected    int64
	SubRejected     int64
	PubRejected     int64
	UnknownIdentity int64
}

// GetAuthStats 返回当前访问控制计数器的快照。计数器在进程生命周期内单调
// 累加(不随客户端断开复位), 适合按差值计算某段时间的拦截率。
func GetAuthStats() AuthStats {
	return AuthStats{
		ConnRejected:    atomic.LoadInt64(&authCounters.connRejected),
		SubRejected:     atomic.LoadInt64(&authCounters.subRejected),
		PubRejected:     atomic.LoadInt64(&authCounters.pubRejected),
		UnknownIdentity: atomic.LoadInt64(&authCounters.unknownIdentity),
	}
}

// ResetAuthStatsForTest 仅供测试复位计数器; 生产环境不应调用, 否则
// "累计"语义被破坏。
func ResetAuthStatsForTest() {
	atomic.StoreInt64(&authCounters.connRejected, 0)
	atomic.StoreInt64(&authCounters.subRejected, 0)
	atomic.StoreInt64(&authCounters.pubRejected, 0)
	atomic.StoreInt64(&authCounters.unknownIdentity, 0)
}

func noteSubRejected() { atomic.AddInt64(&authCounters.subRejected, 1) }
func notePubRejected() { atomic.AddInt64(&authCounters.pubRejected, 1) }

// AnonymousIdentity 是匿名模式下赋予连接的固定身份名。
const AnonymousIdentity = "anonymous"

// ErrNotAuthenticated 表示连接未通过身份校验。
var ErrNotAuthenticated = errors.New("client is not authenticated")

// Credentials 是单个身份: 用户名、密码, 以及该身份被允许 publish /
// subscribe 的主题过滤器列表。过滤器列表为空表示"无任何主题被允许"
// (白名单语义, 不是黑名单语义), 因此漏配的结果是拒绝而不是放行。
type Credentials struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	PubTopics []string `json:"pubTopics"`
	SubTopics []string `json:"subTopics"`
}

// AuthConfig 是 AuthManager 的可序列化输入, 同时供 JSON 配置文件使用。
//
// AllowedCredentials 与 AllowAnonymous 必须二选一:
//   - 只给 AllowedCredentials → 严格模式(推荐)
//   - 只给 AllowAnonymous:true → 匿名模式, 明确关闭访问控制
//   - 都不给 → fail-closed, 拒绝一切 CONNECT
//   - 同时给 → SetAuthConfig 返回错误(拒绝启动, 避免"以为有鉴权其实没有")
type AuthConfig struct {
	AllowedCredentials []Credentials `json:"allowedCredentials"`
	AllowAnonymous     bool          `json:"allowAnonymous"`
}

type authMode uint8

const (
	authModeClosed    authMode = iota // 未配置: 拒绝一切连接(fail-closed)
	authModeAnonymous                 // 显式匿名: 全部放行(无访问控制)
	authModeStrict                    // 凭据表 + ACL
)

// AuthManager 保存凭据表与 ACL。CONNECT 校验发生在每个连接自己的
// goroutine 中, 而配置可能被并发替换, 故整表用 RWMutex 保护。
type AuthManager struct {
	mu    sync.RWMutex
	mode  authMode
	users map[string]*Credentials
}

// NewAuthManager 返回 fail-closed 的空管理器: 在成功配置之前拒绝一切
// CONNECT。
func NewAuthManager() *AuthManager {
	return &AuthManager{mode: authModeClosed, users: make(map[string]*Credentials)}
}

// SetAuthConfig 用给定配置替换凭据表。
//   - cfg 为 nil → fail-closed(拒绝一切 CONNECT)
//   - AllowAnonymous 与 AllowedCredentials 同时设置 → 返回错误, 表保持原状
//   - 用户名重复/为空/含 NUL → 返回错误, 表保持原状
//
// 返回错误的设计意图: 配置错误必须在启动阶段暴露, 不能静默降级成
// "少了一个身份"或"多了一条放行"。
func (a *AuthManager) SetAuthConfig(cfg *AuthConfig) error {
	if a == nil {
		return errors.New("auth: AuthManager is nil")
	}
	if cfg == nil {
		a.mu.Lock()
		a.mode = authModeClosed
		a.users = make(map[string]*Credentials)
		a.mu.Unlock()
		return nil
	}
	if cfg.AllowAnonymous && len(cfg.AllowedCredentials) > 0 {
		return errors.New("auth: allowAnonymous cannot be combined with allowedCredentials")
	}
	if cfg.AllowAnonymous {
		a.mu.Lock()
		a.mode = authModeAnonymous
		a.users = make(map[string]*Credentials)
		a.mu.Unlock()
		INFO.Println("auth: anonymous mode enabled, access control is OFF")
		return nil
	}
	if len(cfg.AllowedCredentials) == 0 {
		// 空表等同未配置: 保持 fail-closed, 不返回错误(启动后由监听日志提示)。
		a.mu.Lock()
		a.mode = authModeClosed
		a.users = make(map[string]*Credentials)
		a.mu.Unlock()
		return nil
	}
	users := make(map[string]*Credentials, len(cfg.AllowedCredentials))
	for i := range cfg.AllowedCredentials {
		u := cfg.AllowedCredentials[i]
		if u.Username == "" {
			return errors.New("auth: username must not be empty")
		}
		if strings.ContainsRune(u.Username, '\u0000') {
			return errors.New("auth: username must not contain NUL")
		}
		if _, dup := users[u.Username]; dup {
			return errors.New("auth: duplicate username: " + u.Username)
		}
		// 复制一份, 避免调用方后续改动 slice 反向影响已生效的 ACL。
		copied := u
		copied.PubTopics = append([]string(nil), u.PubTopics...)
		copied.SubTopics = append([]string(nil), u.SubTopics...)
		users[u.Username] = &copied
	}
	a.mu.Lock()
	a.mode = authModeStrict
	a.users = users
	a.mu.Unlock()
	return nil
}

// Mode 报告当前模式, 便于启动日志与测试断言。
func (a *AuthManager) Mode() string {
	if a == nil {
		return "closed"
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	switch a.mode {
	case authModeAnonymous:
		return "anonymous"
	case authModeStrict:
		return "strict"
	default:
		return "closed"
	}
}

// Enabled 报告访问控制是否已生效(严格或匿名模式)。
func (a *AuthManager) Enabled() bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.mode != authModeClosed
}

// Authenticate 校验 CONNECT 提供的用户名/密码, 返回 (身份, 是否通过):
//   - fail-closed(未配置) → ("", false)
//   - 匿名模式 → (AnonymousIdentity, true)
//   - 用户名不存在 / 密码不匹配 → ("", false), 计数 +1
//   - 通过 → (凭据表中的用户名, true)
//
// 关于"抹平时序差"的实际边界(README 必须与此一致): 用户名不存在时确实
// 会走一次对定长 31 字节串的常量时间比较, 让"查不到用户"这条路径也付出
// 一次可观的比较代价; 但 crypto/subtle.ConstantTimeCompare 在长度不等时
// 立即返回 false, 并不进入逐字节比较, 因此"用户不存在"与"密码长度恰好
// 等于真实密码长度但内容错误"之间仍存在长度可区分的时序差异。账号枚举的
// 主要防御是三种失败统一返回 0x04(返回码不可区分), 时序侧信道只被削弱
// 而未被消除。
func (a *AuthManager) Authenticate(username string, password []byte) (string, bool) {
	if a == nil {
		return "", false
	}
	a.mu.RLock()
	mode := a.mode
	cred, ok := a.users[username]
	a.mu.RUnlock()
	switch mode {
	case authModeClosed:
		return "", false
	case authModeAnonymous:
		return AnonymousIdentity, true
	}
	if !ok {
		// dummy 比较只为让"用户不存在"路径也付出一次比较代价, 结果有意丢弃。
		_ = constantTimeEqual(password, []byte("dummy-password-for-timing-parity"))
		atomic.AddInt64(&authCounters.unknownIdentity, 1)
		atomic.AddInt64(&authCounters.connRejected, 1)
		return "", false
	}
	if !constantTimeEqual(password, []byte(cred.Password)) {
		atomic.AddInt64(&authCounters.connRejected, 1)
		return "", false
	}
	return cred.Username, true
}

// constantTimeEqual 是 crypto/subtle.ConstantTimeCompare 的薄封装; 长度
// 不等直接返回 false(不进逐字节比较, 也不泄露更长前缀)。因此它抹平的
// 是"等长但内容不同"的时序差, 而非任意长度下的耗时。
func constantTimeEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare(a, b) == 1
}

// CanPublish 裁决 topic 是否允许被 identity 发布。identity 为空(未认证
// 会话)一律拒绝。匿名模式放行; 严格模式下查身份的 PubTopics。
func (a *AuthManager) CanPublish(identity, topic string) bool {
	return a.allow(identity, topic, false)
}

// CanSubscribe 裁决 topic filter 是否允许被 identity 订阅, 语义同
// CanPublish, 查的是 SubTopics。
func (a *AuthManager) CanSubscribe(identity, topicFilter string) bool {
	return a.allow(identity, topicFilter, true)
}

func (a *AuthManager) allow(identity, topic string, forSubscribe bool) bool {
	if a == nil || identity == "" {
		return false
	}
	a.mu.RLock()
	mode := a.mode
	cred, ok := a.users[identity]
	a.mu.RUnlock()
	switch mode {
	case authModeClosed:
		return false
	case authModeAnonymous:
		return true
	}
	if !ok {
		// 身份在 CONNECT 后才被移出凭据表(配置被替换): 保守拒绝。
		atomic.AddInt64(&authCounters.unknownIdentity, 1)
		return false
	}
	if forSubscribe {
		return topicAllowed(cred.SubTopics, topic)
	}
	return topicAllowed(cred.PubTopics, topic)
}

// topicAllowed 判断 topic 是否命中 patterns 中任意一条主题过滤器。复用
// broker 包内既有的 match(route, topic) 主题匹配原语: ACL 规则本身也是
// 主题过滤器, 所以 "site/+/temp" 覆盖 "site/1/temp", 而 "#" 只命中显式
// 写了 "#" 的规则——这就是"'#' 只能被显式授权的身份订阅"的落点。
func topicAllowed(patterns []string, topic string) bool {
	if len(patterns) == 0 {
		return false
	}
	topicSegs := strings.Split(topic, "/")
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if match(strings.Split(p, "/"), topicSegs) {
			return true
		}
	}
	return false
}
