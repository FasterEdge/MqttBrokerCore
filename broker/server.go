// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package hrotti

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	. "github.com/FasterEdge/MqttBrokerCore/packets"
	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

type Hrotti struct {
	PersistStore       Persistence
	listeners          map[string]*internalListener
	listenersMu        sync.RWMutex
	listenersWaitGroup sync.WaitGroup
	maxQueueDepth      int
	clients            *clients
	subs               *subscriptionMap
	// auth 是 CONNECT 身份校验与主题级 ACL 的唯一裁决点。零值不可用,
	// 必须经 NewAuthManager() 或 SetAuthManager() 注入; 未注入时
	// InitClient 按 fail-closed 拒绝连接。
	auth *AuthManager
}

type internalListener struct {
	name        string
	url         url.URL
	mu          sync.Mutex // 保护 connections(多连接并发 append 是数据竞争)
	connections []net.Conn
	stop        chan struct{}
}

func NewHrotti(maxQueueDepth int, persistence Persistence) *Hrotti {
	h := &Hrotti{
		PersistStore:  persistence,
		listeners:     make(map[string]*internalListener),
		maxQueueDepth: maxQueueDepth,
		clients:       newClients(),
		subs:          newSubMap(),
		auth:          NewAuthManager(),
	}
	//start the goroutine that generates internal message ids for when clients receive messages
	//but are not connected.
	h.PersistStore.Init()
	return h
}

func (h *Hrotti) getClient(id string) *Client {
	h.clients.RLock()
	defer h.clients.RUnlock()
	return h.clients.list[id]
}

// SetAuthManager 替换访问控制管理器。传 nil 会把管理器置为 fail-closed
// 空表(拒绝一切 CONNECT), 这是有意的: 想放开鉴权必须显式配置
// AllowAnonymous 走 SetAuthConfig, 不能靠"不设置"隐式放开。
func (h *Hrotti) SetAuthManager(a *AuthManager) {
	if a == nil {
		a = NewAuthManager()
	}
	h.auth = a
}

// SetAuthConfig 用一份 AuthConfig 配置(或覆盖)本 broker 的凭据表与 ACL。
// 传 nil 或空 AllowedCredentials 会清空表并进入 fail-closed。
func (h *Hrotti) SetAuthConfig(cfg *AuthConfig) error {
	if h.auth == nil {
		h.auth = NewAuthManager()
	}
	return h.auth.SetAuthConfig(cfg)
}

// AuthManager 返回本 broker 的访问控制管理器(便于运维/测试读取状态),
// 不会因返回 nil 而使调用方 panic: NewHrotti 始终注入非 nil 实例。
func (h *Hrotti) AuthManager() *AuthManager {
	if h.auth == nil {
		h.auth = NewAuthManager()
	}
	return h.auth
}

func (h *Hrotti) AddListener(name string, config *ListenerConfig) error {
	listener := &internalListener{name: name, url: *config.URL}
	listener.stop = make(chan struct{})

	h.listenersMu.Lock()
	h.listeners[name] = listener
	h.listenersMu.Unlock()

	ln, err := net.Listen("tcp", listener.url.Host)
	if err != nil {
		ERROR.Println(err.Error())
		return err
	}

	if listener.url.Scheme == "ws" && len(listener.url.Path) == 0 {
		listener.url.Path = "/"
	}

	h.listenersWaitGroup.Add(1)
	INFO.Println("Starting MQTT listener on", listener.url.String())

	go func() {
		<-listener.stop
		INFO.Println("Listener", name, "is stopping...")
		ln.Close()
	}()
	//if this is a WebSocket listener
	if listener.url.Scheme == "ws" {
		var server websocket.Server
		//override the Websocket handshake to accept any protocol name
		server.Handshake = func(c *websocket.Config, req *http.Request) error {
			c.Origin, _ = url.Parse(req.RemoteAddr)
			c.Protocol = []string{"mqtt"}
			return nil
		}
		//set up the ws connection handler, ie what we do when we get a new websocket connection
		server.Handler = func(ws *websocket.Conn) {
			ws.PayloadType = websocket.BinaryFrame
			INFO.Println("New incoming websocket connection", ws.RemoteAddr())
			listener.mu.Lock()
			listener.connections = append(listener.connections, ws)
			listener.mu.Unlock()
			h.InitClient(ws)
		}
		//set the path that the http server will recognise as related to this websocket
		//server, needs to be configurable really.
		http.Handle(listener.url.Path, server)
		//ListenAndServe loops forever receiving connections and initiating the handler
		//for each one.
		go func(ln net.Listener) {
			defer h.listenersWaitGroup.Done()
			// 仅设 ReadHeaderTimeout: WebSocket 连接是长连接, 不能设 Read/Write 总超时,
			// 但握手阶段需要防御慢速头注入(slowloris)占用连接与 goroutine。
			srv := &http.Server{Handler: nil, ReadHeaderTimeout: 10 * time.Second}
			err := srv.Serve(ln)
			if err != nil {
				ERROR.Println(err.Error())
				return
			}
		}(ln)
	} else {
		//loop forever accepting connections and launch InitClient as a goroutine with the connection
		go func() {
			defer h.listenersWaitGroup.Done()
			for {
				conn, err := ln.Accept()
				if err != nil {
					ERROR.Println(err.Error())
					return
				}
				INFO.Println("New incoming connection", conn.RemoteAddr())
				listener.mu.Lock()
				listener.connections = append(listener.connections, conn)
				listener.mu.Unlock()
				go h.InitClient(conn)
			}
		}()
	}
	return nil
}

func (h *Hrotti) StopListener(name string) error {
	h.listenersMu.Lock()
	defer h.listenersMu.Unlock()
	if listener, ok := h.listeners[name]; ok {
		close(listener.stop)
		listener.mu.Lock()
		conns := append([]net.Conn(nil), listener.connections...)
		listener.mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
		delete(h.listeners, name)
		return nil
	}
	return errors.New("Listener not found")
}

func (h *Hrotti) Stop() {
	INFO.Println("Exiting...")
	// 与 StopListener 互斥: 两者都会 close(listener.stop), 并发调用会对
	// 同一 listener 双 close 触发 panic。写锁 + 锁内清空 map 保证每个
	// stop channel 恰好 close 一次; 重复调用 Stop 遍历空 map 为安全 no-op。
	h.listenersMu.Lock()
	for _, listener := range h.listeners {
		close(listener.stop)
		listener.mu.Lock()
		conns := append([]net.Conn(nil), listener.connections...)
		listener.mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	}
	h.listeners = make(map[string]*internalListener)
	h.listenersMu.Unlock()
	h.listenersWaitGroup.Wait()
}

func (h *Hrotti) InitClient(conn net.Conn) {
	var sendSessionID bool
	/*var cph fixedHeader

	//create a bufio conn from the network connection
	bufferedConn := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	//first byte off the wire should be the msg type
	typeByte, _ := bufferedConn.ReadByte()
	//unpack the first byte into the fixed header
	cph.unpack(typeByte)

	if cph.messageType != CONNECT {
		//If the first packet isn't a CONNECT, it's not MQTT or not compliant, so kill the connection and we're done.
		conn.Close()
		return
	}

	//read the remaining length field from the network, this can be 1-3 bytes generally although in this case
	//it should always be 1 byte, but using the generic method.
	cph.remainingLength = decodeLength(bufferedConn)
	//a buffer to receive the rest of the connect packet
	body := make([]byte, cph.remainingLength)
	io.ReadFull(bufferedConn, body)
	//create a new empty CONNECT packet to unpack the body of the CONNECT into
	cp := newControlPacket(CONNECT).(*connectPacket)
	cp.fixedHeader = cph
	cp.unpack(body)*/
	rp, err := ReadPacket(conn)
	if err != nil || rp == nil {
		ERROR.Println("Failed to read CONNECT from", conn.RemoteAddr(), err)
		conn.Close()
		return
	}
	cp, ok := rp.(*ConnectPacket)
	if !ok {
		ERROR.Println("First packet from", conn.RemoteAddr(), "is not a CONNECT")
		conn.Close()
		return
	}

	//Validate the CONNECT, check fields, values etc.
	rc := cp.Validate()
	//If it didn't validate...
	if rc != CONN_ACCEPTED {
		//and it wasn't because of a protocol violation...
		if rc != CONN_PROTOCOL_VIOLATION {
			//create and send a CONNACK with the correct rc in it.
			ca := NewControlPacket(CONNACK).(*ConnackPacket)
			ca.ReturnCode = rc
			ca.Write(conn)
		}
		//Put up a local message indicating an errored connection attempt and close the connection
		ERROR.Println(ConnackReturnCodes[rc], conn.RemoteAddr())
		conn.Close()
		return
	} else {
		// CONNECT 报文本身合规, 但此刻尚未鉴权, 不能记 "Connection
		// Accepted"(会让日志显示"已接受"紧接着"拒绝"的误导性组合)。
		DEBUG.Println("CONNECT validated for", cp.ClientIdentifier, conn.RemoteAddr())
	}

	// MQTT 3.1.1 §4.1/§4.2: 身份校验必须发生在任何会话状态(客户端表、
	// 持久化、订阅表)被触碰之前, 否则未认证连接可以借"同 ClientID 重连"
	// 接管他人的持久会话。凭据缺失/不符时回 CONN_REF_BAD_USER_PASS。
	identity, ok := h.auth.Authenticate(cp.Username, cp.Password)
	if !ok {
		// 不区分"用户名不存在"与"密码错误", 统一 0x04, 避免账号枚举。
		ca := NewControlPacket(CONNACK).(*ConnackPacket)
		ca.ReturnCode = CONN_REF_BAD_USER_PASS
		_ = ca.Write(conn)
		ERROR.Println(ConnackReturnCodes[CONN_REF_BAD_USER_PASS], conn.RemoteAddr())
		conn.Close()
		return
	}
	INFO.Println("Authenticated", cp.ClientIdentifier, "as", identity, conn.RemoteAddr())

	//check for a zero length client id and if it exists create one from the UUID library and return
	//it on $SYS/session_identifier. MQTT 3.1.1 §3.1.3.1: when ClientIdentifier is empty the
	//client MUST set CleanSession=1; otherwise the server MUST reject.
	if len(cp.ClientIdentifier) == 0 {
		if !cp.CleanSession {
			ERROR.Println("Rejecting empty ClientID with CleanSession=0 from", conn.RemoteAddr())
			ca := NewControlPacket(CONNACK).(*ConnackPacket)
			ca.ReturnCode = CONN_REF_ID_REJ
			_ = ca.Write(conn)
			conn.Close()
			return
		}
		cp.ClientIdentifier = uuid.New().String()
		sendSessionID = true
	}
	//Lock the clients hashmap while we check if we already know this clientid.
	h.clients.Lock()
	c, ok := h.clients.list[cp.ClientIdentifier]
	if ok && cp.CleanSession {
		//and if we do, if the clientid is currently connected...
		if c.Connected() {
			INFO.Println("Clientid", c.clientID, "already connected, stopping first client")
			//stop the parts of it that need to stop before we can change the network connection it's using.
			c.StopForTakeover()
		} else {
			//if the clientid known but not connected, ie cleansession false
			INFO.Println("Durable client reconnecting", c.clientID)
			//disconnected client will no longer have the channels for messages
			c.outboundMessages = make(chan *PublishPacket, h.maxQueueDepth)
			c.outboundPriority = make(chan ControlPacket, h.maxQueueDepth)
		}
		//this function stays running until the client disconnects as the function called by an http
		//Handler has to remain running until its work is complete. So add one to the client waitgroup.
		c.Add(1)
		//create a new sync.Once for stopping with later, set the connections and create the stop channel.
		c.stopOnce = new(sync.Once)
		c.conn = conn
		//c.bufferedConn = bufferedConn
		c.stop = make(chan struct{})
		// 重连(含接管)也必须重新绑定身份: 若连接被新身份取走而沿用旧
		// 身份的 ACL, 会出现"低权限客户端继承高权限订阅"的提权路径。
		c.identity = identity
		//start the client.
		go c.Start(cp, h)
	} else {
		//This is a brand new client so create a NewClient and add to the clients map
		c = newClient(conn, cp.ClientIdentifier, identity, h.maxQueueDepth)
		h.clients.list[cp.ClientIdentifier] = c
		if sendSessionID {
			go func() {
				sessionIDPacket := NewControlPacket(PUBLISH).(*PublishPacket)
				sessionIDPacket.TopicName = "$SYS/session_identifier"
				sessionIDPacket.Payload = []byte(cp.ClientIdentifier)
				sessionIDPacket.Qos = 1
				// Non-blocking: drop if the queue is full; client will see the ID via CONNACK path if needed.
				select {
				case c.outboundMessages <- sessionIDPacket:
				default:
				}
			}()
		}
		//As before this function has to remain running but to avoid races we want to make sure its finished
		//before doing anything else so add it to the waitgroup so we can wait on it later
		c.Add(1)
		go c.Start(cp, h)
	}
	//finished with the clients hashmap
	h.clients.Unlock()
	//wait on the stop channel, we never actually send values down this channel but a closed channel with
	//return the default empty value for it's type without blocking.
	<-c.stop
	//call Done() on the client waitgroup.
	c.Done()
}
