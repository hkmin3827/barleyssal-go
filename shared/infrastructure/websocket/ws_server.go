package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"barleyssal-go/config"
	"barleyssal-go/shared/ports"
	"barleyssal-go/shared/session"
)


type clientMeta struct {
	mu                sync.Mutex
	userID            string
	verifiedUserID    string 
	subscribedSymbols map[string]struct{}
}

type incomingMsg struct {
	Type    string      `json:"type"`
	Symbols []string    `json:"symbols"`
	UserID  interface{} `json:"userId"`
}

type client struct {
	conn *websocket.Conn
	send chan []byte
	meta *clientMeta
	hub  *Hub
	log  *zap.Logger
}

type Hub struct {
	log      *zap.Logger
	upgrader websocket.Upgrader
	pnlSvc   ports.PnlSubscriptionService 
	sessionRes *session.Resolver

	mu           sync.RWMutex
	userClients  map[string]map[*client]struct{}
	priceClients map[string]map[*client]struct{}
	allClients   map[*client]struct{}

	connCount int64
}



func NewHub(log *zap.Logger, 	cfg *config.Config, sessionRes *session.Resolver) *Hub {
	return &Hub{
		log: log,
		sessionRes: sessionRes,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			CheckOrigin: func(r *http.Request) bool {
				return r.Header.Get("Origin") == cfg.CorsOrigin
		},
		},
		userClients:  make(map[string]map[*client]struct{}),
		priceClients: make(map[string]map[*client]struct{}),
		allClients:   make(map[*client]struct{}),
	}
}

func (h *Hub) SetPnlService(svc ports.PnlSubscriptionService) {
	h.pnlSvc = svc
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const maxConns = 5000
	if atomic.LoadInt64(&h.connCount) >= maxConns {
		http.Error(w, `{"error":"too many connections"}`, http.StatusServiceUnavailable)
		return
	}

	var verifiedUserID string
	if cookie, err := r.Cookie("SESSION"); err == nil {
		if uid, err := h.sessionRes.UserIDFromCookie(r.Context(), cookie.Value); err == nil {
			verifiedUserID = uid
		}
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warn("WebSocket upgrade failed", zap.Error(err))
		return
	}

	c := &client{
		conn: conn,
		send: make(chan []byte, 256),
		meta: &clientMeta{
			subscribedSymbols: make(map[string]struct{}),
		verifiedUserID:    verifiedUserID,
		},
		hub:  h,
		log:  h.log,
	}

	h.mu.Lock()
	h.allClients[c] = struct{}{}
	h.mu.Unlock()

	atomic.AddInt64(&h.connCount, 1)
	h.log.Info("WS 클라이언트 연결",
		zap.String("remoteAddr", r.RemoteAddr),
		zap.Int64("total", atomic.LoadInt64(&h.connCount)))

	c.sendJSON(map[string]interface{}{"type": "CONNECTED", "ts": nowMS()})

	go c.writePump()
	c.readPump()

	atomic.AddInt64(&h.connCount, -1)
}


func (c *client) readPump() {
	defer func() {
		c.hub.disconnect(c)
		c.conn.Close()
	}()

	c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.log.Warn("WS client error", zap.Error(err))
			}
			return
		}
		c.hub.handleMessage(c, raw)
	}
}

func (c *client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *client) sendJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	safeSend(c.send, data, c.log)
}

func safeSend(ch chan []byte, data []byte, log *zap.Logger) (sent bool) {
	defer func() {
		if r := recover(); r != nil {
			sent = false
		}
	}()
	select {
	case ch <- data:
		return true
	default:
		if log != nil {
			log.Warn("WS send buffer full, dropping message")
		}
		return false
	}
}

func (h *Hub) handleMessage(c *client, raw []byte) {
	var msg incomingMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "SUBSCRIBE_PRICE":
		h.handleSubscribePrice(c, msg.Symbols)

	case "IDENTIFY_USER":
		c.meta.mu.Lock()
		uid := c.meta.verifiedUserID
		c.meta.mu.Unlock()
 
		if uid == "" {
			c.sendJSON(map[string]interface{}{"type": "AUTH_REQUIRED", "ts": nowMS()})
			return
		}
		h.handleIdentifyUser(c, uid)

	case "SUBSCRIBE_ACCOUNT":
		// userClients 등록 + PnlService 구독 (AccountPage 전용)
		c.meta.mu.Lock()
		uid := c.meta.verifiedUserID
		c.meta.mu.Unlock()
 
		if uid == "" {
			c.sendJSON(map[string]interface{}{"type": "AUTH_REQUIRED", "ts": nowMS()})
			return
		}
		h.handleSubscribeAccount(c, uid)


	case "UNSUBSCRIBE":
		h.disconnect(c)

	case "PING":
		c.sendJSON(map[string]interface{}{"type": "PONG", "ts": nowMS()})
	}
}

func (h *Hub) handleSubscribePrice(c *client, symbols []string) {
	c.meta.mu.Lock()
	defer c.meta.mu.Unlock()

	h.mu.Lock()

	for code := range c.meta.subscribedSymbols {
		if s := h.priceClients[code]; s != nil {
			delete(s, c)
			if len(s) == 0 {
				delete(h.priceClients, code)
			}
		}
	}

	c.meta.subscribedSymbols = make(map[string]struct{}, len(symbols))
	for _, code := range symbols {
		if h.priceClients[code] == nil {
			h.priceClients[code] = make(map[*client]struct{})
		}
		h.priceClients[code][c] = struct{}{}
		c.meta.subscribedSymbols[code] = struct{}{}
	}
	h.mu.Unlock()

	c.sendJSON(map[string]interface{}{"type": "SUBSCRIBED_PRICE", "symbols": symbols, "ts": nowMS()})
}

func (h *Hub) handleIdentifyUser(c *client, userID string) {
	c.meta.mu.Lock()
	oldUserID := c.meta.userID
	if oldUserID != "" && oldUserID != userID {
		h.mu.Lock()
		if s := h.userClients[oldUserID]; s != nil {
			delete(s, c)
			if len(s) == 0 {
				delete(h.userClients, oldUserID)
			}
		}
		h.mu.Unlock()
	}
	c.meta.userID = userID
	c.meta.mu.Unlock()

	h.mu.Lock()
	if h.userClients[userID] == nil {
		h.userClients[userID] = make(map[*client]struct{})
	}
	h.userClients[userID][c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) handleSubscribeAccount(c *client, userID string) {
	c.meta.mu.Lock()
	oldUserID := c.meta.userID

	if oldUserID != "" {
		h.mu.Lock()
		if s := h.userClients[oldUserID]; s != nil {
			delete(s, c)
			if len(s) == 0 {
				delete(h.userClients, oldUserID)
			}
		}
		h.mu.Unlock()
		if h.pnlSvc != nil {
			h.pnlSvc.UnsubscribeUser(oldUserID)
		}
	}

	c.meta.userID = userID
	c.meta.mu.Unlock()

	h.mu.Lock()
	if h.userClients[userID] == nil {
		h.userClients[userID] = make(map[*client]struct{})
	}
	h.userClients[userID][c] = struct{}{}
	h.mu.Unlock()

	if h.pnlSvc != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.pnlSvc.SubscribeUser(ctx, userID); err != nil {
			h.log.Warn("SubscribeUser failed", zap.String("userId", userID), zap.Error(err))
		}
	}

	c.sendJSON(map[string]interface{}{"type": "SUBSCRIBED_ACCOUNT", "userId": userID, "ts": nowMS()})
}

func (h *Hub) disconnect(c *client) {
	c.meta.mu.Lock()
	symbols := make([]string, 0, len(c.meta.subscribedSymbols))
	for code := range c.meta.subscribedSymbols {
		symbols = append(symbols, code)
	}
	userID := c.meta.userID
	c.meta.mu.Unlock()

	h.mu.Lock()
	for _, code := range symbols {
		if s := h.priceClients[code]; s != nil {
			delete(s, c)
			if len(s) == 0 {
				delete(h.priceClients, code)
			}
		}
	}
	if userID != "" {
		if s := h.userClients[userID]; s != nil {
			delete(s, c)
			if len(s) == 0 {
				delete(h.userClients, userID)
			}
		}
	}
	delete(h.allClients, c)
	h.mu.Unlock()

	if userID != "" && h.pnlSvc != nil {
		h.pnlSvc.UnsubscribeUser(userID)
	}

	close(c.send)
}

func (h *Hub) BroadcastPriceUpdate(stockCode string, tickData map[string]interface{}) {
	payload, _ := json.Marshal(tickData)

	h.mu.RLock()
	clients := copyClientSet(h.priceClients[stockCode])
	h.mu.RUnlock()

	for c := range clients {
		safeSend(c.send, payload, nil)
	}
}

func (h *Hub) BroadcastBarComplete(stockCode string, bar ports.BarEvent) {
    payload, _ := json.Marshal(map[string]interface{}{
        "type":      "BAR_COMPLETE",
        "stockCode": stockCode,
        "bar":       bar,
        "ts":        time.Now().UnixMilli(),
    })
    h.mu.RLock()
    clients := copyClientSet(h.priceClients[stockCode])
    h.mu.RUnlock()
    for c := range clients {
        safeSend(c.send, payload, nil)
    }
}

func (h *Hub) BroadcastToAll(payload []byte) {
	h.mu.RLock()
	clients := copyClientSet(h.allClients)
	h.mu.RUnlock()

	for c := range clients {
		safeSend(c.send, payload, nil)
	}
}

func (h *Hub) PushToUser(userID string, data interface{}) {
	h.mu.RLock()
	clients := copyClientSet(h.userClients[userID])
	h.mu.RUnlock()

	if len(clients) == 0 {
		return
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}
	for c := range clients {
		safeSend(c.send, payload, nil)
	}
}

func (h *Hub) NotifyExecution(data interface{}) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return
	}
	userID, _ := m["userId"].(string)
	h.PushToUser(userID, data)

	if h.pnlSvc != nil && userID != "" {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.pnlSvc.RefreshUserSubscription(ctx, userID); err != nil {
				h.log.Warn("refreshUserSubscription failed", zap.String("userId", userID), zap.Error(err))
			}
		}()
	}
}

func (h *Hub) GetConnectedCount() int {
	return int(atomic.LoadInt64(&h.connCount))
}

func copyClientSet(s map[*client]struct{}) map[*client]struct{} {
	cp := make(map[*client]struct{}, len(s))
	for k := range s {
		cp[k] = struct{}{}
	}
	return cp
}

func nowMS() int64 { return time.Now().UnixMilli() }

func floatToString(f float64) string {
	i := int64(f)
	if float64(i) != f {
		return ""
	}
	if i == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		buf = append(buf, byte('0'+i%10))
		i /= 10
	}
	if neg {
		buf = append(buf, '-')
	}
	for l, r := 0, len(buf)-1; l < r; l, r = l+1, r-1 {
		buf[l], buf[r] = buf[r], buf[l]
	}
	return string(buf)
}