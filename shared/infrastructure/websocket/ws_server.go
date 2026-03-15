// Package websocket implements the internal WebSocket hub.
// It mirrors src/shared/infrastructure/websocket/wsServer.js exactly.
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

	"barleyssal-go/shared/ports"
)

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

// clientMeta holds per-connection subscription state (mirrors ClientMeta typedef).
type clientMeta struct {
	mu                sync.Mutex
	userID            string
	subscribedSymbols map[string]struct{}
}

// incomingMsg is the parsed client → server message.
type incomingMsg struct {
	Type    string      `json:"type"`
	Symbols []string    `json:"symbols"`
	UserID  interface{} `json:"userId"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Hub
// ─────────────────────────────────────────────────────────────────────────────

// Hub is the WebSocket server hub.  It implements ports.UserNotifier.
type Hub struct {
	log      *zap.Logger
	upgrader websocket.Upgrader
	pnlSvc   ports.PnlSubscriptionService // injected after construction to break cycles

	mu sync.RWMutex
	// userClients:  userID → set of *client
	userClients map[string]map[*client]struct{}
	// priceClients: stockCode → set of *client
	priceClients map[string]map[*client]struct{}

	connCount int64 // atomic
}

// client represents a single WebSocket connection.
type client struct {
	conn *websocket.Conn
	send chan []byte
	meta *clientMeta
	hub  *Hub
	log  *zap.Logger
}

// ─────────────────────────────────────────────────────────────────────────────
// Constructor & Setup
// ─────────────────────────────────────────────────────────────────────────────

// NewHub creates a new Hub.
func NewHub(log *zap.Logger) *Hub {
	h := &Hub{
		log: log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		userClients:  make(map[string]map[*client]struct{}),
		priceClients: make(map[string]map[*client]struct{}),
	}
	return h
}

// SetPnlService injects the PnL service (avoids circular dependency at construction time).
func (h *Hub) SetPnlService(svc ports.PnlSubscriptionService) {
	h.pnlSvc = svc
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP handler — called by Echo router at GET /ws
// ─────────────────────────────────────────────────────────────────────────────

// ServeHTTP upgrades the HTTP connection and runs the client loop.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Warn("WebSocket upgrade failed", zap.Error(err))
		return
	}

	c := &client{
		conn: conn,
		send: make(chan []byte, 256),
		meta: &clientMeta{subscribedSymbols: make(map[string]struct{})},
		hub:  h,
		log:  h.log,
	}

	atomic.AddInt64(&h.connCount, 1)
	h.log.Info("WS client connected",
		zap.String("remoteAddr", r.RemoteAddr),
		zap.Int64("total", atomic.LoadInt64(&h.connCount)))

	c.sendJSON(map[string]interface{}{"type": "CONNECTED", "ts": nowMS()})

	go c.writePump()
	c.readPump() // blocks until disconnect

	atomic.AddInt64(&h.connCount, -1)
}

// ─────────────────────────────────────────────────────────────────────────────
// readPump / writePump
// ─────────────────────────────────────────────────────────────────────────────

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
	select {
	case c.send <- data:
	default:
		c.log.Warn("WS send buffer full, dropping message")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// handleMessage — mirrors handleMessage switch
// ─────────────────────────────────────────────────────────────────────────────

func (h *Hub) handleMessage(c *client, raw []byte) {
	var msg incomingMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "SUBSCRIBE_PRICE":
		h.handleSubscribePrice(c, msg.Symbols)

	case "SUBSCRIBE_ACCOUNT":
		userID := ""
		switch v := msg.UserID.(type) {
		case string:
			userID = v
		case float64:
			// JSON numbers arrive as float64
			userID = toString(v)
		}
		if userID != "" {
			h.handleSubscribeAccount(c, userID)
		}

	case "UNSUBSCRIBE":
		h.disconnect(c)

	case "PING":
		c.sendJSON(map[string]interface{}{"type": "PONG", "ts": nowMS()})
	}
}

func (h *Hub) handleSubscribePrice(c *client, symbols []string) {
	c.meta.mu.Lock()
	defer c.meta.mu.Unlock()

	// Remove old subscriptions
	h.mu.Lock()
	for code := range c.meta.subscribedSymbols {
		if s := h.priceClients[code]; s != nil {
			delete(s, c)
			if len(s) == 0 {
				delete(h.priceClients, code)
			}
		}
	}

	// Add new subscriptions
	c.meta.subscribedSymbols = make(map[string]struct{}, len(symbols))
	for _, code := range symbols {
		if h.priceClients[code] == nil {
			h.priceClients[code] = make(map[*client]struct{})
		}
		h.priceClients[code][c] = struct{}{}
		c.meta.subscribedSymbols[code] = struct{}{}
	}
	h.mu.Unlock()

	c.sendJSON(map[string]interface{}{
		"type": "SUBSCRIBED_PRICE", "symbols": symbols, "ts": nowMS(),
	})
}

func (h *Hub) handleSubscribeAccount(c *client, userID string) {
	c.meta.mu.Lock()
	oldUserID := c.meta.userID

	// Detach from old user subscription
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

	// Attach to new user
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

	c.sendJSON(map[string]interface{}{
		"type": "SUBSCRIBED_ACCOUNT", "userId": userID, "ts": nowMS(),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// disconnect — mirrors handleDisconnect
// ─────────────────────────────────────────────────────────────────────────────

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
	h.mu.Unlock()

	if userID != "" && h.pnlSvc != nil {
		h.pnlSvc.UnsubscribeUser(userID)
	}

	close(c.send)
}

// ─────────────────────────────────────────────────────────────────────────────
// ports.UserNotifier implementation
// ─────────────────────────────────────────────────────────────────────────────

// BroadcastPriceUpdate sends a PRICE_UPDATE to all price-subscribed clients for stockCode.
func (h *Hub) BroadcastPriceUpdate(stockCode string, price float64, volume int64) {
	h.mu.RLock()
	clients := copyClientSet(h.priceClients[stockCode])
	h.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"type":      "PRICE_UPDATE",
		"stockCode": stockCode,
		"price":     price,
		"volume":    volume,
		"ts":        nowMS(),
	})

	for c := range clients {
		select {
		case c.send <- payload:
		default:
		}
	}
}

// PushToUser sends an arbitrary JSON payload to all WS connections for userId.
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
		select {
		case c.send <- payload:
		default:
		}
	}
}

// NotifyExecution sends an EXECUTION event to the user and schedules a PnL refresh.
func (h *Hub) NotifyExecution(data interface{}) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return
	}
	userID := ""
	if uid, ok := m["userId"].(string); ok {
		userID = uid
	}

	h.PushToUser(userID, data)

	if h.pnlSvc != nil && userID != "" {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.pnlSvc.RefreshUserSubscription(ctx, userID); err != nil {
				h.log.Warn("refreshUserSubscription failed",
					zap.String("userId", userID), zap.Error(err))
			}
		}()
	}
}

// GetConnectedCount returns the number of currently connected WebSocket clients.
func (h *Hub) GetConnectedCount() int {
	return int(atomic.LoadInt64(&h.connCount))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func copyClientSet(s map[*client]struct{}) map[*client]struct{} {
	cp := make(map[*client]struct{}, len(s))
	for k := range s {
		cp[k] = struct{}{}
	}
	return cp
}

func nowMS() int64 { return time.Now().UnixMilli() }

func toString(f float64) string {
	if f == float64(int64(f)) {
		return int64ToString(int64(f))
	}
	return ""
}

func int64ToString(i int64) string {
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
