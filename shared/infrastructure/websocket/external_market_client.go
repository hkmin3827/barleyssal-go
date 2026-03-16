package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	kisauth "barleyssal-go/shared/infrastructure/kis_auth"
	"barleyssal-go/shared/ports"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type ExternalMarketClient struct {
	wsURL     string
	watchList []string
	authSvc   *kisauth.KisAuthService
	priceHook ports.PriceEventHandler
	hub       *Hub
	log       *zap.Logger

	mu       sync.Mutex
	conn     *websocket.Conn
	stopping bool
	stopCh   chan struct{}
}

func NewExternalMarketClient(
	wsURL string,
	watchList []string,
	authSvc *kisauth.KisAuthService,
	priceHook ports.PriceEventHandler,
	hub *Hub,
	log *zap.Logger,
) *ExternalMarketClient {
	return &ExternalMarketClient{
		wsURL: wsURL, watchList: watchList,
		authSvc: authSvc, priceHook: priceHook, hub: hub, log: log,
		stopCh: make(chan struct{}),
	}
}

func (c *ExternalMarketClient) Start(ctx context.Context) error {
	if c.wsURL == "" {
		c.log.Error("EXTERNAL_WS_URL is not set! Cannot start KIS Client.")
		return nil
	}
	c.mu.Lock()
	c.stopping = false
	c.mu.Unlock()

	if _, err := c.authSvc.FetchApprovalKey(ctx); err != nil {
		return err
	}
	go c.connect(ctx)
	return nil
}

func (c *ExternalMarketClient) Stop() {
	c.mu.Lock()
	c.stopping = true
	conn := c.conn
	c.mu.Unlock()

	close(c.stopCh)

	if conn != nil {
		c.unsubscribeAll(conn)
		time.Sleep(time.Duration(len(c.watchList)*50+1000) * time.Millisecond)
		conn.Close()
		c.log.Info("✅ KIS 웹소켓 구독 취소 및 연결 종료 완료.")
	}
}

func (c *ExternalMarketClient) connect(ctx context.Context) {
	c.log.Info("Connecting to KIS WebSocket...", zap.String("url", c.wsURL))
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, c.wsURL, nil)
	if err != nil {
		c.log.Error("KIS WS dial failed", zap.Error(err))
		c.scheduleReconnect(ctx)
		return
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.log.Info("KIS WS connected")
	c.subscribeAll(conn)

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.stopCh:
				return
			default:
			}
			c.log.Warn("KIS WS read error", zap.Error(err))
			c.scheduleReconnect(ctx)
			return
		}
		c.handleMessage(ctx, string(raw), conn)
	}
}

func (c *ExternalMarketClient) scheduleReconnect(ctx context.Context) {
	c.mu.Lock()
	stopping := c.stopping
	c.mu.Unlock()
	if stopping {
		return
	}
	time.Sleep(5 * time.Second)
	if _, err := c.authSvc.FetchApprovalKey(ctx); err != nil {
		c.log.Error("Approval Key 재발급 실패", zap.Error(err))
		c.scheduleReconnect(ctx)
		return
	}
	go c.connect(ctx)
}

func (c *ExternalMarketClient) subscribeAll(conn *websocket.Conn) {
	key := c.authSvc.GetApprovalKey()
	if key == "" {
		c.log.Error("Approval Key 없음 - KIS 구독 중단")
		return
	}
	for i, code := range c.watchList {
		time.Sleep(time.Duration(i) * 50 * time.Millisecond)
		conn.WriteMessage(websocket.TextMessage, c.buildMsg(key, code, "1"))
	}
	c.log.Info("Sent subscription requests", zap.Int("count", len(c.watchList)))
}

func (c *ExternalMarketClient) unsubscribeAll(conn *websocket.Conn) {
	key := c.authSvc.GetApprovalKey()
	if key == "" {
		return
	}
	for i, code := range c.watchList {
		time.Sleep(time.Duration(i) * 50 * time.Millisecond)
		conn.WriteMessage(websocket.TextMessage, c.buildMsg(key, code, "2"))
	}
}

func (c *ExternalMarketClient) buildMsg(approvalKey, code, trType string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"header": map[string]string{
			"approval_key": approvalKey, "custtype": "P",
			"tr_type": trType, "content_type": "utf-8",
		},
		"body": map[string]interface{}{
			"input": map[string]string{"tr_id": "H0STCNT0", "tr_key": code},
		},
	})
	return b
}

func (c *ExternalMarketClient) handleMessage(ctx context.Context, raw string, conn *websocket.Conn) {
	if strings.HasPrefix(raw, "{") {
		var j map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &j); err == nil {
			if hdr, ok := j["header"].(map[string]interface{}); ok {
				if hdr["tr_id"] == "PINGPONG" {
					conn.WriteMessage(websocket.TextMessage, []byte(raw))
				}
			}
		}
		return
	}

	parts := strings.Split(raw, "|")
	if len(parts) < 4 || parts[1] != "H0STCNT0" {
		return
	}

	fields := strings.Split(parts[3], "^")
	if len(fields) < 14 || fields[0] == "" || fields[2] == "" {
		return
	}

	stockCode := fields[0]
	price, err := strconv.ParseFloat(fields[2], 64)
	if err != nil || price <= 0 {
		return
	}

	prdyVrssSign := fields[3]
	prdyVrss, _ := strconv.ParseFloat(fields[4], 64)
	changeRate, _ := strconv.ParseFloat(fields[5], 64)
	wghnAvrgStckPrc, _ := strconv.ParseFloat(fields[6], 64)
	stckOprc, _ := strconv.ParseFloat(fields[7], 64)
	stckHgpr, _ := strconv.ParseFloat(fields[8], 64)
	stckLwpr, _ := strconv.ParseFloat(fields[9], 64)
	askp1, _ := strconv.ParseFloat(fields[10], 64)
	bidp1, _ := strconv.ParseFloat(fields[11], 64)
	cntgVol, _ := strconv.ParseFloat(fields[12], 64)
	acmlVol, _ := strconv.ParseInt(fields[13], 10, 64)
	cttr, _ := strconv.ParseFloat(fields[18], 64)

	
	var shnuCntgSmtn float64
	if len(fields) > 20 {
		shnuCntgSmtn, _ = strconv.ParseFloat(fields[20], 64)
	}

	var selnCntgCsnu, shnuCntgCsnu float64
	if len(fields) > 16 {
		selnCntgCsnu, _ = strconv.ParseFloat(fields[15], 64)
		shnuCntgCsnu, _ = strconv.ParseFloat(fields[16], 64)
	}

	var ccldDvsn string
	if shnuCntgCsnu > selnCntgCsnu {
	ccldDvsn = "1" // 매수
	} else {
		ccldDvsn = "5" // 매도
	}

	mkopCode := ""
	if len(fields) > 34 {
		mkopCode = fields[34]
	}

	kisTimeStr := fields[1]
	loc, _ := time.LoadLocation("Asia/Seoul")
	nowKST := time.Now().In(loc)

	fullTimeStr := fmt.Sprintf("%s%s", nowKST.Format("20060102"), kisTimeStr)
	parsedTimeKST, _ := time.ParseInLocation("20060102150405", fullTimeStr, loc)

	utcTime := parsedTimeKST.UTC()

	if err := c.priceHook.OnPriceUpdate(ctx, stockCode, price, changeRate, acmlVol, utcTime); err != nil {
			c.log.Warn("priceHook.OnPriceUpdate failed", zap.String("stockCode", stockCode), zap.Error(err))
	}

	if shnuCntgSmtn > 0 {
		c.priceHook.UpdateBuyVolume(ctx, stockCode, shnuCntgSmtn)
	}

	tickData := map[string]interface{}{
	"type":            "PRICE_UPDATE",
	"stockCode":       stockCode,
	"price":           price,
	"prdyVrssSign":    prdyVrssSign,
	"prdyVrss":        prdyVrss,
	"prdyCtrt":        changeRate,
	"wghnAvrgStckPrc": wghnAvrgStckPrc,
	"stckOprc":        stckOprc,
	"stckHgpr":        stckHgpr,
	"stckLwpr":        stckLwpr,
	"askp1":           askp1,
	"bidp1":           bidp1,
	"cntgVol":         cntgVol,
	"acmlVol":         acmlVol,
	"cttr":            cttr,
	"selnCntgCsnu":    selnCntgCsnu,
	"shnuCntgCsnu":    shnuCntgCsnu,
	"ccldDvsn":        ccldDvsn,
	"mkopCode":        mkopCode,
	"shnuCntgSmtn":    shnuCntgSmtn, 
	"ts":              utcTime.UnixMilli(),
	}

	c.hub.BroadcastPriceUpdate(stockCode, tickData)
}
