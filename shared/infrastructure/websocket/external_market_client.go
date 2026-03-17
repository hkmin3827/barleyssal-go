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

// ExternalMarketClient는 KIS(한국투자증권) 실시간 시세 웹소켓 클라이언트다.
// 틱 데이터를 수신하여 priceHook(PriceService)과 hub(WebSocket Hub)에 전달한다.
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
		wsURL:     wsURL,
		watchList: watchList,
		authSvc:   authSvc,
		priceHook: priceHook,
		hub:       hub,
		log:       log,
		stopCh:    make(chan struct{}),
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
		c.log.Info("KIS 웹소켓 구독 취소 및 연결 종료 완료")
	}
}

func (c *ExternalMarketClient) connect(ctx context.Context) {
	c.log.Info("KIS WebSocket 연결 중...", zap.String("url", c.wsURL))
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, c.wsURL, nil)
	if err != nil {
		c.log.Error("KIS WS 연결 실패", zap.Error(err))
		c.scheduleReconnect(ctx)
		return
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.log.Info("KIS WS 연결 성공")
	c.subscribeAll(conn)

	// ==========================================
	// go func() {
	// 	ticker := time.NewTicker(2 * time.Second) // 2초마다 가짜 데이터 생성
	// 	defer ticker.Stop()
	// 	for {
	// 		select {
	// 		case <-c.stopCh:
	// 			return
	// 		case <-ticker.C:
	// 			// 시간 생성 (HHMMSS)
	// 			nowStr := time.Now().Format("150405")
				
	// 			// KIS 실시간 데이터 포맷에 맞춘 가짜 문자열 (총 35개 필드, 구분자 ^)
	// 			// 인덱스 0:종목(005930), 1:시간, 2:현재가(80000), 3:전일대비부호(2), 4:전일대비(100), 5:등락율(0.12), 12:단일체결량(10) 등
	// 			mockFields := []string{
	// 				"005930", nowStr, "80000", "2", "100", "0.12", "80000", "79000", "80500", "78500",
	// 				"80100", "79900", "10", "1000000", "80000000000", "500", "600",
	// 				"0", "55.5", "0", "600000", "0", "0", "0", "0", "0", "0", "0", "0", "0",
	// 				"0", "0", "0", "0", "20",
	// 			}
	// 			mockData := fmt.Sprintf("0|H0STCNT0|001|%s", strings.Join(mockFields, "^"))

	// 			// handleMessage 호출 (실제 로직 태우기)
	// 			c.handleMessage(ctx, mockData, c.conn)
	// 		}
	// 	}
	// }()
	// ==========================================
	
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
		_ = conn.WriteMessage(websocket.TextMessage, c.buildMsg(key, code, "1"))
	}
	c.log.Info("KIS 구독 요청 완료", zap.Int("count", len(c.watchList)))
}

func (c *ExternalMarketClient) unsubscribeAll(conn *websocket.Conn) {
	key := c.authSvc.GetApprovalKey()
	if key == "" {
		return
	}
	for i, code := range c.watchList {
		time.Sleep(time.Duration(i) * 50 * time.Millisecond)
		_ = conn.WriteMessage(websocket.TextMessage, c.buildMsg(key, code, "2"))
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

	// c.log.Info("KIS RAW RESPONSE", zap.String("raw_data", raw))

	if strings.HasPrefix(raw, "{") {
		var j map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &j); err == nil {
			if hdr, ok := j["header"].(map[string]interface{}); ok {
				if hdr["tr_id"] == "PINGPONG" {
					_ = conn.WriteMessage(websocket.TextMessage, []byte(raw))
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
	if len(fields) < 22 || fields[0] == "" || fields[2] == "" {
		return
	}
	if len(fields) > 34 {
		if mkop := fields[34]; len(mkop) > 0 && mkop[0] == '3' {
			return
		}
	}

	stockCode := fields[0]
	price, err := strconv.ParseFloat(fields[2], 64)
	if err != nil || price <= 0 {
		return
	}

	prdyVrssSign := fields[3]
	prdyVrss, _ := strconv.ParseFloat(fields[4], 64)
	changeRate, _ := strconv.ParseFloat(fields[5], 64)
	stckOprc, _ := strconv.ParseFloat(fields[7], 64)
	stckHgpr, _ := strconv.ParseFloat(fields[8], 64)
	stckLwpr, _ := strconv.ParseFloat(fields[9], 64)
	askp1, _ := strconv.ParseFloat(fields[10], 64)
	bidp1, _ := strconv.ParseFloat(fields[11], 64)
	cntgVol, _ := strconv.ParseFloat(fields[12], 64) // 단일 체결량
	acmlVol, _ := strconv.ParseInt(fields[13], 10, 64)  // 누적 거래량
	acmlTrPbmn, _ := strconv.ParseInt(fields[14], 10, 64)
	selnCntgCsnu, _ := strconv.ParseFloat(fields[15], 64)
	shnuCntgCsnu, _ := strconv.ParseFloat(fields[16], 64)
	cttr, _ := strconv.ParseFloat(fields[18], 64)
	shnuCntgSmtn, _ := strconv.ParseFloat(fields[20], 64)

	var mkopCode string
	if len(fields) > 34 {
		mkopCode = fields[34]
	}

	// KIS 시간 문자열 → UTC 변환
	kisTimeStr := fields[1]
	loc, _ := time.LoadLocation("Asia/Seoul")
	nowKST := time.Now().In(loc)
	fullTimeStr := fmt.Sprintf("%s%s", nowKST.Format("20060102"), kisTimeStr)
	parsedKST, _ := time.ParseInLocation("20060102150405", fullTimeStr, loc)
	utcTime := parsedKST.UTC()


	if err := c.priceHook.OnPriceUpdate(ctx, stockCode, price, changeRate, int64(cntgVol), prdyVrss, stckOprc, stckHgpr, stckLwpr, acmlVol, utcTime,); err != nil {
		c.log.Warn("priceHook.OnPriceUpdate failed", zap.String("stockCode", stockCode), zap.Error(err))
	}

	// 금일 매수 거래량 갱신
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
		"stckOprc":        stckOprc,
		"stckHgpr":        stckHgpr,
		"stckLwpr":        stckLwpr,
		"askp1":           askp1,
		"bidp1":           bidp1,
		"cntgVol":         cntgVol,   // 단일 체결량 (차트 live bar volume 갱신용)
		"acmlVol":         acmlVol,   // 당일 누적 거래량 (UI 표시용)
		"acmlTrPbmn":      acmlTrPbmn,
		"cttr":            cttr,
		"selnCntgCsnu":    selnCntgCsnu,
		"shnuCntgCsnu":    shnuCntgCsnu,
		"mkopCode":        mkopCode,
		"shnuCntgSmtn":    shnuCntgSmtn,
		"ts":              utcTime.UnixMilli(),
	}
	c.hub.BroadcastPriceUpdate(stockCode, tickData)
}
