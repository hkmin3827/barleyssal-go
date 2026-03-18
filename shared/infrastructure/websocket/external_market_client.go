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
	c.stopCh = make(chan struct{})
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
	c.conn = nil
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

	
	const pongWait = 60 * time.Second
	const pingPeriod = (pongWait * 9) / 10 
	
	conn.SetReadDeadline(time.Now().Add(pongWait))

	conn.SetPongHandler(func(string) error { 
        conn.SetReadDeadline(time.Now().Add(pongWait))
        return nil 
    })

	go func() {
        ticker := time.NewTicker(pingPeriod)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                c.mu.Lock()
                if c.conn != nil {
                    if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                        c.log.Warn("Standard Ping failed", zap.Error(err))
                    }
                }
                c.mu.Unlock()
            case <-c.stopCh:
                return
            }
        }
    }()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.stopCh:
				c.log.Info("스케줄러에 의해 정상적으로 수신 루프가 종료되었습니다.")
				return
			default:
			}
			c.log.Warn("KIS WS 비정상 연결 끊김 감지", zap.Error(err))

			c.scheduleReconnect(ctx)
			return
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))
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

func (c *ExternalMarketClient) RunScheduler(ctx context.Context) {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		c.log.Error("타임존 로드 실패, KST 대신 Local 시간 사용", zap.Error(err))
		loc = time.Local
	}

	ticker := time.NewTicker(1 * time.Minute) // 1분마다 시간 체크
	defer ticker.Stop()

	// 시작하자마자 현재 시간 체크해서 켤지 말지 결정
	c.checkAndToggle(ctx, loc)

	for {
		select {
		case <-ctx.Done(): // 메인 서버 종료 시 스케줄러도 종료
			return
		case <-ticker.C:
			c.checkAndToggle(ctx, loc)
		}
	}
}

func (c *ExternalMarketClient) checkAndToggle(ctx context.Context, loc *time.Location) {
	now := time.Now().In(loc)
	
	// 평일 (월~금) 체크
	isWeekday := now.Weekday() >= time.Monday && now.Weekday() <= time.Friday
	
	// 시간 코드로 변환 (예: 08:30 -> 830, 16:00 -> 1600)
	hour, min, _ := now.Clock()
	timeCode := hour*100 + min

	// 동작 시간: 평일 08:30 ~ 16:00 (시뮬레이션 환경에 맞춰 조정 가능)
	isActiveTime := isWeekday && timeCode >= 630 && timeCode <= 1800

	c.mu.Lock()
	isRunning := !c.stopping && c.conn != nil
	c.mu.Unlock()

	if isActiveTime && !isRunning {
		c.log.Info("📈 장 운영 시간입니다. KIS WS 연결을 시작합니다.")
		// 기존 Start 내부에서 stopCh를 다시 make 해주는 로직이 필요합니다 (아래 STEP 2 참고)
		if err := c.Start(ctx); err != nil {
			c.log.Error("스케줄러: KIS WS 시작 실패", zap.Error(err))
		}
	} else if !isActiveTime && isRunning {
		c.log.Info("💤 장 마감 시간입니다. KIS WS 연결을 안전하게 종료합니다.")
		c.Stop()
	}
}

func (c *ExternalMarketClient) subscribeAll(conn *websocket.Conn) {
	key := c.authSvc.GetApprovalKey()
	if key == "" {
		c.log.Error("Approval Key 없음 - KIS 구독 중단")
		return
	}
	for i, code := range c.watchList {
		time.Sleep(time.Duration(i) * 50 * time.Millisecond)
		_ = c.safeWrite(conn, websocket.TextMessage, c.buildMsg(key, code, "1"))
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
		_ = c.safeWrite(conn, websocket.TextMessage, c.buildMsg(key, code, "2"))
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
				_ = c.safeWrite(conn, websocket.TextMessage, []byte(raw))
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

	prdyVrssSignInt := parseInt64(prdyVrssSign)

	if err := c.priceHook.OnPriceUpdate(ctx, stockCode, price, changeRate, int64(cntgVol), prdyVrssSignInt ,prdyVrss, stckOprc, stckHgpr, stckLwpr, acmlVol, utcTime, mkopCode); err != nil {
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
		"cntgVol":         cntgVol,  
		"acmlVol":         acmlVol,  
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

func parseInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	s, ok := v.(string)
	if !ok {
		return 0
	}
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

func (c *ExternalMarketClient) safeWrite(conn *websocket.Conn, messageType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return conn.WriteMessage(messageType, data)
}