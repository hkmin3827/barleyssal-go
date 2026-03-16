package chartapplication

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"

	"barleyssal-go/config"
	kisauth "barleyssal-go/shared/infrastructure/kis_auth"
	"barleyssal-go/shared/utils"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type ChartData struct {
	Date   string  `json:"date"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

type OhlcvBar struct {
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
	Minute string
}

type OhlcvBarJSON struct {
	T string  `json:"t"`
	O float64 `json:"o"`
	H float64 `json:"h"`
	L float64 `json:"l"`
	C float64 `json:"c"`
	V float64 `json:"v"`
}

type OhlcvEntry struct {
	T         string  `json:"t"`
	O         float64 `json:"o"`
	H         float64 `json:"h"`
	L         float64 `json:"l"`
	C         float64 `json:"c"`
	V         float64 `json:"v"`
	Timestamp int64   `json:"timestamp"`
}

type ChartService struct {
	cfg        *config.Config
	rdb        *redis.Client
	authSvc    *kisauth.KisAuthService
	restClient *utils.KisRestClient
	log        *zap.Logger

	bufMu  sync.RWMutex
	buffer map[string]*OhlcvBar
}

func New(
	cfg *config.Config,
	rdb *redis.Client,
	authSvc *kisauth.KisAuthService,
	restClient *utils.KisRestClient,
	log *zap.Logger,
) *ChartService {
	return &ChartService{
		cfg:        cfg,
		rdb:        rdb,
		authSvc:    authSvc,
		restClient: restClient,
		log:        log,
		buffer:     make(map[string]*OhlcvBar),
	}
}

func ohlcvKey(code, tf string) string {
	return fmt.Sprintf("market:ohlcv:%s:%s", tf, code)
}

func (s *ChartService) GetPeriodChartData(ctx context.Context, stockCode, period, startDate, endDate string) ([]ChartData, error) {
	token, err := s.authSvc.GetAccessToken(ctx)
	if err != nil || token == "" {
		return nil, fmt.Errorf("KIS REST API Token is missing: %w", err)
	}

	endpoint := "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
	url := fmt.Sprintf("%s%s?FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD=%s&FID_INPUT_DATE_1=%s&FID_INPUT_DATE_2=%s&FID_PERIOD_DIV_CODE=%s&FID_ORG_ADJ_PRC=0",
		s.cfg.External.KisBaseURL, endpoint,
		stockCode, startDate, endDate, period)

	headers := map[string]string{
		"content-type":  "application/json; charset=utf-8",
		"authorization": "Bearer " + token,
		"appkey":        s.cfg.External.KisAppKey,
		"appsecret":     s.cfg.External.KisAppSecret,
		"tr_id":         "FHKST03010100",
		"custtype":      "P",
	}

	data, err := s.restClient.FetchKisAPI(ctx, url, headers)
	if err != nil {
		return nil, fmt.Errorf("KIS API 호출 실패: %w", err)
	}	

	if rtCd, _ := data["rt_cd"].(string); rtCd != "0" {
		msg, _ := data["msg1"].(string)
		return nil, fmt.Errorf("KIS API Error: %s", msg)
	}

	output2, ok := data["output2"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("KIS API: output2 배열 없음")
	}


	chartList := make([]ChartData, 0, len(output2))

	loc, _ := time.LoadLocation("Asia/Seoul")
	if loc == nil {
		loc = time.FixedZone("KST", 9*60*60)
	}

	for _, item := range output2 {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		dateRaw := strVal(m, "stck_bsop_date")
		parsedTime, err := time.ParseInLocation("20060102", dateRaw, loc)

		var finalDate string
		if err == nil {
			// 3. 파싱된 시간을 표준 UTC로 변환한 뒤 YYYY-MM-DD 형식으로 포맷팅
			// 일봉 차트에서 lightweight-charts는 'YYYY-MM-DD' 형식을 매우 잘 인식합니다.
			finalDate = parsedTime.Format("2006-01-02")
		} else {
			finalDate = dateRaw // 파싱 실패 시 원본이라도 유지
		}

		chartList = append(chartList, ChartData{
			Date:   finalDate,
			Open:   parseFloat(strVal(m, "stck_oprc")),
			High:   parseFloat(strVal(m, "stck_hgpr")),
			Low:    parseFloat(strVal(m, "stck_lwpr")),
			Close:  parseFloat(strVal(m, "stck_clpr")),
			Volume: parseFloat(strVal(m, "acml_vol")),
		})
	}

	// KIS returns newest-first → reverse to ascending order
	reverseChartData(chartList)
	return chartList, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Intraday OHLCV  — mirrors getOhlcv
// ─────────────────────────────────────────────────────────────────────────────

var minuteRegex = regexp.MustCompile(`^(\d+)m$`)

// GetOhlcv retrieves aggregated intraday bars from Redis.
// timeframe examples: "1m", "5m", "10m", "30m".
func (s *ChartService) GetOhlcv(ctx context.Context, stockCode, timeframe string, limit int) ([]OhlcvEntry, error) {
	if stockCode == "" {
		return nil, nil
	}

	chunk := 1
	if m := minuteRegex.FindStringSubmatch(timeframe); len(m) == 2 {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			chunk = n
		}
	}

	fetchLimit := int64(limit * chunk)
	key := ohlcvKey(stockCode, "1m")

	rawList, err := s.rdb.LRange(ctx, key, 0, fetchLimit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("Redis LRange failed for %s: %w", key, err)
	}

	// Parse stored bars
	bars := make([]OhlcvBarJSON, 0, len(rawList))
	for _, raw := range rawList {
		var b OhlcvBarJSON
		if err := json.Unmarshal([]byte(raw), &b); err == nil {
			bars = append(bars, b)
		}
	}

	// Reverse to ascending order (Redis stores newest-first via LPUSH)
	reverseBarJSON(bars)

	if chunk == 1 {
		entries := formatBarsForFrontend(bars)
		if len(entries) > limit {
			entries = entries[len(entries)-limit:]
		}
		return entries, nil
	}

	// ── N-minute aggregation (mirrors the JS loop exactly) ──────────────────
	aggregated := make([]OhlcvBarJSON, 0)
	var currentBar *OhlcvBarJSON
	count := 0

	for _, bar := range bars {
		b := bar // copy
		if currentBar == nil {
			currentBar = &b
			count = 1
		} else {
			if b.H > currentBar.H {
				currentBar.H = b.H
			}
			if b.L < currentBar.L {
				currentBar.L = b.L
			}
			currentBar.C = b.C  // 종가 갱신
			currentBar.V += b.V // 거래량 누적
			count++
		}

		if count == chunk {
			aggregated = append(aggregated, *currentBar)
			currentBar = nil
			count = 0
		}
	}
	if currentBar != nil {
		aggregated = append(aggregated, *currentBar)
	}

	entries := formatBarsForFrontend(aggregated)
	if len(entries) > limit {
		entries = entries[len(entries)-limit:]
	}
	return entries, nil
}

// formatBarsForFrontend adds Unix-ms timestamps (KST) to each bar.
// Mirrors the inner formatBarsForFrontend closure in chartService.js.
func formatBarsForFrontend(bars []OhlcvBarJSON) []OhlcvEntry {
	entries := make([]OhlcvEntry, 0, len(bars))
	for _, bar := range bars {
		t := bar.T
		if len(t) < 12 {
			continue
		}
		y, _ := strconv.Atoi(t[0:4])
		mo, _ := strconv.Atoi(t[4:6])
		d, _ := strconv.Atoi(t[6:8])
		h, _ := strconv.Atoi(t[8:10])
		mi, _ := strconv.Atoi(t[10:12])

		ts := time.Date(y, time.Month(mo), d, h, mi, 0, 0, time.UTC).UnixMilli()


		entries = append(entries, OhlcvEntry{
			T: bar.T, O: bar.O, H: bar.H, L: bar.L, C: bar.C, V: bar.V,
			Timestamp: ts,
		})
	}
	return entries
}

// ─────────────────────────────────────────────────────────────────────────────
// OHLCV buffer update  — mirrors updateOhlcvBuffer + flushOhlcvBar
// ─────────────────────────────────────────────────────────────────────────────

// UpdateOhlcvBuffer is called on each price tick. It accumulates into the
// in-memory 1-minute bar and flushes the completed bar to the Redis pipeline.
// The caller must call pipeline.Exec() after this returns.
func (s *ChartService) UpdateOhlcvBuffer(ctx context.Context, stockCode string, price, volume float64, pipe redis.Pipeliner, eventTime time.Time) {
	minute := utils.BuildMinuteKey(eventTime)

	s.bufMu.Lock()
	defer s.bufMu.Unlock()

	bar, exists := s.buffer[stockCode]

	if !exists || bar.Minute != minute {
		if exists && bar != nil {
			s.flushOhlcvBar(ctx, stockCode, bar, pipe)
		}
		// Open new bar
		s.buffer[stockCode] = &OhlcvBar{
			Open:   price,
			High:   price,
			Low:    price,
			Close:  price,
			Volume: 0,
			Minute: minute,
		}
		bar = s.buffer[stockCode]
	} else {
		bar.High = math.Max(bar.High, price)
		bar.Low = math.Min(bar.Low, price)
		bar.Close = price
	}
	bar.Volume += volume
}

// flushOhlcvBar writes a completed 1-minute bar to the Redis pipeline (LPUSH + LTRIM + EXPIRE).
func (s *ChartService) flushOhlcvBar(ctx context.Context, stockCode string, bar *OhlcvBar, pipe redis.Pipeliner) {
	payload, err := json.Marshal(OhlcvBarJSON{
		T: bar.Minute,
		O: bar.Open, H: bar.High, L: bar.Low, C: bar.Close, V: bar.Volume,
	})
	if err != nil {
		s.log.Error("OHLCV bar JSON marshal failed", zap.Error(err))
		return
	}

	key := ohlcvKey(stockCode, "1m")
	pipe.LPush(ctx, key, string(payload))
	pipe.LTrim(ctx, key, 0, 999) // keep latest 1000 bars
	pipe.Expire(ctx, key, time.Duration(s.cfg.Cache.OhlcvTTL)*time.Second)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func reverseChartData(s []ChartData) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func reverseBarJSON(s []OhlcvBarJSON) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func strVal(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		if f, ok := v.(float64); ok {
			return strconv.FormatFloat(f, 'f', -1, 64)
		}
	}
	return ""
}

func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}
