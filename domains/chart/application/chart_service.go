// Package chartapplication handles real-time OHLCV bar building and chart data retrieval.
package chartapplication

import (
	"context"
	"encoding/json"
	"fmt"
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

// ──────────────────────────────────────────────────────────────────────────────
// 타입 정의
// ──────────────────────────────────────────────────────────────────────────────

// ChartData: 일/주/월봉 API 응답 구조
type ChartData struct {
	Date   string  `json:"date"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

// OhlcvBarJSON: Redis에 직렬화되는 1분봉 구조
// Key: market:ohlcv:1m:{code} (List), market:ohlcv:live:{code} (String)
type OhlcvBarJSON struct {
	T string  `json:"t"` // YYYYMMDDHHmm (UTC)
	O float64 `json:"o"`
	H float64 `json:"h"`
	L float64 `json:"l"`
	C float64 `json:"c"`
	V float64 `json:"v"`
}

// OhlcvEntry: 프론트엔드에 반환하는 분봉 (Unix ms timestamp 포함)
type OhlcvEntry struct {
	T         string  `json:"t"`
	O         float64 `json:"o"`
	H         float64 `json:"h"`
	L         float64 `json:"l"`
	C         float64 `json:"c"`
	V         float64 `json:"v"`
	Timestamp int64   `json:"timestamp"`
}

// barBuf: 메모리 내 진행 중인 1분봉 (unexported)
type barBuf struct {
	open   float64
	high   float64
	low    float64
	close  float64
	volume float64 // cntgVol(단일 체결량)만 누적
	minute string  // YYYYMMDDHHmm (UTC)
}

// ──────────────────────────────────────────────────────────────────────────────
// ChartService
// ──────────────────────────────────────────────────────────────────────────────

type ChartService struct {
	cfg        *config.Config
	rdb        *redis.Client
	authSvc    *kisauth.KisAuthService
	restClient *utils.KisRestClient
	log        *zap.Logger

	mu     sync.Mutex
	buffer map[string]*barBuf // stockCode → 진행 중인 1분봉
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
		buffer:     make(map[string]*barBuf),
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// 핵심: 틱 → 메모리 buffer 갱신 + Redis pipeline 명령 추가
// ──────────────────────────────────────────────────────────────────────────────

// UpdateOhlcvBuffer는 틱이 들어올 때 호출된다.
//   - cntgVol: 단일 체결량 (acmlVol 당일 누적 거래량 절대 사용 금지!)
//   - pipe: 호출자(PriceService)가 exec할 Redis pipeline
//
// 분이 바뀌면 이전 봉을 pipeline에 LPUSH 추가하고 새 봉을 시작한다.
func (s *ChartService) UpdateOhlcvBuffer(ctx context.Context, code string, price, cntgVol float64, pipe redis.Pipeliner, eventTime time.Time) {
	minute := utils.BuildMinuteKey(eventTime) // YYYYMMDDHHmm

	// applyTick은 mutex 안에서만 buffer 조작 후 값 복사본을 반환.
	// unlock 후 복사본을 안전하게 직렬화할 수 있다.
	snapshot := s.applyTick(ctx, code, price, cntgVol, minute, pipe)

	// Live bar를 Redis에 덮어쓰기 (key: market:ohlcv:live:{code}, TTL 2분)
	payload, _ := json.Marshal(OhlcvBarJSON{
		T: snapshot.minute,
		O: snapshot.open,
		H: snapshot.high,
		L: snapshot.low,
		C: snapshot.close,
		V: snapshot.volume,
	})
	pipe.Set(ctx, "market:ohlcv:live:"+code, string(payload), 2*time.Minute)
}

// applyTick: mutex 보호 하에 메모리 버퍼를 갱신하고 값 복사본을 반환.
// 분이 바뀌면 이전 봉을 pushCompletedBar로 pipeline에 LPUSH 예약.
//
// [BUG FIX] defer unlock 하나만 사용 → 이중 unlock(panic) 방지
func (s *ChartService) applyTick(ctx context.Context, code string, price, cntgVol float64, minute string, pipe redis.Pipeliner) barBuf {
	s.mu.Lock()
	defer s.mu.Unlock()

	bar, exists := s.buffer[code]

	if !exists || bar.minute != minute {
		// 분이 바뀌었으면 이전 봉 완성 → pipeline에 LPUSH 예약
		if exists && bar != nil {
			s.pushCompletedBar(ctx, code, bar, pipe)
		}
		// 새 1분봉 시작 (volume은 0으로 초기화, 이번 틱의 cntgVol은 아래에서 누적)
		bar = &barBuf{
			open:   price,
			high:   price,
			low:    price,
			close:  price,
			volume: 0,
			minute: minute,
		}
		s.buffer[code] = bar
	} else {
		// 같은 분: H/L/C 갱신
		if price > bar.high {
			bar.high = price
		}
		if price < bar.low {
			bar.low = price
		}
		bar.close = price
	}

	bar.volume += cntgVol // 단일 체결량(cntgVol)만 누적!

	return *bar // 값 복사본 반환 → unlock 후 안전하게 사용
}

// pushCompletedBar: 완성된 1분봉을 Redis List에 LPUSH (newest-first, 최대 1000개 유지)
// Key: market:ohlcv:1m:{code}
func (s *ChartService) pushCompletedBar(ctx context.Context, code string, bar *barBuf, pipe redis.Pipeliner) {
	payload, err := json.Marshal(OhlcvBarJSON{
		T: bar.minute,
		O: bar.open,
		H: bar.high,
		L: bar.low,
		C: bar.close,
		V: bar.volume,
	})
	if err != nil {
		s.log.Error("OHLCV bar marshal failed", zap.String("code", code), zap.Error(err))
		return
	}

	key := "market:ohlcv:1m:" + code
	pipe.LPush(ctx, key, string(payload))
	pipe.LTrim(ctx, key, 0, 999) // 최근 1000개 유지
	pipe.Expire(ctx, key, time.Duration(s.cfg.Cache.OhlcvTTL)*time.Second)
}

// ──────────────────────────────────────────────────────────────────────────────
// REST API: 분봉 조회 + In-Memory N분봉 Aggregation
// ──────────────────────────────────────────────────────────────────────────────

var minuteRegex = regexp.MustCompile(`^(\d+)m$`)

// GetOhlcv: 완성된 1분봉 list + 진행 중인 live bar를 조합하여 반환.
//   - 5m/10m/30m: Redis에 별도 저장 없이 1분봉 배열을 Go 메모리에서 aggregation.
//   - Live bar는 배열 끝(최신)에 끼워 넣어 반환.
func (s *ChartService) GetOhlcv(ctx context.Context, code, timeframe string, limit int) ([]OhlcvEntry, error) {
	if code == "" {
		return nil, nil
	}

	// timeframe → chunk 크기 (1m=1, 5m=5, 10m=10, 30m=30)
	chunk := 1
	if m := minuteRegex.FindStringSubmatch(timeframe); len(m) == 2 {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			chunk = n
		}
	}

	// aggregation을 위해 limit*chunk개의 1분봉 조회
	rawList, err := s.rdb.LRange(ctx, "market:ohlcv:1m:"+code, 0, int64(limit*chunk)-1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis LRange %s: %w", code, err)
	}

	// JSON 파싱 (Redis: LPUSH → newest-first)
	bars := make([]OhlcvBarJSON, 0, len(rawList)+1)
	for _, raw := range rawList {
		var b OhlcvBarJSON
		if json.Unmarshal([]byte(raw), &b) == nil {
			bars = append(bars, b)
		}
	}

	// Live bar 추가: 아직 완성 안 된 현재 분봉을 맨 앞(newest)에 추가.
	// bars[0]과 같은 시간이면 live로 덮어씌우기 (rollover 직후 찰나의 중복 방지)
	if liveRaw, err := s.rdb.Get(ctx, "market:ohlcv:live:"+code).Result(); err == nil {
		var live OhlcvBarJSON
		if json.Unmarshal([]byte(liveRaw), &live) == nil {
			if len(bars) == 0 || bars[0].T != live.T {
				bars = append([]OhlcvBarJSON{live}, bars...)
			}
			// 같은 시간이면 list[0]이 이미 live와 중복 → 무시
		}
	}

	// ascending 정렬 (LPUSH = newest-first → reverse)
	reverseBarJSON(bars)

	// N분봉 aggregation (5m/10m/30m)
	if chunk > 1 {
		bars = aggregateBars(bars, chunk)
	}

	// limit 적용 (최신 limit개)
	if len(bars) > limit {
		bars = bars[len(bars)-limit:]
	}

	return toEntries(bars), nil
}

// aggregateBars: 1분봉 배열(ascending)을 chunk개 단위로 묶어 N분봉 생성.
// 마지막 미완성 chunk(live bar 포함)도 그대로 포함.
func aggregateBars(bars []OhlcvBarJSON, chunk int) []OhlcvBarJSON {
	out := make([]OhlcvBarJSON, 0, len(bars)/chunk+1)
	var cur *OhlcvBarJSON
	cnt := 0

	for i := range bars {
		b := bars[i]
		if cur == nil {
			cp := b
			cur = &cp
			cnt = 1
		} else {
			if b.H > cur.H {
				cur.H = b.H
			}
			if b.L < cur.L {
				cur.L = b.L
			}
			cur.C = b.C  // 종가 갱신
			cur.V += b.V // 거래량 누적
			cnt++
		}
		if cnt == chunk {
			out = append(out, *cur)
			cur = nil
			cnt = 0
		}
	}
	// 나머지 미완성 chunk (마지막 live bar 포함)
	if cur != nil {
		out = append(out, *cur)
	}
	return out
}

// toEntries: OhlcvBarJSON 배열 → OhlcvEntry (Unix ms timestamp 추가)
func toEntries(bars []OhlcvBarJSON) []OhlcvEntry {
	entries := make([]OhlcvEntry, 0, len(bars))
	for _, b := range bars {
		ts := barTimeToMs(b.T)
		if ts == 0 {
			continue
		}
		entries = append(entries, OhlcvEntry{
			T: b.T, O: b.O, H: b.H, L: b.L, C: b.C, V: b.V,
			Timestamp: ts,
		})
	}
	return entries
}

// barTimeToMs: "YYYYMMDDHHmm" → Unix milliseconds (UTC)
func barTimeToMs(t string) int64 {
	if len(t) < 12 {
		return 0
	}
	y, _ := strconv.Atoi(t[0:4])
	mo, _ := strconv.Atoi(t[4:6])
	d, _ := strconv.Atoi(t[6:8])
	h, _ := strconv.Atoi(t[8:10])
	mi, _ := strconv.Atoi(t[10:12])
	return time.Date(y, time.Month(mo), d, h, mi, 0, 0, time.UTC).UnixMilli()
}

// ──────────────────────────────────────────────────────────────────────────────
// 일/주/월봉 차트 (KIS REST API)
// ──────────────────────────────────────────────────────────────────────────────

func (s *ChartService) GetPeriodChartData(ctx context.Context, stockCode, period, startDate, endDate string) ([]ChartData, error) {
	token, err := s.authSvc.GetAccessToken(ctx)
	if err != nil || token == "" {
		return nil, fmt.Errorf("KIS REST API Token missing: %w", err)
	}

	url := fmt.Sprintf(
		"%s/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice?"+
			"FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD=%s&FID_INPUT_DATE_1=%s&FID_INPUT_DATE_2=%s&FID_PERIOD_DIV_CODE=%s&FID_ORG_ADJ_PRC=0",
		s.cfg.External.KisBaseURL, stockCode, startDate, endDate, period,
	)
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
		return nil, fmt.Errorf("KIS API: output2 없음")
	}

	loc, _ := time.LoadLocation("Asia/Seoul")
	if loc == nil {
		loc = time.FixedZone("KST", 9*60*60)
	}

	list := make([]ChartData, 0, len(output2))
	for _, item := range output2 {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		dateRaw := strVal(m, "stck_bsop_date")
		finalDate := dateRaw
		if t, err := time.ParseInLocation("20060102", dateRaw, loc); err == nil {
			finalDate = t.Format("2006-01-02") // lightweight-charts 호환 형식
		}
		list = append(list, ChartData{
			Date:   finalDate,
			Open:   parseFloat(strVal(m, "stck_oprc")),
			High:   parseFloat(strVal(m, "stck_hgpr")),
			Low:    parseFloat(strVal(m, "stck_lwpr")),
			Close:  parseFloat(strVal(m, "stck_clpr")),
			Volume: parseFloat(strVal(m, "acml_vol")),
		})
	}

	reverseChartData(list) // KIS: newest-first → ascending
	return list, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// 헬퍼
// ──────────────────────────────────────────────────────────────────────────────

func reverseBarJSON(s []OhlcvBarJSON) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func reverseChartData(s []ChartData) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func strVal(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		switch t := v.(type) {
		case string:
			return t
		case float64:
			return strconv.FormatFloat(t, 'f', -1, 64)
		}
	}
	return ""
}

func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
