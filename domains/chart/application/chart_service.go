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
	kisrest "barleyssal-go/shared/infrastructure/kis_rest"
	"barleyssal-go/shared/ports"
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

type OhlcvBarJSON struct {
	T string  `json:"t"` // YYYYMMDDHHmm (UTC)
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

type barBuf struct {
	open   float64
	high   float64
	low    float64
	close  float64
	volume float64 
	minute string  // YYYYMMDDHHmm (UTC)
}

type ChartService struct {
	cfg        *config.Config
	rdb        *redis.Client
	authSvc    *kisauth.KisAuthService
	restClient *kisrest.KisRestClient
	log        *zap.Logger
	notifier ports.UserNotifier

	mu     sync.Mutex
	buffer map[string]*barBuf 
}

func New(
	cfg *config.Config,
	rdb *redis.Client,
	authSvc *kisauth.KisAuthService,
	restClient *kisrest.KisRestClient,
	notifier ports.UserNotifier,
	log *zap.Logger,
) *ChartService {
	return &ChartService{
		cfg:        cfg,
		rdb:        rdb,
		authSvc:    authSvc,
		restClient: restClient,
		notifier: notifier,
		log:        log,
		buffer:     make(map[string]*barBuf),
	}
}

func (s *ChartService) UpdateOhlcvBuffer(ctx context.Context, code string, price, cntgVol float64, pipe redis.Pipeliner, eventTime time.Time) ports.BarEvent {
	minute := utils.BuildMinuteKey(eventTime)

	snapshot := s.applyTick(ctx, code, price, cntgVol, minute, pipe)

	bar := ports.BarEvent{T: snapshot.minute, O: snapshot.open, H: snapshot.high, L: snapshot.low, C: snapshot.close, V: snapshot.volume}
  payload, _ := json.Marshal(bar)

	pipe.Set(ctx, "market:ohlcv:live:"+code, string(payload), 2*time.Minute)

	return bar
}

func (s *ChartService) applyTick(ctx context.Context, code string, price, cntgVol float64, minute string, pipe redis.Pipeliner) barBuf {
	s.mu.Lock()
	defer s.mu.Unlock()

	bar, exists := s.buffer[code]

	if !exists || bar.minute != minute {
		if exists && bar != nil {
			s.pushCompletedBar(ctx, code, bar, pipe)
		}
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
		if price > bar.high {
			bar.high = price
		}
		if price < bar.low {
			bar.low = price
		}
		bar.close = price
	}

	bar.volume += cntgVol // 단일 체결량(cntgVol)만 누적!

	return *bar 
}

func (s *ChartService) StartFlusher(ctx context.Context) {
	go func() {
		now := time.Now().UTC()
		nextMinute := now.Truncate(time.Minute).Add(time.Minute)
		select {
		case <-time.After(time.Until(nextMinute)):
		case <-ctx.Done():
			return
		}

		s.flushExpiredBars(ctx)

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.flushExpiredBars(ctx)
			}
		}
	}()
	s.log.Info("OHLCV 주기적 플러셔 시작 (KIS WS 연결 연동)")
}

func (s *ChartService) flushExpiredBars(ctx context.Context) {
	currentMinute := utils.BuildMinuteKey(time.Now().UTC())

	s.mu.Lock()
	toFlush := make(map[string]*barBuf)
	for code, bar := range s.buffer {
		if bar.minute < currentMinute {
			toFlush[code] = bar
			delete(s.buffer, code)
		}
	}
	s.mu.Unlock()

	if len(toFlush) == 0 {
		return
	}

	pipe := s.rdb.Pipeline()
	for code, bar := range toFlush {
		s.pushCompletedBar(ctx, code, bar, pipe)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Warn("flushExpiredBars pipeline 실패", zap.Error(err))
		return
	}
	s.log.Info("OHLCV 주기 플러시 완료", zap.Int("codes", len(toFlush)), zap.String("minute", currentMinute))
}

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
	pipe.LTrim(ctx, key, 0, 2999) 
	pipe.Expire(ctx, key, time.Duration(s.cfg.Cache.OhlcvTTL)*time.Second)

	 if s.notifier != nil {
        s.notifier.BroadcastBarComplete(code, ports.BarEvent{
            T: bar.minute, O: bar.open, H: bar.high,
            L: bar.low,  C: bar.close, V: bar.volume,
        })
    }
}

var minuteRegex = regexp.MustCompile(`^(\d+)m$`)

func (s *ChartService) GetOhlcv(ctx context.Context, code, timeframe string, limit int) ([]OhlcvEntry, error) {
	if code == "" {
		return nil, nil
	}

	chunk := 1
	if m := minuteRegex.FindStringSubmatch(timeframe); len(m) == 2 {
		if n, _ := strconv.Atoi(m[1]); n > 0 {
			chunk = n
		}
	}

	rawList, err := s.rdb.LRange(ctx, "market:ohlcv:1m:"+code, 0, int64(limit*chunk)-1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis LRange %s: %w", code, err)
	}

	bars := make([]OhlcvBarJSON, 0, len(rawList)+1)
	for _, raw := range rawList {
		var b OhlcvBarJSON
		if json.Unmarshal([]byte(raw), &b) == nil {
			bars = append(bars, b)
		}
	}

	if liveRaw, err := s.rdb.Get(ctx, "market:ohlcv:live:"+code).Result(); err == nil {
		var live OhlcvBarJSON
		if json.Unmarshal([]byte(liveRaw), &live) == nil {
			if len(bars) == 0 || bars[0].T != live.T {
				bars = append([]OhlcvBarJSON{live}, bars...)
			}
		}
	}

	reverseSlice(bars)

	if chunk > 1 {
		bars = aggregateBars(bars, chunk)
	}

	if len(bars) > limit {
		bars = bars[len(bars)-limit:]
	}

	return toEntries(bars), nil
}

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
			cur.C = b.C 
			cur.V += b.V 
			cnt++
		}
		if cnt == chunk {
			out = append(out, *cur)
			cur = nil
			cnt = 0
		}
	}
	if cur != nil {
		out = append(out, *cur)
	}
	return out
}

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
			finalDate = t.Format("2006-01-02")
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

	reverseSlice(list) 
	return list, nil
}



func reverseSlice[T any](s []T) {
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