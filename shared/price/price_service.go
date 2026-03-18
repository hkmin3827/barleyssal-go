package price

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"barleyssal-go/config"
	chartapp "barleyssal-go/domains/chart/application"
	matchingapp "barleyssal-go/domains/order_matching/application"
	pnlapp "barleyssal-go/domains/pnl/application"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func priceKey(code string) string  { return "market:price:" + code }
func infoKey(code string) string   { return "market:info:" + code }

const (
	zsetChangeRate = "market:ranking:todayChangeRate" // score: 등락률(%)
	zsetBuyVolume  = "market:ranking:todayBuyVolume"  // score: 매수 누적 체결량
	zsetAcmlVolume     = "market:ranking:acmlVolume"           // score: 누적 거래량
)

type StockInfo struct {
	StockCode  string   `json:"stockCode"`
	StockName  string   `json:"stockName"`
	Price      float64 `json:"price"`
	PrdyVrssSign int64 `json:"prdyVrssSign"`
	PrdyVrss   float64 `json:"prdyVrss"`  
	ChangeRate float64 `json:"changeRate"`
	AcmlVol     float64  `json:"acmlVol"`
}

type RankingItem struct {
	StockCode  string  `json:"stockCode"`
	StockName  string  `json:"stockName"`
	Price      float64 `json:"price"`
	ChangeRate float64 `json:"changeRate"`
	BuyVolPct  float64 `json:"buyVolPct,omitempty"` 
}

type PriceService struct {
	cfg      *config.Config
	rdb      *redis.Client
	chartSvc *chartapp.ChartService
	matchEng *matchingapp.MatchingEngine
	pnlSvc   *pnlapp.PnlService
	log      *zap.Logger
}

func New(
	cfg *config.Config,
	rdb *redis.Client,
	chartSvc *chartapp.ChartService,
	matchEng *matchingapp.MatchingEngine,
	pnlSvc *pnlapp.PnlService,
	log *zap.Logger,
) *PriceService {
	return &PriceService{cfg: cfg, rdb: rdb, chartSvc: chartSvc, matchEng: matchEng, pnlSvc: pnlSvc, log: log}
}


func (s *PriceService) OnPriceUpdate(
		ctx context.Context,
		stockCode string,
		price float64,
		changeRate float64,
		volume int64,
		prdyVrssSign int64,
		prdyVrss float64,
		stckOprc float64,
		stckHgpr float64,
		stckLwpr float64,
		acmlVol int64,
		eventTime time.Time,
		mkopCode string,
	) error {
	if stockCode == "" || math.IsNaN(price) || price <= 0 {
		return nil
	}

	pipe := s.rdb.Pipeline()

	priceStr := strconv.FormatFloat(price, 'f', -1, 64)  // 매칭엔진용

	pipe.Set(ctx, priceKey(stockCode), priceStr, time.Duration(s.cfg.Cache.PriceTTL)*time.Second)


	pipe.HSet(ctx, infoKey(stockCode), map[string]interface{}{
		"price":      price,
		"changeRate": changeRate,
		"prdyVrssSign": prdyVrssSign,
		"prdyVrss":   prdyVrss,    // 전일 대비 (원)
		"stckOprc":   stckOprc,    // 시가
		"stckHgpr":   stckHgpr,    // 고가
		"stckLwpr":   stckLwpr,    // 저가
		"acmlVol":    acmlVol,     // 누적 거래량
		"volume":     volume,      // 단일 체결량 (chartSvc 호환 유지)
		"mKopCode":   mkopCode,    // 신 장운영 구분 코드
	})
	pipe.Expire(ctx, infoKey(stockCode), time.Duration(s.cfg.Cache.InfoTTL)*time.Second)
 

	pipe.ZAdd(ctx, zsetChangeRate, redis.Z{Score: changeRate, Member: stockCode})
	pipe.ZAdd(ctx, zsetAcmlVolume, redis.Z{Score: float64(acmlVol), Member: stockCode})

	s.chartSvc.UpdateOhlcvBuffer(ctx, stockCode, price, float64(volume), pipe, eventTime)

	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Warn("price pipeline exec failed", zap.String("code", stockCode), zap.Error(err))
		return err
	}

	if err := s.matchEng.CheckLimitOrders(ctx, stockCode, priceStr); err != nil {
		s.log.Warn("checkLimitOrders failed", zap.String("code", stockCode), zap.Error(err))
	}

	s.pnlSvc.OnPriceUpdate(stockCode, price)

	return nil
}

func (s *PriceService) UpdateBuyVolume(ctx context.Context, stockCode string, shnuCntgSmtn float64) {
	if stockCode == "" || shnuCntgSmtn <= 0 {
		return
	}
	if err := s.rdb.ZAdd(ctx, zsetBuyVolume, redis.Z{
		Score:  shnuCntgSmtn,
		Member: stockCode,
	}).Err(); err != nil {
		s.log.Warn("ZAdd buyVolume failed", zap.String("code", stockCode), zap.Error(err))
	}
}

func (s *PriceService) GetTopChangeRate(ctx context.Context, topN int) ([]RankingItem, error) {
	return s.getTopFromZSet(ctx, zsetChangeRate, topN, false)
}


func (s *PriceService) GetTopBuyVolume(ctx context.Context, topN int) ([]RankingItem, error) {
	results, err := s.rdb.ZRevRangeWithScores(ctx, zsetBuyVolume, 0, int64(topN-1)).Result()
	if err != nil || len(results) == 0 {
		return nil, err
	}
 
	allScores, err := s.rdb.ZRangeWithScores(ctx, zsetBuyVolume, 0, -1).Result()
	var total float64
	if err == nil {
		for _, z := range allScores {
			total += z.Score
		}
	}
 
	codes := make([]string, len(results))
	for i, z := range results {
		codes[i] = z.Member.(string)
	}
	items, err := s.fetchInfoBatch(ctx, codes)
	if err != nil {
		return nil, err
	}
 
	for i := range items {
		if total > 0 {
			items[i].BuyVolPct = (results[i].Score / total) * 100
		}
	}
	return items, nil
}
 
func (s *PriceService) GetTopVolume(ctx context.Context, topN int) ([]StockInfo, error) {
	return s.getSortedStocksFull(ctx, zsetAcmlVolume)
}
 

func (s *PriceService) GetSortedStocks(ctx context.Context, sortKey string) ([]StockInfo, error) {
	switch sortKey {
	case "changeRate":
		return s.getSortedStocksFull(ctx, zsetChangeRate)
	case "acmlVol":
		return s.getSortedStocksFull(ctx, zsetAcmlVolume)
	case "name":
		return s.getSortedByName(ctx)
	default:
		return nil, fmt.Errorf("unsupported sort key: %s", sortKey)
	}
}

func (s *PriceService) getSortedByName(ctx context.Context) ([]StockInfo, error) {
	codes := make([]string, 0, len(config.KoreaStocks))
	for _, st := range config.KoreaStocks {
		if st.Code != "" {
			codes = append(codes, st.Code)
		}
	}
	items, err := s.fetchStockInfoBatch(ctx, codes)
	if err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].StockName < items[j].StockName
	})
	return items, nil
}

func (s *PriceService) getSortedStocksFull(ctx context.Context, zsetKey string) ([]StockInfo, error) {
	codes, err := s.rdb.ZRevRange(ctx, zsetKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("ZREVRANGE %s: %w", zsetKey, err)
	}
	if len(codes) == 0 {
		return []StockInfo{}, nil
	}
	return s.fetchStockInfoBatch(ctx, codes)
}

func (s *PriceService) fetchStockInfoBatch(ctx context.Context, codes []string) ([]StockInfo, error) {
	nameMap := buildNameMap()
 
	pipe := s.rdb.Pipeline()
	fields := []string{"price", "changeRate", "prdyVrssSign", "prdyVrss", "acmlVol"}
	cmds := make([]*redis.SliceCmd, len(codes))
	for i, code := range codes {
		cmds[i] = pipe.HMGet(ctx, infoKey(code), fields...)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("pipeline HMGET: %w", err)
	}
 
	results := make([]StockInfo, 0, len(codes))
	for i, code := range codes {
		vals := cmds[i].Val()
		item := StockInfo{
			StockCode: code,
			StockName: nameMap[code],
		}
		if len(vals) >= 5 {
			item.Price = parseF64(vals[0])
			item.ChangeRate = parseF64(vals[1])
			item.PrdyVrssSign = parseInt64(vals[2])
			item.PrdyVrss = parseF64(vals[3])
			item.AcmlVol = parseF64(vals[4])
		}
		results = append(results, item)
	}
	return results, nil
}


func (s *PriceService) fetchInfoBatch(ctx context.Context, codes []string) ([]RankingItem, error) {
	nameMap := buildNameMap()
	pipe := s.rdb.Pipeline()
	fields := []string{"price", "changeRate"}
	cmds := make([]*redis.SliceCmd, len(codes))
	for i, code := range codes {
		cmds[i] = pipe.HMGet(ctx, infoKey(code), fields...)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
 
	items := make([]RankingItem, 0, len(codes))
	for i, code := range codes {
		vals := cmds[i].Val()
		item := RankingItem{StockCode: code, StockName: nameMap[code]}
		if len(vals) >= 2 {
			item.Price = parseF64(vals[0])
			item.ChangeRate = parseF64(vals[1])
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *PriceService) getTopFromZSet(ctx context.Context, zsetKey string, topN int, _ bool) ([]RankingItem, error) {
	codes, err := s.rdb.ZRevRange(ctx, zsetKey, 0, int64(topN-1)).Result()
	if err != nil || len(codes) == 0 {
		return nil, err
	}
	return s.fetchInfoBatch(ctx, codes)
}



func (s *PriceService) RegisterStocks(ctx context.Context, codes []string) error {
	if len(codes) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for _, code := range codes {
		if code != "" {
			pipe.HSetNX(ctx, infoKey(code), "acmlVol", 0)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *PriceService) GetStocksBatch(ctx context.Context, codes []string) ([]StockInfo, error) {
	if len(codes) == 0 {
		return []StockInfo{}, nil
	}
	return s.fetchStockInfoBatch(ctx, codes)
}
 

func (s *PriceService) GetCurrentPrice(ctx context.Context, stockCode string) (*float64, error) {
	if stockCode == "" {
		return nil, nil
	}
	val, err := s.rdb.Get(ctx, priceKey(stockCode)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get price: %w", err)
	}
	p, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, nil
	}
	return &p, nil
}


func (s *PriceService) GetStockInfo(ctx context.Context, stockCode string) (map[string]float64, string, error) {
	numFields := []string{"price", "changeRate", "prdyVrssSign", "prdyVrss", "stckOprc", "stckHgpr", "stckLwpr", "acmlVol", "cntgVol"}
	allFields := append(numFields, "mKopCode")
	vals, err := s.rdb.HMGet(ctx, infoKey(stockCode), allFields...).Result()
	if err != nil {
		return nil, "", err
	}
	result := make(map[string]float64, len(numFields))
	for i, f := range numFields {
		result[f] = parseF64(vals[i])
	}
	mkopCode := ""
	if len(vals) > len(numFields) && vals[len(numFields)] != nil {
		mkopCode, _ = vals[len(numFields)].(string)
	}
	return result, mkopCode, nil
}

func (s *PriceService) SearchStocks(ctx context.Context, query string, limit int) ([]StockInfo, error) {
	if limit <= 0 {
		limit = 40
	}

	q := strings.ToUpper(query)
	var targets []config.Stock


	for _, st := range config.KoreaStocks {
		if q == "" || strings.Contains(st.Code, q) || strings.Contains(strings.ToUpper(st.Name), q) {
			targets = append(targets, st)
		}
	}
	if len(targets) == 0 {
		return []StockInfo{}, nil
	}

	pipe := s.rdb.Pipeline()
	fields := []string{"price", "changeRate", "prdyVrss", "acmlVol"}
	infoCmds := make([]*redis.SliceCmd, len(targets))
	for i, st := range targets {
		infoCmds[i] = pipe.HMGet(ctx, infoKey(st.Code), fields...)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("SearchStocks pipeline: %w", err)
	}

	results := make([]StockInfo, 0, len(targets))
	for i, st := range targets {
		vals := infoCmds[i].Val()
		item := StockInfo{
			StockCode: st.Code,
			StockName: st.Name,
		}
		if len(vals) >= 4 {
			item.Price = parseF64(vals[0])
			item.ChangeRate = parseF64(vals[1])
			item.PrdyVrss = parseF64(vals[2])
			item.AcmlVol = parseF64(vals[3])
		}
		results = append(results, item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].AcmlVol > results[j].AcmlVol
	})

	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

var stockNameMap map[string]string
 


func init() {
	stockNameMap = make(map[string]string, len(config.KoreaStocks))
	for _, s := range config.KoreaStocks {
		stockNameMap[s.Code] = s.Name
	}
}

func buildNameMap() map[string]string {
	return stockNameMap
}

func parseF64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	s, ok := v.(string)
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
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