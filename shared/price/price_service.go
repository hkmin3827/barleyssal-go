// Package price implements shared price caching and stock search.
// Mirrors src/shared/price/priceService.js.
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

func priceKey(code string) string { return "market:price:" + code }
func infoKey(code string) string { return "market:info:" + code }
func buyVolKey(code string) string { return "market:buyvol:" + code }

type StockInfo struct {
	StockCode  string   `json:"stockCode"`
	StockName  string   `json:"stockName"` 
	Price      *float64 `json:"price"`
	ChangeRate *float64 `json:"changeRate"`
	Volume     float64  `json:"volume"`
	BuyVolume  float64  `json:"buyVolume"`
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

func (s *PriceService) OnPriceUpdate(ctx context.Context, stockCode string, price float64, changeRate float64, volume int64, eventTime time.Time) error {
	if stockCode == "" || math.IsNaN(price) {
		return nil
	}

	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, infoKey(stockCode), map[string]interface{}{
		"price": price,
		"changeRate": changeRate,
		"volume": volume,
	})

	priceStr := strconv.FormatFloat(price, 'f', -1, 64)
	pipe.Set(ctx, priceKey(stockCode), priceStr, time.Duration(s.cfg.Cache.PriceTTL)*time.Second)

	s.chartSvc.UpdateOhlcvBuffer(ctx, stockCode, price, float64(volume), pipe, eventTime)

	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Warn("onPriceUpdate pipeline failed", zap.String("stockCode", stockCode), zap.Error(err))
		return err
	}

	if err := s.matchEng.CheckLimitOrders(ctx, stockCode, priceStr); err != nil {
		s.log.Warn("checkLimitOrders failed", zap.String("stockCode", stockCode), zap.Error(err))
	}

	s.pnlSvc.OnPriceUpdate(stockCode, price)
	return nil
}

func (s *PriceService) UpdateBuyVolume(ctx context.Context, stockCode string, shnuCntgSmtn float64) {
	if stockCode == "" || shnuCntgSmtn <= 0 {
		return
	}
	s.rdb.Set(ctx, buyVolKey(stockCode), strconv.FormatFloat(shnuCntgSmtn, 'f', -1, 64),
		time.Duration(s.cfg.Cache.PriceTTL)*time.Second)
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

// 종목 목록을 조회하고 정렬
func (s *PriceService) SearchStocks(ctx context.Context, query string, limit int) ([]StockInfo, error) {
	if limit <= 0 {
		limit = 40
	}
	var targetStocks []config.Stock
	q := strings.ToUpper(query)
	for _, st := range config.KoreaStocks {
		if q == "" || strings.Contains(st.Code, q) || strings.Contains(st.Name, q) {
			targetStocks = append(targetStocks, st)
		}
	}
	
	if len(targetStocks) == 0 {
		return []StockInfo{}, nil
	}

	pipe := s.rdb.Pipeline()
	infoCmds := make(map[string]*redis.MapStringStringCmd)
	buyVolCmds := make(map[string]*redis.StringCmd)
	for _, st := range targetStocks {
		infoCmds[st.Code] = pipe.HGetAll(ctx, infoKey(st.Code))
		buyVolCmds[st.Code] = pipe.Get(ctx, buyVolKey(st.Code))
	}
	_, _ = pipe.Exec(ctx)


	results := make([]StockInfo, 0, len(targetStocks))
	for _, st := range targetStocks {
		data := infoCmds[st.Code].Val()

		var p, r *float64
		var v, bv float64

		if len(data) > 0 {
			if pv, err := strconv.ParseFloat(data["price"], 64); err == nil {
				p = &pv
			}
			if rv, err := strconv.ParseFloat(data["changeRate"], 64); err == nil {
				r = &rv
			}
			v, _ = strconv.ParseFloat(data["volume"], 64)
		}

		// Buy volume from dedicated key
		if raw, err := buyVolCmds[st.Code].Result(); err == nil {
			bv, _ = strconv.ParseFloat(raw, 64)
		}

		results = append(results, StockInfo{
			StockCode:  st.Code,
			StockName:  st.Name,
			Price:      p,
			ChangeRate: r,
			Volume:     v,
			BuyVolume:  bv,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Volume > results[j].Volume
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
	// if query != "" {
	// 	keys, err := s.rdb.Keys(ctx, "market:price:*"+strings.ToUpper(query)+"*").Result()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	result := make([]StockInfo, 0)
	// 	for i, k := range keys {
	// 		if i >= limit {
	// 			break
	// 		}
	// 		code := strings.TrimPrefix(k, "market:price:")
	// 		var pp *float64
	// 		if raw, _ := s.rdb.Get(ctx, priceKey(code)).Result(); raw != "" {
	// 			if pv, err := strconv.ParseFloat(raw, 64); err == nil {
	// 				pp = &pv
	// 			}
	// 		}
	// 		result = append(result, StockInfo{StockCode: code, Price: pp})
	// 	}
	// 	return result, nil
	// }

	// pairs, err := s.rdb.ZRevRangeWithScores(ctx, stocksKey, 0, int64(limit-1)).Result()
	// if err != nil {
	// 	return nil, err
	// }
	// result := make([]StockInfo, 0, len(pairs))
	// for _, z := range pairs {
	// 	code, _ := z.Member.(string)
	// 	if code == "" {
	// 		continue
	// 	}
	// 	var pp *float64
	// 	if raw, _ := s.rdb.Get(ctx, priceKey(code)).Result(); raw != "" {
	// 		if pv, err := strconv.ParseFloat(raw, 64); err == nil {
	// 			pp = &pv
	// 		}
	// 	}
	// 	result = append(result, StockInfo{StockCode: code, Price: pp, Volume: z.Score})
	// }
	// return result, nil
}

func (s *PriceService) RegisterStocks(ctx context.Context, codes []string) error {
	if len(codes) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for _, code := range codes {
		if code != "" {
			pipe.HSetNX(ctx, infoKey(code), "volume", 0)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}
