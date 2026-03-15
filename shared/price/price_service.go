// Package price implements shared price caching and stock search.
// Mirrors src/shared/price/priceService.js.
package price

import (
	"context"
	"fmt"
	"math"
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

const stocksKey = "market:symbols"

func priceKey(code string) string { return "market:price:" + code }

// StockInfo is returned by SearchStocks / GetCurrentPrice.
type StockInfo struct {
	StockCode string   `json:"stockCode"`
	Price     *float64 `json:"price"`
	Volume    float64  `json:"volume"`
}

// PriceService orchestrates price caching, OHLCV buffering, limit-order checking
// and PnL notifications on each incoming tick.
type PriceService struct {
	cfg      *config.Config
	rdb      *redis.Client
	chartSvc *chartapp.ChartService
	matchEng *matchingapp.MatchingEngine
	pnlSvc   *pnlapp.PnlService
	log      *zap.Logger
}

// New creates a new PriceService.
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

// OnPriceUpdate is called by the external KIS client on every price tick.
func (s *PriceService) OnPriceUpdate(ctx context.Context, stockCode string, price float64, volume int64) error {
	if stockCode == "" || math.IsNaN(price) {
		return nil
	}

	pipe := s.rdb.Pipeline()

	pipe.Set(ctx, priceKey(stockCode), strconv.FormatFloat(price, 'f', -1, 64),
		time.Duration(s.cfg.Cache.PriceTTL)*time.Second)
	pipe.ZIncrBy(ctx, stocksKey, float64(volume), stockCode)
	s.chartSvc.UpdateOhlcvBuffer(ctx, stockCode, price, float64(volume), pipe)

	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Warn("onPriceUpdate pipeline failed", zap.String("stockCode", stockCode), zap.Error(err))
		return err
	}

	priceStr := strconv.FormatFloat(price, 'f', -1, 64)
	if err := s.matchEng.CheckLimitOrders(ctx, stockCode, priceStr); err != nil {
		s.log.Warn("checkLimitOrders failed", zap.String("stockCode", stockCode), zap.Error(err))
	}

	s.pnlSvc.OnPriceUpdate(stockCode, price)
	return nil
}

// GetCurrentPrice returns the cached price for a stock code, or nil if not found.
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

// SearchStocks returns stocks matching the query ordered by volume.
func (s *PriceService) SearchStocks(ctx context.Context, query string, limit int) ([]StockInfo, error) {
	if limit <= 0 {
		limit = 40
	}
	if query != "" {
		keys, err := s.rdb.Keys(ctx, "market:price:*"+strings.ToUpper(query)+"*").Result()
		if err != nil {
			return nil, err
		}
		result := make([]StockInfo, 0)
		for i, k := range keys {
			if i >= limit {
				break
			}
			code := strings.TrimPrefix(k, "market:price:")
			var pp *float64
			if raw, _ := s.rdb.Get(ctx, priceKey(code)).Result(); raw != "" {
				if pv, err := strconv.ParseFloat(raw, 64); err == nil {
					pp = &pv
				}
			}
			result = append(result, StockInfo{StockCode: code, Price: pp})
		}
		return result, nil
	}

	pairs, err := s.rdb.ZRevRangeWithScores(ctx, stocksKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	result := make([]StockInfo, 0, len(pairs))
	for _, z := range pairs {
		code, _ := z.Member.(string)
		if code == "" {
			continue
		}
		var pp *float64
		if raw, _ := s.rdb.Get(ctx, priceKey(code)).Result(); raw != "" {
			if pv, err := strconv.ParseFloat(raw, 64); err == nil {
				pp = &pv
			}
		}
		result = append(result, StockInfo{StockCode: code, Price: pp, Volume: z.Score})
	}
	return result, nil
}

// RegisterStocks initialises each stock in the market:symbols ZSet.
func (s *PriceService) RegisterStocks(ctx context.Context, codes []string) error {
	if len(codes) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for _, code := range codes {
		if code != "" {
			pipe.ZAddNX(ctx, stocksKey, redis.Z{Score: 0, Member: code})
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}
