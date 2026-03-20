package pnlapplication

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"barleyssal-go/shared/ports"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type HoldingMeta struct {
	AvgPrice      string `json:"avgPrice"`
	TotalQuantity string `json:"totalQuantity"`
}

type HoldingDetail struct {
	StockCode     string  `json:"stockCode"`
	TotalQuantity int64   `json:"totalQuantity"`
	AvgPrice      float64 `json:"avgPrice"`
	CurrentPrice  float64 `json:"currentPrice"`
	HoldValue     float64 `json:"holdValue"`
	PnlAmount     float64 `json:"pnlAmount"`
	PnlRate       float64 `json:"pnlRate"`  
}

type PnlUpdatePayload struct {
	Type                string          `json:"type"`
	UserID              string          `json:"userId"`
	Deposit             float64         `json:"deposit"`
	Principal           float64         `json:"principal"`
	StockValue          float64         `json:"stockValue"`   
	RealtimeTotalEquity float64         `json:"realtimeTotalEquity"` 
	TotalPnlAmount      float64         `json:"totalPnlAmount"`  
	TotalPnlRate        float64         `json:"totalPnlRate"`
	Holdings            []HoldingDetail `json:"holdings"`
	Ts                  int64           `json:"ts"`
}

type PnlService struct {
	rdb      *redis.Client
	notifier ports.UserNotifier
	log      *zap.Logger

	mu               sync.RWMutex
	stockSubscribers map[string]map[string]struct{}
	userHoldings     map[string]map[string]struct{}
}

func New(rdb *redis.Client, notifier ports.UserNotifier, log *zap.Logger) *PnlService {
	return &PnlService{
		rdb:              rdb,
		notifier:         notifier,
		log:              log,
		stockSubscribers: make(map[string]map[string]struct{}),
		userHoldings:     make(map[string]map[string]struct{}),
	}
}

func (s *PnlService) SubscribeUser(ctx context.Context, userID string) error {
	holdings, err := s.rdb.HGetAll(ctx, "account:holdings:"+userID).Result()
	if err != nil {
		s.log.Warn("subscribeUser: Redis HGetAll failed", zap.String("userId", userID), zap.Error(err))
		return err
	}

	codes := make(map[string]struct{}, len(holdings))
	for code := range holdings {
		codes[code] = struct{}{}
	}

	s.mu.Lock()
	s.userHoldings[userID] = codes
	for code := range codes {
		if s.stockSubscribers[code] == nil {
			s.stockSubscribers[code] = make(map[string]struct{})
		}
		s.stockSubscribers[code][userID] = struct{}{}
	}
	s.mu.Unlock()

	codeList := make([]string, 0, len(codes))
	for c := range codes {
		codeList = append(codeList, c)
	}
	s.log.Debug("PnL subscription registered", zap.String("userId", userID), zap.Strings("codes", codeList))

	go func() {
		if err := s.calcAndPush(context.Background(), userID, 0, ""); err != nil {
			s.log.Warn("initial PNL snapshot failed", zap.String("userId", userID), zap.Error(err))
		}
	}()

	return nil
}

func (s *PnlService) UnsubscribeUser(userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for code := range s.userHoldings[userID] {
		if subs := s.stockSubscribers[code]; subs != nil {
			delete(subs, userID)
			if len(subs) == 0 {
				delete(s.stockSubscribers, code)
			}
		}
	}
	delete(s.userHoldings, userID)
}

func (s *PnlService) RefreshUserSubscription(ctx context.Context, userID string) error {
	s.UnsubscribeUser(userID)
	return s.SubscribeUser(ctx, userID)
}

func (s *PnlService) OnPriceUpdate(stockCode string, currentPrice float64) {
	s.mu.RLock()
	subs := s.stockSubscribers[stockCode]
	if len(subs) == 0 {
		s.mu.RUnlock()
		return
	}
	userIDs := make([]string, 0, len(subs))
	for uid := range subs {
		userIDs = append(userIDs, uid)
	}
	s.mu.RUnlock()

	ctx := context.Background()
	for _, uid := range userIDs {
		go func(userID string) {
			if err := s.calcAndPush(ctx, userID, currentPrice, stockCode); err != nil {
				s.log.Warn("PnL calc failed",
					zap.String("userId", userID), zap.String("stockCode", stockCode), zap.Error(err))
			}
		}(uid)
	}
}

func (s *PnlService) calcAndPush(ctx context.Context, userID string, triggerPrice float64, triggerCode string) error {

	snapshot, err := s.CalcSnapshot(ctx, userID)

	if err != nil { return err }
	if snapshot == nil {
		return nil
	}

	if triggerCode != "" && triggerPrice > 0 {
		for i := range snapshot.Holdings {
			h := &snapshot.Holdings[i]
			if h.StockCode != triggerCode {
				continue
			}

			oldHoldValue := h.HoldValue
			oldPnlAmount := h.PnlAmount

			newHoldValue := triggerPrice * float64(h.TotalQuantity)
			newPnlAmount := (triggerPrice - h.AvgPrice) * float64(h.TotalQuantity)
			var newPnlRate float64
			if h.AvgPrice > 0 {
				newPnlRate = ((triggerPrice - h.AvgPrice) / h.AvgPrice) * 100
			}
			
			h.CurrentPrice = triggerPrice
			h.HoldValue = round2(newHoldValue)
			h.PnlAmount = round2(newPnlAmount)
			h.PnlRate = round2(newPnlRate)


			newStockValue := snapshot.StockValue - oldHoldValue + newHoldValue
			snapshot.StockValue = round2(newStockValue)
			snapshot.TotalPnlAmount = round2(snapshot.TotalPnlAmount - oldPnlAmount + newPnlAmount)
			snapshot.RealtimeTotalEquity = round2(snapshot.Deposit + newStockValue)


			var totalCostBasis float64
			for _, h2 := range snapshot.Holdings {
				totalCostBasis += h2.AvgPrice * float64(h2.TotalQuantity)
			}
			if totalCostBasis > 0 {
				snapshot.TotalPnlRate = round2((newStockValue - totalCostBasis) / totalCostBasis * 100)
			}
 
			snapshot.Ts = time.Now().UnixMilli()
			break
		}
	}
	s.notifier.PushToUser(userID, snapshot)
	return nil
}


func (s *PnlService) CalcSnapshot(ctx context.Context, userID string) (*PnlUpdatePayload, error) {
	status, err := s.rdb.HGetAll(ctx, "account:status:"+userID).Result()
	if err != nil || status["deposit"] == "" || status["principal"] == "" {
		return nil, nil
	}

	deposit, _ := strconv.ParseFloat(status["deposit"], 64)
	principal, _ := strconv.ParseFloat(status["principal"], 64)

	holdingsMeta, err := s.rdb.HGetAll(ctx, "account:holdings:meta:"+userID).Result()
	if err != nil {
		return nil, fmt.Errorf("holdings meta: %w", err)
	}

	codes := make([]string, 0, len(holdingsMeta))
	for code := range holdingsMeta {
		codes = append(codes, code)
	}

	redisPrice := make(map[string]float64, len(codes))
	if len(codes) > 0 {
		pipe := s.rdb.Pipeline()
		cmds := make(map[string]*redis.SliceCmd, len(codes))
		for _, code := range codes {
			cmds[code] = pipe.HMGet(ctx, "market:info:"+code, "price")
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			s.log.Warn("market:info batch fetch failed", zap.Error(err))
		}
		for code, cmd := range cmds {
			vals := cmd.Val()
			if len(vals) > 0 && vals[0] != nil {
				if p := parseInfoPrice(vals[0]); p > 0 {
					redisPrice[code] = p
				}
			}
		}
	}

	var stockValue, totalCostBasis, totalPnlAmount float64
	holdingDetails := make([]HoldingDetail, 0, len(holdingsMeta))

	for code, metaStr := range holdingsMeta {
		var meta HoldingMeta
		if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
			continue
		}
		totalQty, err := strconv.ParseInt(meta.TotalQuantity, 10, 64)
		if err != nil || totalQty <= 0 {
			continue
		}

		avgPrice, _ := strconv.ParseFloat(meta.AvgPrice, 64)

		currentPrice := avgPrice
		if p, ok := redisPrice[code]; ok {
			currentPrice = p
		}

		holdValue := currentPrice * float64(totalQty)

		pnlAmount := (currentPrice - avgPrice) * float64(totalQty)

		var pnlRate float64
		if avgPrice > 0 {
			pnlRate = ((currentPrice - avgPrice) / avgPrice) * 100
		}

		stockValue += holdValue
		totalCostBasis += avgPrice * float64(totalQty)
		totalPnlAmount += pnlAmount

		holdingDetails = append(holdingDetails, HoldingDetail{
			StockCode: code,
			TotalQuantity: totalQty,
			AvgPrice: avgPrice,
			CurrentPrice: currentPrice,
			HoldValue: holdValue,
			PnlAmount: round2(pnlAmount),
			PnlRate: round2(pnlRate),
		})
	}

	sort.Slice(holdingDetails, func(i, j int) bool {
		return holdingDetails[i].HoldValue > holdingDetails[j].HoldValue
	})

	realtimeTotalEquity := deposit + stockValue

	var totalPnlRate float64
	if totalCostBasis > 0 {
		totalPnlRate = ((stockValue - totalCostBasis) / totalCostBasis) * 100
	}

	return &PnlUpdatePayload{
		Type:                "PNL_UPDATE",
		UserID:              userID,
		Deposit:             deposit,
		Principal:           principal,
		StockValue:          round2(stockValue),
		RealtimeTotalEquity: round2(realtimeTotalEquity),
		TotalPnlAmount:      round2(totalPnlAmount),
		TotalPnlRate:        round2(totalPnlRate),
		Holdings:            holdingDetails,
		Ts:                  time.Now().UnixMilli(),
	}, nil
}

func parseInfoPrice(v interface{}) float64 {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}