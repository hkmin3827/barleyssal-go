// Package pnlapplication computes real-time PnL and pushes results over WebSocket.
// Business logic is ported 1:1 from src/domains/pnl/application/pnlService.js.
package pnlapplication

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"barleyssal-go/shared/ports"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// HoldingMeta mirrors the Redis-stored JSON in account:holdings:meta:{userId}.
type HoldingMeta struct {
	AvgPrice      string `json:"avgPrice"`
	TotalQuantity string `json:"totalQuantity"`
}

// HoldingDetail is the per-stock PnL record sent to the frontend.
type HoldingDetail struct {
	StockCode     string  `json:"stockCode"`
	TotalQuantity int64   `json:"totalQuantity"`
	AvgPrice      float64 `json:"avgPrice"`
	CurrentPrice  float64 `json:"currentPrice"`
	HoldValue     float64 `json:"holdValue"`
	PnlRate       float64 `json:"pnlRate"`
}

// PnlUpdatePayload is the WebSocket message sent to the user (PNL_UPDATE).
type PnlUpdatePayload struct {
	Type                string          `json:"type"`
	UserID              string          `json:"userId"`
	Deposit             float64         `json:"deposit"`
	Principal           float64         `json:"principal"`
	StockValue          float64         `json:"stockValue"`
	RealtimeTotalEquity float64         `json:"realtimeTotalEquity"`
	TotalPnlRate        float64         `json:"totalPnlRate"`
	Holdings            []HoldingDetail `json:"holdings"`
	Ts                  int64           `json:"ts"`
}

// PnlService manages per-user PnL subscriptions and calculations.
type PnlService struct {
	rdb      *redis.Client
	notifier ports.UserNotifier
	log      *zap.Logger

	mu               sync.RWMutex
	stockSubscribers map[string]map[string]struct{} // stockCode -> set of userIDs
	userHoldings     map[string]map[string]struct{} // userId    -> set of stockCodes
}

// New creates a new PnlService.
func New(rdb *redis.Client, notifier ports.UserNotifier, log *zap.Logger) *PnlService {
	return &PnlService{
		rdb:              rdb,
		notifier:         notifier,
		log:              log,
		stockSubscribers: make(map[string]map[string]struct{}),
		userHoldings:     make(map[string]map[string]struct{}),
	}
}

// SubscribeUser loads the user's holdings from Redis and registers price subscriptions.
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
	return nil
}

// UnsubscribeUser removes all price subscriptions for the user.
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

// RefreshUserSubscription unsubscribes then re-subscribes.
func (s *PnlService) RefreshUserSubscription(ctx context.Context, userID string) error {
	s.UnsubscribeUser(userID)
	return s.SubscribeUser(ctx, userID)
}

// OnPriceUpdate triggers PnL recalculation for all users subscribed to the stock.
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

// calcAndPush computes PnL for a user and pushes a PNL_UPDATE message over WebSocket.
// This mirrors calcAndPush in pnlService.js exactly.
func (s *PnlService) calcAndPush(ctx context.Context, userID string, triggerPrice float64, triggerCode string) error {
	status, err := s.rdb.HGetAll(ctx, "account:status:"+userID).Result()
	if err != nil || status["deposit"] == "" || status["principal"] == "" {
		return nil
	}

	deposit, _ := strconv.ParseFloat(status["deposit"], 64)
	principal, _ := strconv.ParseFloat(status["principal"], 64)

	holdingsMeta, err := s.rdb.HGetAll(ctx, "account:holdings:meta:"+userID).Result()
	if err != nil {
		return fmt.Errorf("holdings meta: %w", err)
	}

	var stockValue float64
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

		var currentPrice float64
		if code == triggerCode {
			currentPrice = triggerPrice
		} else {
			priceStr, err := s.rdb.Get(ctx, "market:price:"+code).Result()
			if err != nil || priceStr == "" {
				avgP, _ := strconv.ParseFloat(meta.AvgPrice, 64)
				currentPrice = avgP
			} else {
				currentPrice, _ = strconv.ParseFloat(priceStr, 64)
			}
		}

		avgPrice, _ := strconv.ParseFloat(meta.AvgPrice, 64)
		holdValue := currentPrice * float64(totalQty)

		var pnlRate float64
		if avgPrice > 0 {
			pnlRate = ((currentPrice - avgPrice) / avgPrice) * 100
		}

		stockValue += holdValue
		holdingDetails = append(holdingDetails, HoldingDetail{
			StockCode:     code,
			TotalQuantity: totalQty,
			AvgPrice:      avgPrice,
			CurrentPrice:  currentPrice,
			HoldValue:     holdValue,
			PnlRate:       math.Round(pnlRate*100) / 100,
		})
	}

	realtimeTotalEquity := deposit + stockValue
	var totalPnlRate float64
	if principal > 0 {
		totalPnlRate = ((realtimeTotalEquity - principal) / principal) * 100
	}

	payload := PnlUpdatePayload{
		Type:                "PNL_UPDATE",
		UserID:              userID,
		Deposit:             deposit,
		Principal:           principal,
		StockValue:          stockValue,
		RealtimeTotalEquity: math.Round(realtimeTotalEquity*100) / 100,
		TotalPnlRate:        math.Round(totalPnlRate*100) / 100,
		Holdings:            holdingDetails,
		Ts:                  time.Now().UnixMilli(),
	}

	s.notifier.PushToUser(userID, payload)
	return nil
}
