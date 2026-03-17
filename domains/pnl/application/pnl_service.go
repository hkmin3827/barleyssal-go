// Package pnlapplication computes real-time PnL and pushes results over WebSocket.
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
	PnlAmount     float64 `json:"pnlAmount"`  // (currentPrice - avgPrice) * qty
	PnlRate       float64 `json:"pnlRate"`    // % 수익률
}

type PnlUpdatePayload struct {
	Type                string          `json:"type"`
	UserID              string          `json:"userId"`
	Deposit             float64         `json:"deposit"`
	Principal           float64         `json:"principal"`
	StockValue          float64         `json:"stockValue"`          // 보유 주식 평가금액 합계
	RealtimeTotalEquity float64         `json:"realtimeTotalEquity"` // deposit + stockValue
	TotalPnlAmount      float64         `json:"totalPnlAmount"`      // 주식 평가손익 합계 (원금 차이 아님)
	TotalPnlRate        float64         `json:"totalPnlRate"`        // 전체 수익률 (원금 기준)
	Holdings            []HoldingDetail `json:"holdings"`
	Ts                  int64           `json:"ts"`
}

type PnlService struct {
	rdb      *redis.Client
	notifier ports.UserNotifier
	log      *zap.Logger

	mu               sync.RWMutex
	stockSubscribers map[string]map[string]struct{} // stockCode -> set of userIDs
	userHoldings     map[string]map[string]struct{} // userId    -> set of stockCodes
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

	// 즉시 초기 스냅샷 전송: triggerCode = "" 이면 calcAndPush가
	// 모든 종목 가격을 Redis에서 조회하여 PNL_UPDATE를 바로 전송한다.
	go func() {
		if err := s.calcAndPush(context.Background(), userID, 0, ""); err != nil {
			s.log.Warn("initial PNL snapshot failed", zap.String("userId", userID), zap.Error(err))
		}
	}()

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
	var totalPnlAmount float64
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
				// Redis에 시세 없으면 평단가로 대체 (손익 0으로 표시)
				avgP, _ := strconv.ParseFloat(meta.AvgPrice, 64)
				currentPrice = avgP
			} else {
				currentPrice, _ = strconv.ParseFloat(priceStr, 64)
			}
		}

		avgPrice, _ := strconv.ParseFloat(meta.AvgPrice, 64)
		holdValue := currentPrice * float64(totalQty)

		// 종목별 평가손익 = (현재가 - 평단가) × 수량
		pnlAmount := (currentPrice - avgPrice) * float64(totalQty)

		var pnlRate float64
		if avgPrice > 0 {
			pnlRate = ((currentPrice - avgPrice) / avgPrice) * 100
		}

		stockValue += holdValue
		totalPnlAmount += pnlAmount

		holdingDetails = append(holdingDetails, HoldingDetail{
			StockCode:     code,
			TotalQuantity: totalQty,
			AvgPrice:      avgPrice,
			CurrentPrice:  currentPrice,
			HoldValue:     holdValue,
			PnlAmount:     math.Round(pnlAmount*100) / 100,
			PnlRate:       math.Round(pnlRate*100) / 100,
		})
	}

	// 총 자산 = 예수금 + 주식 평가금액
	realtimeTotalEquity := deposit + stockValue

	// 전체 수익률: 원금 기준 (예수금+평가금 vs 원금)
	var totalPnlRate float64
	if principal > 0 {
		totalPnlRate = ((realtimeTotalEquity - principal) / principal) * 100
	}

	payload := PnlUpdatePayload{
		Type:                "PNL_UPDATE",
		UserID:              userID,
		Deposit:             deposit,
		Principal:           principal,
		StockValue:          math.Round(stockValue*100) / 100,
		RealtimeTotalEquity: math.Round(realtimeTotalEquity*100) / 100,
		TotalPnlAmount:      math.Round(totalPnlAmount*100) / 100, // 주식 평가손익만
		TotalPnlRate:        math.Round(totalPnlRate*100) / 100,
		Holdings:            holdingDetails,
		Ts:                  time.Now().UnixMilli(),
	}

	s.notifier.PushToUser(userID, payload)
	return nil
}