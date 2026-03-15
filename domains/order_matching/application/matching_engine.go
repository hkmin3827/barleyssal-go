package matchingapplication

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"barleyssal-go/config"
	"barleyssal-go/shared/ports"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Redis key helpers (mirrors KEY object in matchingEngine.js)
func priceKey(code string) string       { return "market:price:" + code }
func pendingBuyKey(code string) string  { return "orders:pending:" + code + ":BUY" }
func pendingSellKey(code string) string { return "orders:pending:" + code + ":SELL" }
func orderMetaKey(id string) string     { return "order:meta:" + id }

type ExecutionPublisher interface {
	PublishExecutionEvent(ctx context.Context, params ExecutionEventParams) error
}

type ExecutionEventParams struct {
	OrderID       string
	UserID        string
	UserName      string
	AccountID     string
	StockCode     string
	OrderSide     string
	ExecutedQty   string
	ExecutedPrice float64
}

type OrderEvent struct {
	OrderID    string `json:"orderId"`
	AccountID  string `json:"accountId"`
	UserID     string `json:"userId"`
	UserName   string `json:"userName"`
	StockCode  string `json:"stockCode"`
	OrderSide  string `json:"orderSide"` // "BUY" | "SELL"
	OrderType  string `json:"orderType"` // "MARKET" | "LIMIT"
	Quantity   string `json:"quantity"`
	LimitPrice string `json:"limitPrice"` // nullable
}

type OrderMeta struct {
	OrderID    string
	UserID     string
	UserName   string
	AccountID  string
	StockCode  string
	OrderSide  string
	Quantity   string
	LimitPrice string
}

// MatchingEngine processes incoming orders against live Redis prices.
type MatchingEngine struct {
	cfg      *config.Config
	rdb      *redis.Client
	producer ExecutionPublisher
	notifier ports.UserNotifier
	log      *zap.Logger
}

// New creates a new MatchingEngine.
func New(
	cfg *config.Config,
	rdb *redis.Client,
	producer ExecutionPublisher,
	notifier ports.UserNotifier,
	log *zap.Logger,
) *MatchingEngine {
	return &MatchingEngine{
		cfg:      cfg,
		rdb:      rdb,
		producer: producer,
		notifier: notifier,
		log:      log,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// OnOrderReceived — mirrors onOrderReceived
// ─────────────────────────────────────────────────────────────────────────────

func (e *MatchingEngine) OnOrderReceived(ctx context.Context, event OrderEvent) error {
	if event.OrderType == "MARKET" {
		return e.executeMarketOrder(ctx, event)
	}
	if err := e.CheckLimitOrders(ctx, event.StockCode, ""); err != nil {
		e.log.Warn("CheckLimitOrders after limit order registration failed",
			zap.String("orderId", event.OrderID), zap.Error(err))
	}
	e.log.Debug("Limit order registered by Spring. Waiting for price match.",
		zap.String("orderId", event.OrderID),
		zap.String("stockCode", event.StockCode))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// CheckLimitOrders — mirrors checkLimitOrders (exported for price-tick calls)
// ─────────────────────────────────────────────────────────────────────────────

func (e *MatchingEngine) CheckLimitOrders(ctx context.Context, stockCode, tickPriceStr string) error {
	currentPriceStr := tickPriceStr
	if currentPriceStr == "" {
		val, err := e.rdb.Get(ctx, priceKey(stockCode)).Result()
		if err != nil {
			return nil
		}
		currentPriceStr = val
	}
	if currentPriceStr == "" {
		return nil
	}
	buyErr := e.matchOrders(ctx, stockCode, currentPriceStr, "BUY")
	sellErr := e.matchOrders(ctx, stockCode, currentPriceStr, "SELL")
	if buyErr != nil {
		return buyErr
	}
	return sellErr
}

// ─────────────────────────────────────────────────────────────────────────────
// executeMarketOrder — mirrors executeMarketOrder
// ─────────────────────────────────────────────────────────────────────────────

func (e *MatchingEngine) executeMarketOrder(ctx context.Context, event OrderEvent) error {
	priceStr, err := e.rdb.Get(ctx, priceKey(event.StockCode)).Result()
	if err != nil || priceStr == "" {
		e.log.Warn("Market order failed: No current price in Redis cache.",
			zap.String("orderId", event.OrderID),
			zap.String("stockCode", event.StockCode))
		return nil
	}
	return e.fireExecution(ctx, fireParams{
		OrderID:       event.OrderID,
		UserID:        event.UserID,
		UserName:      event.UserName,
		AccountID:     event.AccountID,
		StockCode:     event.StockCode,
		OrderSide:     event.OrderSide,
		Quantity:      event.Quantity,
		ExecutedPrice: priceStr,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// matchOrders — mirrors matchOrders
// ─────────────────────────────────────────────────────────────────────────────

func (e *MatchingEngine) matchOrders(ctx context.Context, stockCode, currentPriceStr, orderSide string) error {
	var zKey string
	if orderSide == "BUY" {
		zKey = pendingBuyKey(stockCode)
	} else {
		zKey = pendingSellKey(stockCode)
	}

	orderIDs, err := e.rdb.ZRangeByScore(ctx, zKey, &redis.ZRangeBy{
		Min: currentPriceStr,
		Max: currentPriceStr,
	}).Result()
	if err != nil {
		return fmt.Errorf("ZRangeByScore failed for %s: %w", zKey, err)
	}
	if len(orderIDs) == 0 {
		return nil
	}

	for _, orderID := range orderIDs {
		rawMeta, err := e.rdb.HGetAll(ctx, orderMetaKey(orderID)).Result()
		if err != nil || len(rawMeta) == 0 || rawMeta["orderId"] == "" {
			_ = e.rdb.ZRem(ctx, zKey, orderID).Err()
			continue
		}

		meta := OrderMeta{
			OrderID:   rawMeta["orderId"],
			UserID:    rawMeta["userId"],
			UserName:  rawMeta["userName"],
			AccountID: rawMeta["accountId"],
			StockCode: rawMeta["stockCode"],
			OrderSide: rawMeta["orderSide"],
			Quantity:  rawMeta["quantity"],
		}

		if err := e.fireExecution(ctx, fireParams{
			OrderID:       meta.OrderID,
			UserID:        meta.UserID,
			UserName:      meta.UserName,
			AccountID:     meta.AccountID,
			StockCode:     meta.StockCode,
			OrderSide:     meta.OrderSide,
			Quantity:      meta.Quantity,
			ExecutedPrice: currentPriceStr,
		}); err != nil {
			e.log.Error("Order matching failed",
				zap.String("orderId", orderID),
				zap.String("side", orderSide),
				zap.Error(err))
			continue
		}
		_ = e.rdb.ZRem(ctx, zKey, orderID).Err()
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// fireExecution — mirrors fireExecution
// ─────────────────────────────────────────────────────────────────────────────

type fireParams struct {
	OrderID       string
	UserID        string
	UserName      string
	AccountID     string
	StockCode     string
	OrderSide     string
	Quantity      string
	ExecutedPrice string
}

func (e *MatchingEngine) fireExecution(ctx context.Context, p fireParams) error {
	execPrice, err := strconv.ParseFloat(p.ExecutedPrice, 64)
	if err != nil {
		return fmt.Errorf("invalid executed price %q: %w", p.ExecutedPrice, err)
	}

	if err := e.producer.PublishExecutionEvent(ctx, ExecutionEventParams{
		OrderID:       p.OrderID,
		UserID:        p.UserID,
		UserName:      p.UserName,
		AccountID:     p.AccountID,
		StockCode:     p.StockCode,
		OrderSide:     p.OrderSide,
		ExecutedQty:   p.Quantity,
		ExecutedPrice: execPrice,
	}); err != nil {
		return fmt.Errorf("publishExecutionEvent failed: %w", err)
	}

	e.notifier.NotifyExecution(map[string]interface{}{
		"type":             "EXECUTION",
		"orderId":          p.OrderID,
		"userId":           p.UserID,
		"userName":         p.UserName,
		"stockCode":        p.StockCode,
		"orderSide":        p.OrderSide,
		"executedPrice":    p.ExecutedPrice,
		"executedQuantity": p.Quantity,
		"ts":               time.Now().UnixMilli(),
	})

	e.log.Info("Order executed",
		zap.String("orderId", p.OrderID),
		zap.String("stockCode", p.StockCode),
		zap.String("side", p.OrderSide),
		zap.String("executedPrice", p.ExecutedPrice),
		zap.String("qty", p.Quantity))
	return nil
}
