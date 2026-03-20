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

func priceKey(code string) string       { return "market:price:" + code }
func infoKey(code string) string { return "market:info:" + code }

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
	OrderType 		string
	ExecutedQty   string
	ExecutedPrice float64
	ExecutionStatus string
}

type OrderEvent struct {
	OrderID    string `json:"orderId"`
	AccountID  string `json:"accountId"`
	UserID     string `json:"userId"`
	UserName   string `json:"userName"`
	StockCode  string `json:"stockCode"`
	OrderSide  string `json:"orderSide"` 
	OrderType  string `json:"orderType"` 
	Quantity   string `json:"quantity"`
	LimitPrice string `json:"limitPrice"`
}

type OrderMeta struct {
	OrderID    string
	UserID     string
	UserName   string
	AccountID  string
	StockCode  string
	OrderSide  string
	OrderType string
	Quantity   string
	LimitPrice string
}

type MatchingEngine struct {
	cfg      *config.Config
	rdb      *redis.Client
	producer ExecutionPublisher
	notifier ports.UserNotifier
	log      *zap.Logger
}

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


func (e *MatchingEngine) CheckLimitOrders(ctx context.Context, stockCode, tickPriceStr string) error {
	currentPriceStr := tickPriceStr

	if currentPriceStr == "" {
		pipe := e.rdb.Pipeline()
		priceCmd := pipe.Get(ctx, priceKey(stockCode))
		infoCmd := pipe.HGet(ctx, infoKey(stockCode), "mKopCode")

		_, _ = pipe.Exec(ctx)

		mkop, _ := infoCmd.Result()
		if mkop != "" && mkop[0] != '2' {
				e.log.Debug("Market closed, skipping match", zap.String("code", stockCode), zap.String("mkop", mkop))
				return nil 
		}

		val, err := priceCmd.Result()
		if err != nil || val == "" {
				return nil
		}
		currentPriceStr = val
	} else {
			mkop, _ := e.rdb.HGet(ctx, infoKey(stockCode), "mKopCode").Result()
			if mkop != "" && mkop[0] != '2' {
					return nil
			}
	}

	buyErr := e.matchOrders(ctx, stockCode, currentPriceStr, "BUY")
	sellErr := e.matchOrders(ctx, stockCode, currentPriceStr, "SELL")
	if buyErr != nil {
		return buyErr
	}
	return sellErr
}


func (e *MatchingEngine) executeMarketOrder(ctx context.Context, event OrderEvent) error {
	pipe := e.rdb.Pipeline()
	priceCmd := pipe.Get(ctx, priceKey(event.StockCode))
	infoCmd := pipe.HGet(ctx, infoKey(event.StockCode), "mKopCode")

	_, _ = pipe.Exec(ctx) 

	mkop, _ := infoCmd.Result()
	if mkop != "" && mkop[0] != '2' {
		e.log.Warn("Market order REJECTED: Market is closed.", 
			zap.String("orderId", event.OrderID), 
			zap.String("mkopCode", mkop))
		
		return e.fireExecution(ctx, fireParams{
			OrderID:       event.OrderID,
			UserID:        event.UserID,
			UserName:      event.UserName,
			AccountID:     event.AccountID,
			StockCode:     event.StockCode,
			OrderSide:     event.OrderSide,
			OrderType: 		event.OrderType,
			Quantity:      "0",
			ExecutedPrice: 	0,
			ExecutionStatus: "CANCELLED",
		})
	}

	priceStr, err := priceCmd.Result()
	if err != nil || priceStr == "" {
		e.log.Warn("Market order CANCELLED: No current price in Redis.", zap.String("orderId", event.OrderID))
		return e.fireExecution(ctx, fireParams{
			OrderID:         event.OrderID,
			UserID:          event.UserID,
			UserName:        event.UserName,
			AccountID:       event.AccountID,
			StockCode:       event.StockCode,
			OrderSide:       event.OrderSide,
			OrderType:       event.OrderType,
			Quantity:        "0",
			ExecutedPrice:   0,
			ExecutionStatus: "CANCELLED",
		})
	}

	execPrice, _ := strconv.ParseFloat(priceStr, 64)

	return e.fireExecution(ctx, fireParams{
		OrderID:       event.OrderID,
		UserID:        event.UserID,
		UserName:      event.UserName,
		AccountID:     event.AccountID,
		StockCode:     event.StockCode,
		OrderSide:     event.OrderSide,
		OrderType:		event.OrderType,
		Quantity:      event.Quantity,
		ExecutedPrice: execPrice,
		ExecutionStatus: "SUCCESS",
	})
}


const fetchAndRemScript = `
	local ids = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])
	if #ids > 0 then
			for _, id in ipairs(ids) do
					redis.call('ZREM', KEYS[1], id)
			end
	end
	return ids
`
func (e *MatchingEngine) matchOrders(ctx context.Context, stockCode, currentPriceStr, orderSide string) error {
	var zKey string
	var min, max string

	if orderSide == "BUY" {
		zKey = pendingBuyKey(stockCode)
		min = currentPriceStr
		max = "+inf"
	} else {
		zKey = pendingSellKey(stockCode)
		min = "-inf"
		max = currentPriceStr
	}

	result, err := e.rdb.Eval(ctx, fetchAndRemScript, []string{zKey}, min, max).Result()
		if err != nil {
			return fmt.Errorf("Lua script execution failed for %s: %w", zKey, err)
		}
	
	orderIDs, ok := result.([]interface{})
	if !ok || len(orderIDs) == 0 {
		return nil
	}

	e.log.Info("Matching orders found", zap.Int("count", len(orderIDs)), zap.String("side", orderSide))

	for _, rawID := range orderIDs {
		orderID := rawID.(string)
		rawMeta, err := e.rdb.HGetAll(ctx, orderMetaKey(orderID)).Result()
		if err != nil || len(rawMeta) == 0 || rawMeta["orderId"] == "" {
			e.log.Warn("Order metadata missing, skipping", zap.String("orderId", orderID))
			continue
		}
				meta := OrderMeta{
			OrderID:   rawMeta["orderId"],
			UserID:    rawMeta["userId"],
			UserName:  rawMeta["userName"],
			AccountID: rawMeta["accountId"],
			StockCode: rawMeta["stockCode"],
			OrderSide: rawMeta["orderSide"],
			OrderType: rawMeta["orderType"],
			Quantity:  rawMeta["quantity"],
		}
		
		execPrice, _ := strconv.ParseFloat(currentPriceStr, 64)

		if err := e.fireExecution(ctx, fireParams{
			OrderID:       meta.OrderID,
			UserID:        meta.UserID,
			UserName:      meta.UserName,
			AccountID:     meta.AccountID,
			StockCode:     meta.StockCode,
			OrderSide:     meta.OrderSide,
			OrderType: 		meta.OrderType,
			Quantity:      meta.Quantity,
			ExecutedPrice: execPrice,
			ExecutionStatus: "SUCCESS",
		}); err != nil {
			e.log.Error("Order matching failed",
				zap.String("orderId", orderID),
				zap.String("side", orderSide),
				zap.Error(err))
			continue
		}
	}
	return nil
}

type fireParams struct {
	OrderID       string
	UserID        string
	UserName      string
	AccountID     string
	StockCode     string
	OrderSide     string
	OrderType			string
	Quantity      string
	ExecutedPrice float64
	ExecutionStatus string
}

func (e *MatchingEngine) fireExecution(ctx context.Context, p fireParams) error {

	err := e.producer.PublishExecutionEvent(ctx, ExecutionEventParams{
		OrderID:       p.OrderID,
		UserID:        p.UserID,
		UserName:      p.UserName,
		AccountID:     p.AccountID,
		StockCode:     p.StockCode,
		OrderSide:     p.OrderSide,
		OrderType:		p.OrderType,
		ExecutedQty:   p.Quantity,
		ExecutedPrice: p.ExecutedPrice,
		ExecutionStatus: p.ExecutionStatus,
	})

	noticeType := "EXECUTION"
	if p.ExecutionStatus == "CANCELLED" { noticeType = "ORDER_CANCELLED" }

	e.notifier.NotifyExecution(map[string]interface{}{
		"type":             noticeType,
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
		zap.Float64("executedPrice", p.ExecutedPrice),
    zap.String("qty", p.Quantity),
    zap.String("status", p.ExecutionStatus),
)
	return err
}
