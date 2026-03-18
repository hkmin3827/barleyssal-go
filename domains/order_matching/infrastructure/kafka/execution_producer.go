// Package kafka provides Kafka infrastructure for the order-matching domain.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"barleyssal-go/config"
	matchingapplication "barleyssal-go/domains/order_matching/application"
	"barleyssal-go/shared/utils"

	kfkgo "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const executionTopic = "execution.event"

type ExecutionProducer struct {
	writer    *kfkgo.Writer
	log       *zap.Logger
	mu        sync.Mutex
	connected bool
}

var _ matchingapplication.ExecutionPublisher = (*ExecutionProducer)(nil)

func NewExecutionProducer(cfg *config.Config, log *zap.Logger) *ExecutionProducer {
	w := &kfkgo.Writer{
		Addr:                   kfkgo.TCP(cfg.Kafka.Brokers...),
		Topic:                  executionTopic,
		Balancer:               &kfkgo.LeastBytes{},
		AllowAutoTopicCreation: false,
		RequiredAcks:           kfkgo.RequireAll,
		MaxAttempts:            5,
		WriteBackoffMin:        100 * time.Millisecond,
		WriteBackoffMax:        1 * time.Second,
	}
	return &ExecutionProducer{writer: w, log: log}
}

func (p *ExecutionProducer) Connect(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connected = true
	p.log.Info("Kafka execution producer connected")
	return nil
}

func (p *ExecutionProducer) PublishExecutionEvent(ctx context.Context, params matchingapplication.ExecutionEventParams) error {
	payload := map[string]interface{}{
		"orderId":          params.OrderID,
		"userId":           params.UserID,
		"userName":         params.UserName,
		"accountId":        params.AccountID,
		"stockCode":        params.StockCode,
		"orderSide":        params.OrderSide,
		"orderType":			params.OrderType,
		"executedQuantity": params.ExecutedQty,
		"executedPrice":    params.ExecutedPrice,
		"executionStatus": params.ExecutionStatus,
		"timestamp":      utils.NowMillis(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("execution event marshal failed: %w", err)
	}

	msg := kfkgo.Message{Key: []byte(params.OrderID), Value: data}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.log.Error("Failed to publish ExecutionEvent",
			zap.String("orderId", params.OrderID), zap.Error(err))
		return fmt.Errorf("kafka write failed: %w", err)
	}

	p.log.Info("ExecutionEvent published",
		zap.String("orderId", params.OrderID),
		zap.String("stockCode", params.StockCode),
		zap.String("side", params.OrderSide),
		zap.Float64("price", params.ExecutedPrice))
	return nil
}

func (p *ExecutionProducer) Disconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.connected {
		return nil
	}
	if err := p.writer.Close(); err != nil {
		return err
	}
	p.connected = false
	return nil
}
