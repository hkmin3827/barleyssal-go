package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"barleyssal-go/config"
	"barleyssal-go/shared/utils"

	"go.uber.org/zap"

	kfkgo "github.com/segmentio/kafka-go"
)

const (
	orderRequestTopic = "order.request"
	dlqTopic          = "service-common-dlq"
)

// OrderRequestEvent mirrors the OrderRequestEvent typedef in orderConsumer.js.
type OrderRequestEvent struct {
	OccurredAt int64  `json:"occurredAt"`
	OrderID    string `json:"orderId"`
	AccountID  string `json:"accountId"`
	UserID     string `json:"userId"`
	UserName   string `json:"userName"`
	StockCode  string `json:"stockCode"`
	OrderSide  string `json:"orderSide"`
	OrderType  string `json:"orderType"`
	Quantity   string `json:"quantity"`
	LimitPrice string `json:"limitPrice"`
	Timestamp  int64 `json:"timestamp"`
}

// OrderHandler is called for each successfully parsed order event.
type OrderHandler func(ctx context.Context, event OrderRequestEvent) error

// DLQMessage is the payload written to the dead-letter queue.
type DLQMessage struct {
	OriginalTopic string `json:"originalTopic"`
	Payload       string `json:"payload"`
	ErrorType     string `json:"errorType"`
	ErrorMessage  string `json:"errorMessage"`
	Timestamp     int64 `json:"timestamp"`
}

// OrderConsumer wraps a kafka-go Reader for the order.request topic.
type OrderConsumer struct {
	reader    *kfkgo.Reader
	dlqWriter *kfkgo.Writer
	handler   OrderHandler
	log       *zap.Logger
	stopCh    chan struct{}
}

// NewOrderConsumer creates a new Kafka consumer for the order-request topic.
func NewOrderConsumer(cfg *config.Config, handler OrderHandler, log *zap.Logger) *OrderConsumer {
	reader := kfkgo.NewReader(kfkgo.ReaderConfig{
		Brokers:           cfg.Kafka.Brokers,
		Topic:             orderRequestTopic,
		GroupID:           "go-order-matcher",
		MinBytes:          1,
		MaxBytes:          1024 * 1024,
		MaxWait:           500 * time.Millisecond,
		CommitInterval:    0,
		StartOffset:       kfkgo.LastOffset,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	})

	dlqWriter := &kfkgo.Writer{
		Addr:     kfkgo.TCP(cfg.Kafka.Brokers...),
		Topic:    dlqTopic,
		Balancer: &kfkgo.LeastBytes{},
	}

	return &OrderConsumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		handler:   handler,
		log:       log,
		stopCh:    make(chan struct{}),
	}
}

// Start begins consuming messages in a background goroutine.
func (c *OrderConsumer) Start(ctx context.Context) {
	go c.consume(ctx)
	c.log.Info("Order consumer started", zap.String("topic", orderRequestTopic))
}

func (c *OrderConsumer) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("Kafka FetchMessage error", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		rawValue := string(msg.Value)

		var event OrderRequestEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.log.Error("Malformed JSON in order.request. Skipping...",
				zap.Error(err), zap.String("rawValue", rawValue))
			c.sendToDLQ(ctx, orderRequestTopic, rawValue, "PARSE_ERROR", err.Error())
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.handler(ctx, event); err != nil {
			if isTransientError(err) {
				c.log.Error("Transient error. Will retry on next fetch.",
					zap.Error(err), zap.String("orderId", event.OrderID))
				// No commit — message will be re-delivered
				continue
			}
			c.log.Error("Permanent error. Sending to DLQ.",
				zap.Error(err), zap.String("orderId", event.OrderID))
			c.sendToDLQ(ctx, orderRequestTopic, rawValue, "BUSINESS_ERROR", err.Error())
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Warn("CommitMessages failed", zap.Error(err))
		} else {
			c.log.Info("Order processed and offset committed",
				zap.String("orderId", event.OrderID))
		}
	}
}

func (c *OrderConsumer) sendToDLQ(ctx context.Context, originalTopic, payload, errType, errMsg string) {
	dlq := DLQMessage{
		OriginalTopic: originalTopic,
		Payload:       payload,
		ErrorType:     errType,
		ErrorMessage:  errMsg,
		Timestamp:      utils.NowMillis(),
	}
	data, _ := json.Marshal(dlq)
	if err := c.dlqWriter.WriteMessages(ctx, kfkgo.Message{Value: data}); err != nil {
		c.log.Error("Could not send to DLQ",
			zap.Error(err), zap.String("payload", payload))
	}
}

// Stop signals the consumer goroutine and closes the reader.
func (c *OrderConsumer) Stop() error {
	close(c.stopCh)
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("order consumer reader close failed: %w", err)
	}
	return c.dlqWriter.Close()
}

func isTransientError(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, p := range []string{"econnrefused", "etimedout", "connection lost", "socket timeout"} {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}
