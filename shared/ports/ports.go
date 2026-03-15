package ports

import "context"

type UserNotifier interface {
	PushToUser(userID string, data interface{})
	BroadcastPriceUpdate(stockCode string, price float64, volume int64)
	NotifyExecution(data interface{})
	GetConnectedCount() int
}

type PnlSubscriptionService interface {
	SubscribeUser(ctx context.Context, userID string) error
	UnsubscribeUser(userID string)
	RefreshUserSubscription(ctx context.Context, userID string) error
	OnPriceUpdate(stockCode string, currentPrice float64)
}

type PriceEventHandler interface {
	OnPriceUpdate(ctx context.Context, stockCode string, price float64, volume int64) error
}
