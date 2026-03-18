package ports

import (
	"context"
	"time"
)


type UserNotifier interface {
	PushToUser(userID string, data interface{})
	BroadcastPriceUpdate(stockCode string, tickData map[string]interface{})
	BroadcastToAll(payload []byte)
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
		OnPriceUpdate(
		ctx       context.Context,
		stockCode string,
		price     float64,
		changeRate float64,
		volume    int64,
		prdyVrssSign int64,
		prdyVrss  float64,
		stckOprc  float64,
		stckHgpr  float64,
		stckLwpr  float64,
		acmlVol   int64,
		eventTime time.Time,
		mkopCode  string,
	) error
	UpdateBuyVolume(ctx context.Context, stockCode string, shnuCntgSmtn float64)
}
