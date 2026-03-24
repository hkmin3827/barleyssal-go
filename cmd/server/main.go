package main

import (
	"barleyssal-go/config"
	chartapp "barleyssal-go/domains/chart/application"
	matchingapp "barleyssal-go/domains/order_matching/application"
	orderkafka "barleyssal-go/domains/order_matching/infrastructure/kafka"
	pnlapp "barleyssal-go/domains/pnl/application"
	httphandler "barleyssal-go/interfaces/http"
	kisauth "barleyssal-go/shared/infrastructure/kis_auth"
	kisrest "barleyssal-go/shared/infrastructure/kis_rest"
	wshub "barleyssal-go/shared/infrastructure/websocket"
	"barleyssal-go/shared/price"
	"barleyssal-go/shared/ratelimit"
	"barleyssal-go/shared/session"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg := config.Load()

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,  
		PoolSize: 20, 
		MinIdleConns: 5,
  	DialTimeout: 3*time.Second,
		PoolTimeout: 4*time.Second,
	})

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := rdb.Ping(ctx).Err(); err == nil {
			break
		}
		log.Warn("Redis not ready, retrying...", zap.Int("attempt", i+1))
		time.Sleep(200 * time.Millisecond)
	}

	authSvc := kisauth.New(cfg, rdb, log)
	if err := authSvc.InitKisAuth(ctx); err != nil {
		log.Error("Failed to initialize KIS Auth – API calls may fail", zap.Error(err))
	}

	execProducer := orderkafka.NewExecutionProducer(cfg, log)
	if err := execProducer.Connect(ctx); err != nil {
		log.Error("Kafka producer connection failed – executions will not work", zap.Error(err))
	}

	sessionRes := session.NewResolver(rdb)
	hub := wshub.NewHub(log, cfg, sessionRes)

	pnlSvc := pnlapp.New(rdb, hub, log)
	hub.SetPnlService(pnlSvc)

	matchEng := matchingapp.New(cfg, rdb, execProducer, hub, log)

	kisRestClient := kisrest.NewKisRestClient(log)
	chartSvc := chartapp.New(cfg, rdb, authSvc, kisRestClient, hub, log)

	priceSvc := price.New(cfg, rdb, chartSvc, matchEng, pnlSvc, log)

	watchCodes := config.WatchList(40)
	if err := priceSvc.RegisterStocks(ctx, watchCodes); err != nil {
		log.Warn("registerStocks failed", zap.Error(err))
	}
	
	orderConsumer := orderkafka.NewOrderConsumer(cfg, func(ctx context.Context, ev orderkafka.OrderRequestEvent) error {
		return matchEng.OnOrderReceived(ctx, matchingapp.OrderEvent{
			OrderID:    ev.OrderID,
			AccountID:  ev.AccountID,
			UserID:     ev.UserID,
			UserName:   ev.UserName,
			StockCode:  ev.StockCode,
			OrderSide:  ev.OrderSide,
			OrderType:  ev.OrderType,
			Quantity:   ev.Quantity,
			LimitPrice: ev.LimitPrice,
		})
	}, log)

	cancelCtx, cancelFn := context.WithCancel(ctx)
	orderConsumer.Start(cancelCtx)

	extClient := wshub.NewExternalMarketClient(
		cfg.External.WsURL,
		watchCodes,
		authSvc,
		priceSvc, 
		hub,
		chartSvc,	
		log,
	)

	go extClient.RunScheduler(cancelCtx)

// 	chartSvc.StartFlusher(cancelCtx)
// 	mockTicker := wshub.NewMockMarketTicker(priceSvc, hub, watchCodes, 500*time.Millisecond, log)
// go mockTicker.Start(cancelCtx)

go func() {
	log.Info("실시간 랭킹 스케쥴러 티커 활동")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cancelCtx.Done():
			return
		case <-ticker.C:
			broadcastCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

			topChangeRate, err := priceSvc.GetTopChangeRate(broadcastCtx, 10)
			if err != nil {
				log.Warn("GetTopChangeRate 실패", zap.Error(err))
				cancel()
				continue
			}

			topBuyVol, err := priceSvc.GetTopBuyVolume(broadcastCtx, 10)
			if err != nil {
				log.Warn("GetTopBuyVolume 실패", zap.Error(err))
				cancel()
				continue
			}

			cancel()

				msg := map[string]interface{}{
					"type":          "home_update",
					"topChangeRate": topChangeRate, 
					"topBuyVolume":  topBuyVol, 
					"ts":            time.Now().UnixMilli(),
				}
 
			msgBytes, err := json.Marshal(msg)
			if err == nil {
				hub.BroadcastToAll(msgBytes)
			}
		}
	}
}()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins:     []string{cfg.CorsOrigin},
		AllowCredentials: true,
		AllowMethods: middleware.DefaultCORSConfig.AllowMethods,
		AllowHeaders: []string{"Content-Type", "X-XSRF-TOKEN"},
	}))

	e.Use(middleware.RateLimiter(ratelimit.NewRedisRateLimiterStore(rdb, 70, time.Second)))

	httphandler.New(chartSvc, priceSvc, pnlSvc, rdb, hub, log, sessionRes).RegisterRoutes(e)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      e,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("✅ Go Market Gateway & Metrics started",
			zap.Int("port", cfg.Port),
			zap.String("ws", fmt.Sprintf("ws://localhost:%d/ws", cfg.Port)),
			zap.String("rest", fmt.Sprintf("http://localhost:%d/api/market", cfg.Port)),
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	log.Info("Shutting down gracefully...", zap.String("signal", sig.String()))

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()

	_ = srv.Shutdown(shutCtx) 
	extClient.Stop() 
	cancelFn()           
	_ = orderConsumer.Stop() 
	_ = execProducer.Disconnect()
	authSvc.StopTokenScheduler()
	rdb.Close()

	log.Info("Shutdown complete")
}