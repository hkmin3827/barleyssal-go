package main

import (
	"barleyssal-go/config"
	chartapp "barleyssal-go/domains/chart/application"
	matchingapp "barleyssal-go/domains/order_matching/application"
	orderkafka "barleyssal-go/domains/order_matching/infrastructure/kafka"
	pnlapp "barleyssal-go/domains/pnl/application"
	httphandler "barleyssal-go/interfaces/http"
	kisauth "barleyssal-go/shared/infrastructure/kis_auth"
	wshub "barleyssal-go/shared/infrastructure/websocket"
	"barleyssal-go/shared/price"
	"barleyssal-go/shared/utils"
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
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg := config.Load()

	// ── Redis ────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	// Wait for Redis to be ready (mirrors index.js retry loop)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := rdb.Ping(ctx).Err(); err == nil {
			break
		}
		log.Warn("Redis not ready, retrying...", zap.Int("attempt", i+1))
		time.Sleep(200 * time.Millisecond)
	}

	// ── KIS Auth Service ─────────────────────────────────────────────────────
	authSvc := kisauth.New(cfg, rdb, log)
	if err := authSvc.InitKisAuth(ctx); err != nil {
		log.Error("Failed to initialize KIS Auth – API calls may fail", zap.Error(err))
	}

	// ── Kafka Execution Producer ─────────────────────────────────────────────
	execProducer := orderkafka.NewExecutionProducer(cfg, log)
	if err := execProducer.Connect(ctx); err != nil {
		log.Error("Kafka producer connection failed – executions will not work", zap.Error(err))
	}

	// ── Dependency wiring ────────────────────────────────────────────────────
	hub := wshub.NewHub(log)

	// 2. Create PnlService with Hub as its notifier
	pnlSvc := pnlapp.New(rdb, hub, log)

	// 3. Inject PnlService back into Hub
	hub.SetPnlService(pnlSvc)

	// 4. MatchingEngine
	matchEng := matchingapp.New(cfg, rdb, execProducer, hub, log)

	// 5. ChartService
	kisRestClient := utils.NewKisRestClient(log)
	chartSvc := chartapp.New(cfg, rdb, authSvc, kisRestClient, log)

	// 6. PriceService (ties everything together)
	priceSvc := price.New(cfg, rdb, chartSvc, matchEng, pnlSvc, log)

	// ── Register stocks in Redis ─────────────────────────────────────────────
	watchCodes := config.WatchList(40)
	if err := priceSvc.RegisterStocks(ctx, watchCodes); err != nil {
		log.Warn("registerStocks failed", zap.Error(err))
	}

	// ── Kafka Order Consumer ─────────────────────────────────────────────────
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

	// ── External KIS WebSocket Client ────────────────────────────────────────
	extClient := wshub.NewExternalMarketClient(
		cfg.External.WsURL,
		watchCodes,
		authSvc,
		priceSvc, // implements ports.PriceEventHandler
		hub,
		log,
	)
	if err := extClient.Start(ctx); err != nil {
		log.Error("KIS external client failed to start", zap.Error(err))
	}

	// ── 실시간 랭킹 브로드캐스트 스케줄러 ──
go func() {
	ticker := time.NewTicker(2 * time.Second) // 2초 주기로 실행 (서버 부하 및 프론트 렌더링 최적화)
	defer ticker.Stop()

	for {
		select {
		case <-cancelCtx.Done(): // 메인 서버 종료 시 스케줄러도 함께 안전하게 종료
			return
		case <-ticker.C:
			// 빈 검색어("")로 호출 시 거래량(ZSET Score) 상위 40개 반환
			ranking, err := priceSvc.SearchStocks(context.Background(), "", 40)
			if err != nil {
				log.Warn("실시간 랭킹 조회 실패", zap.Error(err))
				continue
			}

			// 프론트엔드에서 데이터 종류를 식별할 수 있도록 type 필드 추가
			msg := map[string]interface{}{
				"type": "ranking",
				"data": ranking,
			}

			msgBytes, err := json.Marshal(msg)
			if err == nil {
				// Hub의 Broadcast 채널로 전송 (Hub 내부 구현에 따라 메서드명은 다를 수 있음)
				hub.Broadcast <- msgBytes 
			}
		}
	}
}()

	// ── Echo HTTP Server ─────────────────────────────────────────────────────
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins:     []string{cfg.CorsOrigin},
		AllowCredentials: true,
	}))
	// Rate limit ~1000 req / 15 min per IP  (70 req/s sustained)
	e.Use(middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(70)))

	httphandler.New(chartSvc, priceSvc, rdb, hub, log).RegisterRoutes(e)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      e,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("✅ Go Market Gateway started",
			zap.Int("port", cfg.Port),
			zap.String("ws", fmt.Sprintf("ws://localhost:%d/ws", cfg.Port)),
			zap.String("rest", fmt.Sprintf("http://localhost:%d/api/market", cfg.Port)),
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	// ── Graceful Shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	log.Info("Shutting down gracefully...", zap.String("signal", sig.String()))

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()

	_ = srv.Shutdown(shutCtx) // stop HTTP
	extClient.Stop()           // stop KIS WS
	cancelFn()                 // stop Kafka consumer goroutine
	_ = orderConsumer.Stop()   // close reader
	_ = execProducer.Disconnect()
	authSvc.StopTokenScheduler()
	rdb.Close()

	log.Info("Shutdown complete")
}
