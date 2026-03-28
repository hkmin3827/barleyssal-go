package websocket

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"barleyssal-go/shared/ports"

	"go.uber.org/zap"
)

type MockMarketTicker struct {
	priceHook  ports.PriceEventHandler
	hub        *Hub
	log        *zap.Logger
	watchCodes []string
	interval   time.Duration

	basePrices map[string]float64
}

func NewMockMarketTicker(
	priceHook ports.PriceEventHandler,
	hub *Hub,
	watchCodes []string,
	interval time.Duration,
	log *zap.Logger,
) *MockMarketTicker {
	base := map[string]float64{
		"005930": 60000,  // 삼성전자
		"000660": 180000, // SK하이닉스
		"373220": 420000, // LG에너지솔루션
		"207940": 780000, // 삼성바이오로직스
		"005380": 220000, // 현대차
		"000270": 110000, // 기아
		"068270": 190000, // 셀트리온
		"005490": 280000, // POSCO홀딩스
		"035420": 185000, // NAVER
		"035720": 42000,  // 카카오
	}
	return &MockMarketTicker{
		priceHook:  priceHook,
		hub:        hub,
		log:        log,
		watchCodes: watchCodes,
		interval:   interval,
		basePrices: base,
	}
}

func (m *MockMarketTicker) Start(ctx context.Context) {
	m.log.Info("MockMarketTicker 시작",
		zap.Int("codes", len(m.watchCodes)),
		zap.Duration("interval", m.interval),
	)

	acmlVolMap := make(map[string]int64, len(m.watchCodes))
	curPriceMap := make(map[string]float64, len(m.watchCodes))
	for _, code := range m.watchCodes {
		curPriceMap[code] = m.getBasePrice(code)
	}

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.log.Info("MockMarketTicker 종료")
			return
		case <-ticker.C:
			now := time.Now().UTC()
			for _, code := range m.watchCodes {
				cur := curPriceMap[code]
				base := m.getBasePrice(code)

				delta := cur * (rand.Float64()*0.006 - 0.003)
				newPrice := math.Round(cur+delta)
				if newPrice <= 0 {
					newPrice = base
				}
				curPriceMap[code] = newPrice

				cntgVol := int64(rand.Intn(50) + 1)
				acmlVolMap[code] += cntgVol

				changeRate := (newPrice - base) / base * 100
				prdyVrssSign := int64(2) // 2=상승, 5=하락
				if changeRate < 0 {
					prdyVrssSign = 5
				}
				prdyVrss := newPrice - base

				liveBar, err := m.priceHook.OnPriceUpdate(
					ctx,
					code,
					newPrice,
					changeRate,
					cntgVol,
					prdyVrssSign,
					prdyVrss,
					base,       // stckOprc (시가 = 기준가로 고정)
					newPrice,   // stckHgpr
					newPrice,   // stckLwpr
					acmlVolMap[code],
					now,
					"20",
				)
				if err != nil {
					m.log.Warn("MockTicker OnPriceUpdate 실패", zap.String("code", code), zap.Error(err))
					continue
				}

				tickData := map[string]interface{}{
					"type":         "PRICE_UPDATE",
					"stockCode":    code,
					"price":        newPrice,
					"prdyVrssSign": fmt.Sprintf("%d", prdyVrssSign),
					"prdyVrss":     prdyVrss,
					"prdyCtrt":     changeRate,
					"stckOprc":     base,
					"stckHgpr":     newPrice,
					"stckLwpr":     newPrice,
					"askp1":        newPrice + 100,
					"bidp1":        newPrice - 100,
					"cntgVol":      float64(cntgVol),
					"acmlVol":      acmlVolMap[code],
					"acmlTrPbmn":   int64(newPrice) * acmlVolMap[code],
					"cttr":         50.0,
					"selnCntgCsnu": float64(rand.Intn(10)),
					"shnuCntgCsnu": float64(rand.Intn(10)),
					"mkopCode":     "20",
					"shnuCntgSmtn": float64(cntgVol),
					"ts":           now.UnixMilli(),
					"ohlcv": map[string]interface{}{
						"t": liveBar.T, "o": liveBar.O, "h": liveBar.H,
						"l": liveBar.L, "c": liveBar.C, "v": liveBar.V,
					},
				}
				m.hub.BroadcastPriceUpdate(code, tickData)
				m.priceHook.UpdateBuyVolume(ctx, code, float64(cntgVol))
			}
		}
	}
}

func (m *MockMarketTicker) getBasePrice(code string) float64 {
	if p, ok := m.basePrices[code]; ok {
		return p
	}
	return 50000
}
