package httphandler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	chartapplication "barleyssal-go/domains/chart/application"
	pnlApplication "barleyssal-go/domains/pnl/application"
	wshub "barleyssal-go/shared/infrastructure/websocket"
	"barleyssal-go/shared/price"
	"barleyssal-go/shared/session"
	"barleyssal-go/shared/utils"

	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Handler struct {
	chartSvc *chartapplication.ChartService
	priceSvc *price.PriceService
	pnlSvc   *pnlApplication.PnlService
	rdb      *redis.Client
	hub      *wshub.Hub
	log      *zap.Logger
	sessionRes *session.Resolver
}

func New(
	chartSvc *chartapplication.ChartService,
	priceSvc *price.PriceService,
	pnlSvc *pnlApplication.PnlService,
	rdb *redis.Client,
	hub *wshub.Hub,
	log *zap.Logger,
	sessionRes *session.Resolver,
) *Handler {
	return &Handler{chartSvc: chartSvc, priceSvc: priceSvc, pnlSvc: pnlSvc, rdb: rdb, hub: hub, log: log, sessionRes: sessionRes}
}

func (h *Handler) RegisterRoutes(e *echo.Echo) {
	chart := e.Group("/api/chart")
	chart.GET("/period/:stockCode", h.getPeriodChart)
	chart.GET("/intraday/:stockCode", h.getIntraDayChart)

	market := e.Group("/api/market")
	market.GET("/search", h.searchStocks)
	market.GET("/account/pnl/:userId", h.getAccountPnl)

	e.GET("/api/stocks", h.getSortedStocks)
	e.GET("/api/stocks/batch", h.getStocksBatch) 
	e.GET("/api/stocks/info/:code", h.getStockInfo)

	e.GET("/api/health", h.health)
	e.GET("/ws", echo.WrapHandler(h.hub))
}

func (h *Handler) getStocksBatch(c echo.Context) error {
	raw := c.QueryParam("codes")
	if raw == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "codes query param required"})
	}
 
	codes := make([]string, 0, 16)
	for _, code := range strings.Split(raw, ",") {
		code = strings.TrimSpace(strings.ToUpper(code))
		if code != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return c.JSON(http.StatusOK, echo.Map{"stocks": []interface{}{}})
	}
 
	stocks, err := h.priceSvc.GetStocksBatch(c.Request().Context(), codes)
	if err != nil {
		h.log.Warn("getStocksBatch failed", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "batch fetch failed"})
	}
	return c.JSON(http.StatusOK, echo.Map{"stocks": stocks})
}
 

func (h *Handler) getSortedStocks(c echo.Context) error {
	sortKey := c.QueryParam("sort")
	if sortKey == "" {
		sortKey = "changeRate"
	}
 
	if sortKey != "name" && sortKey != "changeRate" && sortKey != "acmlVol" {
		return c.JSON(http.StatusBadRequest, echo.Map{
			"error": "sort must be one of: name, changeRate, acmlVol",
		})
	}
 
	stocks, err := h.priceSvc.GetSortedStocks(c.Request().Context(), sortKey)
	if err != nil {
		h.log.Warn("getSortedStocks failed", zap.String("sort", sortKey), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "sort failed"})
	}
 
	return c.JSON(http.StatusOK, echo.Map{
		"sort":   sortKey,
		"stocks": stocks,
	})
}

func (h *Handler) getStockInfo(c echo.Context) error {
	code := strings.ToUpper(c.Param("code"))
	if code == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "stock code required"})
	}
 
	info, mkopCode, err := h.priceSvc.GetStockInfo(c.Request().Context(), code)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "info fetch failed"})
	}

	if info["price"] == 0 {
		return c.JSON(http.StatusNotFound, echo.Map{"error": "price not available yet"})
	}

	return c.JSON(http.StatusOK, echo.Map{
		"stockCode":  code,
		"price":      info["price"],
		"changeRate": info["changeRate"],
		"prdyVrssSign": info["prdyVrssSign"],
		"prdyVrss":   info["prdyVrss"],
		"stckOprc":   info["stckOprc"],
		"stckHgpr":   info["stckHgpr"],
		"stckLwpr":   info["stckLwpr"],
		"volume":     info["volume"],
		"acmlVol":    int64(info["acmlVol"]),
		"mKopCode":   mkopCode,
		"ts":         time.Now().UnixMilli(),
	})
}
 
func (h *Handler) getPeriodChart(c echo.Context) error {
	stockCode := strings.ToUpper(c.Param("stockCode"))
	if stockCode == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"success": false, "message": "종목 코드가 필요합니다."})
	}
	periodStr := c.QueryParam("period")
	if !map[string]bool{"D": true, "W": true, "M": true, "Y": true}[periodStr] {
		periodStr = "D"
	}
	h.log.Info("getPeriodChart called", zap.String("stockCode", stockCode), zap.String("period", periodStr))

	dr := utils.GetChartDateRange(periodStr)
	data, err := h.chartSvc.GetPeriodChartData(c.Request().Context(), stockCode, periodStr, dr.StartDate, dr.EndDate)
	if err != nil {
		h.log.Error("getPeriodChart failed", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, echo.Map{"success": false, "message": "차트 데이터를 불러오는 데 실패했습니다."})
	}
	return c.JSON(http.StatusOK, echo.Map{"success": true, "data": data})
}

func (h *Handler) getIntraDayChart(c echo.Context) error {
	stockCode := strings.ToUpper(c.Param("stockCode"))
	if stockCode == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"success": false, "message": "종목 코드가 필요합니다."})
	}
	timeframe := c.QueryParam("timeframe")
	if timeframe == "" {
		timeframe = "1m"
	}
	limit := 100
	if n, err := strconv.Atoi(c.QueryParam("limit")); err == nil && n > 0 {
		limit = n
	}
	data, err := h.chartSvc.GetOhlcv(c.Request().Context(), stockCode, timeframe, limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"success": false, "message": "분봉 데이터를 불러오는데 실패하였습니다."})
	}
	return c.JSON(http.StatusOK, echo.Map{"success": true, "data": data})
}


func (h *Handler) searchStocks(c echo.Context) error {
	q := c.QueryParam("q")
	limit := 20
	if n, err := strconv.Atoi(c.QueryParam("limit")); err == nil && n > 0 && n <= 100 {
		limit = n
	}
	results, err := h.priceSvc.SearchStocks(c.Request().Context(), q, limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": "search failed"})
	}
	return c.JSON(http.StatusOK, echo.Map{"query": q, "results": results})
}

func (h *Handler) getAccountPnl(c echo.Context) error {
    userID := c.Param("userId")
    snapshot, err := h.pnlSvc.CalcSnapshot(c.Request().Context(), userID)
    if err != nil || snapshot == nil {
        return c.JSON(http.StatusNotFound, echo.Map{"error": "Account not found"})
    }
    return c.JSON(http.StatusOK, snapshot)
}

func (h *Handler) health(c echo.Context) error {
	ctx := c.Request().Context()
	redisOk := h.rdb.Ping(ctx).Err() == nil
	statusCode := http.StatusOK
	statusStr, redisStr := "UP", "UP"
	if !redisOk {
		statusCode = http.StatusServiceUnavailable
		statusStr, redisStr = "DEGRADED", "DOWN"
		h.log.Error("Health check failed: Redis is unreachable")
	}
	return c.JSON(statusCode, echo.Map{
		"status":    statusStr,
		"redis":     redisStr,
		"wsClients": h.hub.GetConnectedCount(),
		"ts":        time.Now().UTC().Format(time.RFC3339),
	})
}
