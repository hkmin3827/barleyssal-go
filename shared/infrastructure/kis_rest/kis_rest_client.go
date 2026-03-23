package kisrest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

const (
	kisRateLimitDelay = 550 * time.Millisecond
	kisTimeoutMs      = 7 * time.Second
	kisMaxRetries     = 3
)

type KisRestClient struct {
	tokenCh    chan struct{}
	httpClient *http.Client
	log        *zap.Logger
}



func NewKisRestClient(log *zap.Logger) *KisRestClient {
	c := &KisRestClient{
		tokenCh:    make(chan struct{}, 1),
		httpClient: &http.Client{Timeout: kisTimeoutMs},
		log:        log,
	}

	c.tokenCh <- struct{}{}

	go func() {
		ticker := time.NewTicker(kisRateLimitDelay)
		for range ticker.C {
			select {
			case c.tokenCh <- struct{}{}: 
			default: 
			}
		}
	}()

	return c
}

func (c *KisRestClient) FetchKisAPI(ctx context.Context, url string, headers map[string]string) (map[string]interface{}, error) {
	var lastErr error

	for attempt := 1; attempt <= kisMaxRetries; attempt++ {

		select {
		case <-c.tokenCh:
		case <-ctx.Done():
			return nil, fmt.Errorf("KIS rate limit wait cancelled: %w", ctx.Err())
		}

		reqCtx, cancel := context.WithTimeout(ctx, kisTimeoutMs)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to build KIS request: %w", err)
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := c.httpClient.Do(req)

		if err != nil {
			cancel()
			lastErr = err
			c.log.Warn("[KIS REST] 요청 실패",
				zap.Int("attempt", attempt), zap.Int("maxRetries", kisMaxRetries),
				zap.Error(err))
			if attempt < kisMaxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			cancel()
			c.log.Warn("[KIS REST] 유량 초과(429). 재시도 대기 중...",
				zap.Int("attempt", attempt), zap.Int("maxRetries", kisMaxRetries))
			time.Sleep(time.Duration(attempt*2) * time.Second)
			continue
		}


		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			cancel()
			return nil, fmt.Errorf("KIS API Error %d: %s", resp.StatusCode, string(body))
		}

		var result map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&result); 
		resp.Body.Close() 
		cancel()
		if err != nil {
					c.log.Error("[KIS REST] JSON 디코딩 실패 또는 취소", 
            zap.String("url", url), 
            zap.Error(err))
			return nil, fmt.Errorf("failed to decode KIS response: %w", err)
		}
		return result, nil
	}

	return nil, fmt.Errorf("KIS API 호출 최종 실패 (%s): %w", url, lastErr)
}