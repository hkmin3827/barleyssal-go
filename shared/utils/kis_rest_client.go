package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	kisRateLimitDelay = 550 * time.Millisecond
	kisTimeoutMs      = 7 * time.Second
	kisMaxRetries     = 3
)

// KisRestClient enforces a per-call rate limit and retry logic when calling
// the KIS REST API (mirrors kisRestClient.js).
type KisRestClient struct {
	mu         sync.Mutex
	lastCallAt time.Time
	httpClient *http.Client
	log        *zap.Logger
}

// NewKisRestClient creates a new KisRestClient.
func NewKisRestClient(log *zap.Logger) *KisRestClient {
	return &KisRestClient{
		httpClient: &http.Client{Timeout: kisTimeoutMs},
		log:        log,
	}
}

// enforceRateLimit waits until at least kisRateLimitDelay has passed since the last call.
func (c *KisRestClient) enforceRateLimit() {
	c.mu.Lock()
	defer c.mu.Unlock()

	elapsed := time.Since(c.lastCallAt)
	if elapsed < kisRateLimitDelay {
		time.Sleep(kisRateLimitDelay - elapsed)
	}
	c.lastCallAt = time.Now()
}

// FetchKisAPI performs a GET request to the KIS API with rate limiting and retries.
// It returns the parsed JSON response as map[string]interface{}.
func (c *KisRestClient) FetchKisAPI(ctx context.Context, url string, headers map[string]string) (map[string]interface{}, error) {
	var lastErr error

	for attempt := 1; attempt <= kisMaxRetries; attempt++ {
		c.enforceRateLimit()

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

		// if resp.StatusCode >= 500 {
		// 	lastErr = fmt.Errorf("KIS Server Error: %d", resp.StatusCode)
		// 	if attempt < kisMaxRetries {
		// 		time.Sleep(time.Duration(attempt) * time.Second)
		// 	}
		// 	continue
		// }

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
