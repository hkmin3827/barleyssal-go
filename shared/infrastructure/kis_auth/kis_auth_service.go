// Package kisauth manages KIS (Korea Investment Securities) authentication tokens.
// It mirrors src/shared/infrastructure/kis-auth/kisAuthService.js.
package kisauth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"barleyssal-go/config"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

const (
	redisTokenKey    = "kis:access_token"
	redisApprovalKey = "kis:approval_key"
	tokenTTL         = 86_100 * time.Second // ~23h 55m
)

type KisAuthService struct {
	cfg   *config.Config
	rdb   *redis.Client
	log   *zap.Logger
	sched *cron.Cron
	http  *http.Client

	mu          sync.RWMutex
	approvalKey string
	accessToken string
}

func New(cfg *config.Config, rdb *redis.Client, log *zap.Logger) *KisAuthService {
	return &KisAuthService{
		cfg:  cfg,
		rdb:  rdb,
		log:  log,
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *KisAuthService) GetApprovalKey() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.approvalKey
}

func (s *KisAuthService) GetAccessToken(ctx context.Context) (string, error) {
	s.mu.RLock()
	if s.accessToken != "" {
		tok := s.accessToken
		s.mu.RUnlock()
		return tok, nil
	}
	s.mu.RUnlock()

	cached, err := s.rdb.Get(ctx, redisTokenKey).Result()
	if err == nil && cached != "" {
		s.mu.Lock()
		s.accessToken = cached
		s.mu.Unlock()
		s.log.Debug("KIS Access Token Redis 캐시에서 복구")
		return cached, nil
	}

	s.log.Info("Redis에 토큰 X -> 신규 발급 시도 ....")
	return s.FetchAccessToken(ctx)
}


func (s *KisAuthService) FetchApprovalKey(ctx context.Context) (string, error) {
	// Check Redis cache first
	cached, err := s.rdb.Get(ctx, redisApprovalKey).Result()
	if err == nil && cached != "" {
		s.mu.Lock()
		s.approvalKey = cached
		s.mu.Unlock()
		s.log.Debug("KIS Approval Key Redis 캐시에서 복구")
		return cached, nil
	}

	ext := s.cfg.External
	if ext.KisAppKey == "" || ext.KisAppSecret == "" {
		return "", fmt.Errorf("KIS_APP_KEY 또는 KIS_APP_SECRET 이 .env에 설정되지 않았습니다")
	}

	s.log.Info("KIS Approval Key 발급 요청 중...", zap.String("url", ext.KisApprovalURL))

	body, _ := json.Marshal(map[string]string{
		"grant_type": "client_credentials",
		"appkey":     ext.KisAppKey,
		"secretkey":  ext.KisAppSecret,
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ext.KisApprovalURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; utf-8")

	resp, err := s.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("KIS Approval Key 발급 HTTP 실패: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("KIS Approval Key 발급 실패: HTTP %d", resp.StatusCode)
	}

	var result struct {
		ApprovalKey string `json:"approval_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("KIS Approval Key 응답 파싱 실패: %w", err)
	}
	if result.ApprovalKey == "" {
		return "", fmt.Errorf("KIS Approval Key 응답에 approval_key 없음")
	}

	s.mu.Lock()
	s.approvalKey = result.ApprovalKey
	s.mu.Unlock()

	if err := s.rdb.Set(ctx, redisApprovalKey, result.ApprovalKey, tokenTTL).Err(); err != nil {
		s.log.Warn("KIS Approval Key Redis 저장 실패 (무시됨)", zap.Error(err))
	} else {
		s.log.Debug("KIS Approval Key Redis에 저장 완료 (23시간 55분)")
	}

	s.log.Info("KIS Approval Key 신규 발급 성공 !!")
	return result.ApprovalKey, nil
}

func (s *KisAuthService) FetchAccessToken(ctx context.Context) (string, error) {
	ext := s.cfg.External
	if ext.KisAppKey == "" || ext.KisAppSecret == "" {
		return "", fmt.Errorf("KIS_APP_KEY 또는 KIS_APP_SECRET 이 .env에 설정되지 않았습니다")
	}

	s.log.Info("KIS Access Token 발급 요청 중...", zap.String("url", ext.KisTokenURL))

	body, _ := json.Marshal(map[string]string{
		"grant_type": "client_credentials",
		"appkey":     ext.KisAppKey,
		"appsecret":  ext.KisAppSecret,
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ext.KisTokenURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := s.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("KIS Access Token HTTP 실패: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("KIS Access Token 응답 파싱 실패: %w", err)
	}
	if result.AccessToken == "" {
		return "", fmt.Errorf("KIS Access Token 응답에 access_token 없음")
	}

	s.mu.Lock()
	s.accessToken = result.AccessToken
	s.mu.Unlock()

	if err := s.rdb.Set(ctx, redisTokenKey, result.AccessToken, tokenTTL).Err(); err != nil {
		s.log.Warn("KIS Access Token Redis 저장 실패 (무시됨)", zap.Error(err))
	} else {
		s.log.Debug("KIS Access Token Redis에 저장")
	}

	s.log.Info("KIS Access Token 발급 성공 ✅")
	return result.AccessToken, nil
}



func (s *KisAuthService) StartTokenScheduler() {
	if s.sched != nil {
		s.log.Warn("KIS Token Scheduler가 이미 실행 중입니다.")
		return
	}

	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		loc = time.FixedZone("KST", 9*60*60)
	}

	s.sched = cron.New(cron.WithLocation(loc))
	_, _ = s.sched.AddFunc("0 0 * * *", func() {
		s.log.Info("[Cron] KIS Access Token 자정 재발급 시작...")
		ctx := context.Background()
		if _, err := s.FetchAccessToken(ctx); err != nil {
			s.log.Error("[Cron] KIS Access Token 재발급 실패", zap.Error(err))

			time.AfterFunc(5*time.Minute, func() {
				if _, err := s.FetchAccessToken(ctx); err != nil {
					s.log.Error("[Cron] KIS Access Token 재시도 실패 – 서버 관리자 수동 발급 필요", zap.Error(err))
				} else {
					s.log.Info("[Cron] KIS Access Token 재시도 성공 ✅")
				}
			})
		} else {
			s.log.Info("[Cron] KIS Access Token 재발급 완료 ✅")
		}
	})
	s.sched.Start()
	s.log.Info("KIS Access Token 자동 갱신 스케줄러 등록 (매일 00:00 KST)")
}

func (s *KisAuthService) StopTokenScheduler() {
	if s.sched != nil {
		s.sched.Stop()
		s.sched = nil
	}
}

func (s *KisAuthService) InitKisAuth(ctx context.Context) error {
	s.log.Info("KIS 인증 정보 초기화 시작...")

	// Access Token
	if cached, err := s.rdb.Get(ctx, redisTokenKey).Result(); err == nil && cached != "" {
		s.mu.Lock()
		s.accessToken = cached
		s.mu.Unlock()
		s.log.Info("KIS Access Token Redis 캐시에서 복구 성공 ✅")
	} else {
		if _, err := s.FetchAccessToken(ctx); err != nil {
			return fmt.Errorf("initKisAuth: access token 발급 실패: %w", err)
		}
	}

	if cached, err := s.rdb.Get(ctx, redisApprovalKey).Result(); err == nil && cached != "" {
		s.mu.Lock()
		s.approvalKey = cached
		s.mu.Unlock()
		s.log.Info("KIS Approval Key Redis 캐시에서 복구 성공 ✅")
	} else {
		if _, err := s.FetchApprovalKey(ctx); err != nil {
			return fmt.Errorf("initKisAuth: approval key 발급 실패: %w", err)
		}
	}

	s.StartTokenScheduler()
	return nil
}
