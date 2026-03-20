package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const rateLimitLua = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
	redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if count > tonumber(ARGV[1]) then
	return 0
end
return 1
`

type RedisRateLimiterStore struct {
	rdb    *redis.Client
	limit  int   
	window time.Duration 
}

func NewRedisRateLimiterStore(rdb *redis.Client, limit int, window time.Duration) *RedisRateLimiterStore {
	return &RedisRateLimiterStore{rdb: rdb, limit: limit, window: window}
}

func (s *RedisRateLimiterStore) Allow(identifier string) (bool, error) {
	key := "ratelimit:go:" + identifier

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, err := s.rdb.Eval(ctx, rateLimitLua, []string{key},
		s.limit, int(s.window.Seconds()),
	).Int()
	if err != nil {
		return true, nil
	}
	return result == 1, nil
}