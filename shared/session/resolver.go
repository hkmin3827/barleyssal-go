package session

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisKeyPrefix  = "barleyssal:session:sessions:"
	tokenAttrField  = "sessionAttr:ACCESS_TOKEN"
)

type Resolver struct {
	rdb *redis.Client
}

func NewResolver(rdb *redis.Client) *Resolver {
	return &Resolver{rdb: rdb}
}

func (r *Resolver) UserIDFromCookie(ctx context.Context, cookieVal string) (string, error) {
	if cookieVal == "" {
		return "", fmt.Errorf("SESSION cookie missing")
	}

	sessionIDBytes, err := base64.RawURLEncoding.DecodeString(cookieVal)
	if err != nil {
		return "", fmt.Errorf("invalid SESSION cookie encoding: %w", err)
	}
	sessionID := string(sessionIDBytes)

	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	raw, err := r.rdb.HGet(ctx, redisKeyPrefix+sessionID, tokenAttrField).Bytes()
	if err == redis.Nil {
		return "", fmt.Errorf("session not found or expired")
	}
	if err != nil {
		return "", fmt.Errorf("redis error: %w", err)
	}

	tokenStr, err := extractJavaString(raw)
	if err != nil {
		return "", fmt.Errorf("failed to read token: %w", err)
	}

	return extractSubject(tokenStr)
}

func extractJavaString(data []byte) (string, error) {
	const javaHeader = "\xac\xed\x00\x05\x74"
	if len(data) >= 7 && strings.HasPrefix(string(data), javaHeader) {
		length := int(data[5])<<8 | int(data[6])
		if len(data) < 7+length {
			return "", fmt.Errorf("java string data truncated")
		}
		return string(data[7 : 7+length]), nil
	}
	// plain string (non-JDK serialization)
	return strings.TrimSpace(string(data)), nil
}


func extractSubject(tokenStr string) (string, error) {
	tokenStr = strings.TrimPrefix(tokenStr, "Bearer ")
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid JWT format")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims struct {
		Sub string `json:"sub"`
		Exp int64  `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("failed to parse JWT claims: %w", err)
	}
	if claims.Sub == "" {
		return "", fmt.Errorf("missing sub claim")
	}
	if claims.Exp > 0 && time.Now().Unix() > claims.Exp {
		return "", fmt.Errorf("token expired")
	}
	return claims.Sub, nil
}