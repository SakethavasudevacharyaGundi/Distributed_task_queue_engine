package ratelimiter

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RateLimiter struct {
	Client *redis.Client
}

func (r *RateLimiter) Allow(key string, limit int, window time.Duration) (bool, error) {
	ctx := context.Background()
	current, err := r.Client.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if current == 1 {
		err = r.Client.Expire(ctx, key, window).Err()
		if err != nil {
			return false, err
		}
	}
	return current <= int64(limit), nil
}
