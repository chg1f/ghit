package ghit

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron"
)

var (
	DefaultNextInterval = time.Second

	EnableLimit = true

	ErrOverhit = errors.New("overhit")
)

type Config struct {
	Redis      redis.Cmdable
	Key        string
	ExpireSpec string
	Limit      int64
}
type Hitter struct {
	cache redis.Cmdable
	key   string

	expire cron.Schedule
	timer  *time.Timer

	limit    int64
	syncedAt time.Time
	remote   int64
	local    int64
}

func NewHitter(conf *Config) (*Hitter, error) {
	schedule, err := cron.ParseStandard(conf.ExpireSpec)
	if err != nil {
		return nil, err
	}
	h := Hitter{
		cache:  conf.Redis,
		key:    conf.Key,
		expire: schedule,
		timer:  time.NewTimer(DefaultNextInterval),
	}
	return &h, nil
}
func (h *Hitter) sync() (next time.Duration) {
	now := time.Now()
	local := atomic.LoadInt64(&h.local)
	elapsed := h.syncedAt.Sub(now)
	remote, err := h.cache.IncrBy(context.Background(), h.key, local).Result()
	if err != nil {
		return DefaultNextInterval
	}
	h.cache.Expire(context.Background(), h.key, h.expire.Next(now).Sub(time.Now()))
	h.syncedAt = now
	atomic.StoreInt64(&h.remote, remote)
	atomic.AddInt64(&h.local, -1*local)
	if remote+local >= h.limit {
		ttl, err := h.cache.TTL(context.Background(), h.key).Result()
		if err != nil {
			return DefaultNextInterval
		}
		return ttl
	}
	return time.Duration((h.limit-(remote+local))/(local/int64(elapsed.Seconds()))/2) * time.Second // XXX: after half of local qps sync remote hitted
}
func (h *Hitter) Hit() (err error) {
	defer func() {
		if err != nil {
			atomic.AddInt64(&h.local, 1)
		}
	}()
	select {
	case <-h.timer.C:
		h.timer.Reset(h.sync())
	default:
	}
	if EnableLimit && h.remote+h.local >= h.limit {
		return ErrOverhit
	}
	return nil
}
