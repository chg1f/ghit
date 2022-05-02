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
	DefaultMinInterval = time.Second
	DefaultMaxInterval = time.Minute

	EnableLimit = true

	ErrOverhit = errors.New("overhit")
)

type Config struct {
	Redis       redis.Cmdable
	Key         string
	ExpireSpec  string
	Limit       int64
	MaxInterval time.Duration
	MinInterval time.Duration
}
type Hitter struct {
	cache redis.Cmdable
	key   string

	expire      cron.Schedule
	timer       *time.Timer
	maxInterval time.Duration
	minInterval time.Duration

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
	maxInterval := DefaultMaxInterval
	if conf.MaxInterval != 0 {
		maxInterval = conf.MaxInterval
	}
	minInterval := DefaultMinInterval
	if conf.MaxInterval != 0 {
		minInterval = conf.MinInterval
	}
	h := Hitter{
		cache: conf.Redis,
		key:   conf.Key,

		expire:      schedule,
		timer:       time.NewTimer(minInterval),
		maxInterval: maxInterval,
		minInterval: minInterval,

		limit:    conf.Limit,
		syncedAt: time.Time{},
		remote:   0,
		local:    0,
	}
	return &h, nil
}
func (h *Hitter) sync() (time.Time, error) {
	var (
		now   = time.Now()
		local = atomic.LoadInt64(&h.local)
	)
	current, err := h.cache.IncrBy(context.Background(), h.key, local).Result()
	if err != nil {
		return now, err
	}
	h.syncedAt = now
	atomic.StoreInt64(&h.remote, current)
	atomic.AddInt64(&h.local, -1*local)
	h.cache.ExpireAt(context.Background(), h.key, h.expire.Next(now))
	if current >= h.limit {
		return h.expire.Next(now), nil
	}
	qps := local / int64(h.syncedAt.Sub(now).Seconds())
	interval := time.Duration((h.limit-current)/qps) * time.Second / 2 // XXX: after half of local qps sync remote hitted
	if interval > h.maxInterval {
		return now.Add(h.maxInterval), nil
	} else if interval < h.minInterval {
		return now.Add(h.minInterval), nil
	}
	return now.Add(interval), nil
}
func (h *Hitter) Sync() error {
	next, err := h.sync()
	if err != nil {
		h.timer.Reset(h.minInterval)
		return err
	}
	h.timer.Reset(next.Sub(time.Now()))
	return nil
}
func (h *Hitter) Hit() (err error) {
	defer func() {
		if err != nil {
			atomic.AddInt64(&h.local, 1)
		}
	}()
	select {
	case <-h.timer.C:
		if err := h.Sync(); err != nil {
			return err
		}
	default:
	}
	if EnableLimit && h.remote+h.local >= h.limit {
		return ErrOverhit
	}
	return nil
}
