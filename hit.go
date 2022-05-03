package ghit

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron"
)

var (
	Enable = true

	DefaultMinInterval time.Duration
	DefaultMaxInterval = time.Second

	ErrOverhit = errors.New("overhit")
)

type Config struct {
	Redis redis.Cmdable
	Key   string

	ExpireSpec     string
	FollowInterval time.Duration
	MaxInterval    time.Duration
	MinInterval    time.Duration

	EnableLimit bool
	Limit       int64
}
type Hitter struct {
	cache redis.Cmdable
	key   string

	expire         cron.Schedule
	timer          *time.Timer
	followInterval time.Duration
	maxInterval    time.Duration
	minInterval    time.Duration

	ewma         ewma.MovingAverage
	latestSyncAt time.Time
	local        int64

	enableLimit bool
	limit       int64

	current int64
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
	if conf.MinInterval != 0 {
		minInterval = conf.MinInterval
	}
	followInterval := DefaultMaxInterval * 3
	if conf.FollowInterval != 0 {
		followInterval = conf.FollowInterval
	}
	h := Hitter{
		cache: conf.Redis,
		key:   conf.Key,

		expire:         schedule,
		timer:          time.NewTimer(minInterval),
		followInterval: followInterval,
		maxInterval:    maxInterval,
		minInterval:    minInterval,

		ewma:         ewma.NewMovingAverage(),
		latestSyncAt: time.Now(),

		enableLimit: conf.EnableLimit,
		limit:       conf.Limit,
	}
	go h.flush()
	return &h, nil
}

func (h *Hitter) flush() {
	if h.cache == nil {
		h.timer.Stop()
		return
	}
	for {
		select {
		case now, ok := <-h.timer.C:
			if !ok {
				return
			}
			h.Sync(now)
		}
	}
}
func (h *Hitter) Sync(now time.Time) (next time.Time, err error) {
	if h.cache == nil {
		return time.Time{}, nil
	}
	var (
		local    = atomic.LoadInt64(&h.local)
		expireAt = h.expire.Next(now)
		key      = h.key + ":" + strconv.FormatInt(expireAt.Unix(), 10)
	)
	defer func() {
		if next.Sub(expireAt) > 0 {
			next = expireAt
		}
		interval := time.Now().Sub(next)
		if interval > h.maxInterval {
			interval = h.maxInterval
		} else if interval < h.minInterval {
			interval = h.minInterval
		}
		h.timer.Reset(interval)
	}()
	if local != 0 {
		current, err := h.cache.IncrBy(context.Background(), key, local).Result()
		if err != nil {
			atomic.AddInt64(&h.local, local)
			return now, err
		}
		h.cache.Expire(context.Background(), key, time.Now().Sub(expireAt)+h.followInterval)

		atomic.StoreInt64(&h.current, current)
		atomic.AddInt64(&h.local, -local)
	}
	if atomic.LoadInt64(&h.current) >= h.limit {
		return expireAt, nil
	}

	h.latestSyncAt = now
	h.ewma.Add(float64(local) / h.latestSyncAt.Sub(now).Seconds())
	estimate := time.Duration(float64(h.limit-atomic.LoadInt64(&h.current)) / h.ewma.Value() * float64(time.Second))
	return now.Add(estimate / 2), nil
}

func (h *Hitter) Hit() (err error) {
	if !Enable {
		return nil
	}
	if h.enableLimit && atomic.LoadInt64(&h.current) >= h.limit {
		h.Sync(time.Now())
		return ErrOverhit
	}
	atomic.AddInt64(&h.current, 1)
	atomic.AddInt64(&h.local, 1)
	return nil
}

func (h *Hitter) Close() error {
	h.Sync(time.Now())
	h.timer.Stop()
	return nil
}
