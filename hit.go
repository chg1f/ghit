package ghit

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/chg1f/errorx"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron"
)

var (
	Enable = true

	NodeID  string
	MaxDate time.Time

	DefaultMinInterval time.Duration
	DefaultMaxInterval = time.Second

	ErrOverhit        = errors.New("overhit")
	ErrNondistrubuted = errors.New("nondistrubuted")

	NodesQPSCacheKeySuffix = ":QPS"
	NodesCacheKeySuffix    = ":NODES"
)

func init() {
	NodeID = strconv.FormatUint(rand.Uint64(), 16)
	MaxDate = time.Now().AddDate(math.MaxInt, math.MaxInt, math.MaxInt)
}

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

	ewma          ewma.MovingAverage
	latestSyncAt  time.Time
	latestHitAt   time.Time
	remote        int64
	local         int64
	spinForSplit  int32
	spinForUpdate int32

	enableLimit bool
	limit       int64
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
	return &h, nil
}

func (h *Hitter) Sync(now time.Time) (next time.Time, err error) {
	if h.cache == nil {
		return MaxDate, ErrNondistrubuted
	}
	var (
		local    = atomic.LoadInt64(&h.local)
		expireAt = h.expire.Next(now)
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
		key := h.key + ":" + strconv.FormatInt(expireAt.Unix(), 10)
		current, err := h.cache.IncrBy(context.Background(), key, local).Result()
		if err != nil {
			return now, err
		}
		h.cache.Expire(context.Background(), key, time.Now().Sub(expireAt)+h.followInterval)

		atomic.StoreInt32(&h.spinForUpdate, 1)
		atomic.StoreInt64(&h.remote, current)
		atomic.AddInt64(&h.local, -local)
		h.latestSyncAt = now
		atomic.StoreInt32(&h.spinForUpdate, 000000000)
		if current >= h.limit {
			return expireAt, nil
		}
	}
	h.ewma.Add(float64(local) / h.latestSyncAt.Sub(now).Seconds())
	qps := h.ewma.Value()
	{
		var ex error
		ex = errorx.Compress(ex, h.cache.Set(context.Background(), h.key+":"+NodeID+NodesQPSCacheKeySuffix, qps, next.Sub(time.Now())+h.maxInterval).Err())
		ex = errorx.Compress(ex, h.cache.ZAdd(context.Background(), h.key+NodesCacheKeySuffix, &redis.Z{Score: float64(now.Unix()), Member: NodeID}).Err())
		h.cache.Expire(context.Background(), h.key+NodesCacheKeySuffix, h.maxInterval*10+h.followInterval)
		if ex == nil {
			nodeIDs, err := h.cache.ZRangeByScore(context.Background(), h.key+NodesCacheKeySuffix, &redis.ZRangeBy{
				Min: strconv.FormatInt(now.Add(h.maxInterval*-5).Unix(), 10),
				Max: strconv.FormatInt(time.Now().Unix(), 10),
			}).Result()
			if err == nil {
				cluster := qps / float64(len(nodeIDs))
				for _, nodeID := range nodeIDs {
					if nodeID == NodeID {
						continue
					}
					v, err := h.cache.Get(context.Background(), h.key+":"+nodeID+NodesQPSCacheKeySuffix).Float64()
					if err != nil {
						cluster += qps / float64(len(nodeIDs))
						continue
					}
					cluster += v / float64(len(nodeIDs))
				}
				qps = cluster
			}
		}
	}
	current := atomic.LoadInt64(&h.remote) + atomic.LoadInt64(&h.local)
	estimate := time.Duration(float64(h.limit-current) / qps * float64(time.Second))
	return now.Add(estimate / 2), nil
}

func (h *Hitter) Hit() (err error) {
	if !Enable {
		return nil
	}
	now := time.Now()
	select {
	case <-h.timer.C:
		atomic.StoreInt32(&h.spinForSplit, 1)
		if now.Sub(h.expire.Next(h.latestHitAt)) > 0 {
			h.Sync(h.latestHitAt)
		}
		atomic.StoreInt32(&h.spinForSplit, 0)
		h.Sync(now)
	default:
	}
	for atomic.LoadInt32(&h.spinForUpdate) != 0 {
	}
	if h.enableLimit &&
		atomic.LoadInt64(&h.remote)+atomic.LoadInt64(&h.local) >= h.limit {
		if atomic.LoadInt64(&h.local) != 0 {
			h.Sync(time.Now())
		}
		return ErrOverhit
	}
	for atomic.LoadInt32(&h.spinForSplit) != 0 {
	}
	atomic.AddInt64(&h.local, 1)
	h.latestHitAt = now
	return nil
}

func (h *Hitter) Close() error {
	h.Sync(time.Now())
	h.timer.Stop()
	return nil
}
