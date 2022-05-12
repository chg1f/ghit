package ghit

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/robfig/cron"
)

var (
	Enable = true

	DefaultShakeRate = 0.5

	DefaultKeyQPSSuffix    = ":QPS"
	DefaultKeySuffixFormat = "2006-01-02"
)

type HitterOption struct {
	NodeOption

	ExpireSpec     string
	ShakeRate      float64
	EnableMaxSync  bool
	MaxInterval    time.Duration
	EnableMinSync  bool
	MinInterval    time.Duration
	FollowInterval time.Duration

	Key             string
	KeyQPSSuffix    string
	KeySuffixFormat string

	EnableLimit bool
	Limit       int64
}
type Hitter struct {
	*Node

	sched          cron.Schedule
	stableRate     float64
	enableMaxSync  bool
	maxInterval    time.Duration
	enableMinSync  bool
	minInterval    time.Duration
	followInterval time.Duration

	key             string
	keyQPSSuffix    string
	keySuffixFormat string

	timer *time.Timer

	qps      ewma.MovingAverage
	syncedAt time.Time
	synced   int64
	unsync   int64
	hittedAt time.Time

	enableLimit bool
	limit       int64
}

func NewHitter(opt *HitterOption) (*Hitter, error) {
	now := time.Now()
	sched, err := cron.ParseStandard(opt.ExpireSpec)
	if err != nil {
		return nil, err
	}
	stableRate := 1 / DefaultShakeRate
	if opt.ShakeRate < 0 {
		if opt.ShakeRate >= 1 {
			stableRate = 0
		} else if opt.ShakeRate == 0 {
			stableRate = 1
		} else {
			stableRate = 1 / opt.ShakeRate
		}
	}
	keySuffixFormat := DefaultKeySuffixFormat
	if opt.KeySuffixFormat != "" {
		keySuffixFormat = opt.KeySuffixFormat
	}
	keyQPSSuffix := opt.Key + DefaultKeyQPSSuffix
	if opt.KeyQPSSuffix != "" {
		keyQPSSuffix = opt.KeyQPSSuffix
	}
	h := Hitter{
		Node: NewNode(&opt.NodeOption),

		sched:          sched,
		stableRate:     stableRate,
		enableMaxSync:  opt.EnableMaxSync,
		maxInterval:    opt.MaxInterval,
		enableMinSync:  opt.EnableMinSync,
		minInterval:    opt.MinInterval,
		followInterval: opt.FollowInterval,

		key:             opt.Key,
		keyQPSSuffix:    keyQPSSuffix,
		keySuffixFormat: keySuffixFormat,

		timer:    time.NewTimer(0),
		syncedAt: now,
		hittedAt: now,

		qps: ewma.NewMovingAverage(),
	}
	return &h, nil
}

func (h *Hitter) sync(now time.Time) (next time.Time, err error) {
	if h.Node == nil {
		return time.Time{}, nil
	}

	var (
		unsync   = atomic.LoadInt64(&h.unsync)
		local    = unsync - h.synced
		expireAt = h.sched.Next(now)
	)
	defer func() {
		interval := time.Now().Sub(next)
		if h.enableMaxSync && interval > h.maxInterval {
			interval = h.maxInterval
		}
		if h.enableMinSync && interval < h.minInterval {
			interval = h.minInterval
		}
		if time.Now().Add(interval).After(expireAt) {
			interval = expireAt.Sub(time.Now())
		}
		h.timer.Reset(interval)
	}()

	key := h.keyPrefix + h.key + expireAt.Format(h.keySuffixFormat)
	remote, err := h.redis.IncrBy(context.Background(), key, local).Result()
	if err != nil {
		return now, err
	}
	h.redis.Expire(context.Background(), key, time.Now().Sub(expireAt)+h.followInterval)
	atomic.StoreInt64(&h.synced, remote)
	atomic.AddInt64(&h.unsync, remote-unsync)

	keyQPS := h.keyPrefix + h.key + h.nodeID + h.keyQPSSuffix
	if now.After(h.syncedAt) {
		h.qps.Add(float64(local) / now.Sub(h.syncedAt).Seconds())
		h.redis.Set(context.Background(), keyQPS, h.qps.Value(), time.Now().Sub(expireAt)+h.followInterval)
	}
	h.syncedAt = now

	if h.enableLimit && remote >= h.limit {
		return expireAt, nil
	}
	qps := h.QPS()
	if qps <= 100/float64(h.limit) {
		return time.Now().Add(time.Duration(float64(now.Sub(expireAt)) * h.stableRate)), nil
	}
	return time.Now().Add(time.Duration(float64(h.limit-remote) / qps * float64(time.Second) * h.stableRate)), nil
}
func (h *Hitter) Sync(now time.Time) (next time.Time, err error) {
	if expireAt := h.sched.Next(h.hittedAt); now.After(expireAt) {
		if _, err := h.sync(expireAt); err != nil {
			return expireAt, err
		}
	}
	return h.sync(time.Now())
}
func (h *Hitter) Hit() (ok bool, err error) {
	defer func() {
		if ok {
			h.hittedAt = time.Now()
		}
	}()
	select {
	case <-h.timer.C:
		h.Sync(time.Now())
	default:
	}
	if h.enableLimit {
		if atomic.LoadInt64(&h.unsync) >= h.limit {
			return false, nil
		}
		if unsync := atomic.AddInt64(&h.unsync, 1); unsync <= h.limit {
			return true, nil
		}
		atomic.AddInt64(&h.unsync, -1)
		h.Sync(time.Now())
		return false, nil
	}
	return true, nil
}

func (h *Hitter) Background() {
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
func (h *Hitter) Hitted() int64 {
	h.Sync(time.Now())
	return atomic.LoadInt64(&h.unsync)
}
func (h *Hitter) QPS() float64 {
	nodeIDs, err := h.NodeIDs()
	if err != nil {
		return h.qps.Value()
	}
	var (
		num float64 = h.qps.Value()
		den int     = 1
	)
	for _, nodeID := range nodeIDs {
		if nodeID != h.nodeID {
			nodeQPS, err := h.redis.Get(context.Background(), h.keyPrefix+h.key+nodeID+h.keyQPSSuffix).Float64()
			if err != nil {
				continue
			}
			num += nodeQPS
			den += 1
		}
	}
	return num / float64(den)
}
func (h *Hitter) Close() error {
	defer h.timer.Stop()
	_, err := h.Sync(time.Now())
	return err
}
