package ghit

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/robfig/cron"
)

var (
	EnableLimit = true

	DefaultKeyQPSSuffix = ":QPS"
)

type HitterOption struct {
	*NodeOption

	ExpireSpec      string
	QPSShakeCoeff   float64
	EnableMaxSync   bool
	MaxSyncInterval time.Duration
	EnableMinSync   bool
	MinSyncInterval time.Duration
	FollowInterval  time.Duration

	Key             string
	KeyQPSSuffix    string
	KeySuffixFormat string

	EnableLimit bool
	Limit       int64

	ExpiredHook func(*Hitter)
	SyncedHook  func(*Hitter)
}
type Hitter struct {
	*Node

	sched           cron.Schedule
	qpsStableRate   float64
	enableMaxSync   bool
	maxSyncInterval time.Duration
	enableMinSync   bool
	minSyncInterval time.Duration
	followInterval  time.Duration

	key             string
	keyQPSSuffix    string
	keySuffixFormat string

	timer *time.Timer

	qps      ewma.MovingAverage
	syncedAt time.Time
	synced   int64
	unsync   int64
	hittedAt time.Time
	expireAt time.Time

	enableLimit bool
	limit       int64

	expiredHook func(*Hitter)
	syncedHook  func(*Hitter)
}

func NewHitter(opt *HitterOption) (*Hitter, error) {
	now := time.Now()
	sched, err := cron.ParseStandard(opt.ExpireSpec)
	if err != nil {
		return nil, err
	}
	keyQPSSuffix := opt.Key + DefaultKeyQPSSuffix
	if opt.KeyQPSSuffix != "" {
		keyQPSSuffix = opt.KeyQPSSuffix
	}
	h := Hitter{
		sched:           sched,
		qpsStableRate:   float64(1) / opt.QPSShakeCoeff,
		enableMaxSync:   opt.EnableMaxSync,
		maxSyncInterval: opt.MaxSyncInterval,
		enableMinSync:   opt.EnableMinSync,
		minSyncInterval: opt.MinSyncInterval,
		followInterval:  opt.FollowInterval,

		key:             opt.Key,
		keyQPSSuffix:    keyQPSSuffix,
		keySuffixFormat: opt.KeySuffixFormat,

		timer:    time.NewTimer(0),
		syncedAt: now,
		hittedAt: now,

		qps: ewma.NewMovingAverage(),

		enableLimit: opt.EnableLimit,
		limit:       opt.Limit,

		expiredHook: opt.ExpiredHook,
		syncedHook:  opt.SyncedHook,
	}
	if opt.NodeOption != nil {
		node, err := NewNode(opt.NodeOption)
		if err != nil {
			return nil, err
		}
		h.Node = node
	}
	_, err = h.Sync(time.Now())
	return &h, err
}

func (h *Hitter) sync(now time.Time) (next time.Time, err error) {
	var (
		unsync = atomic.LoadInt64(&h.unsync)
		local  = unsync - atomic.LoadInt64(&h.synced)
	)

	if h.Node == nil {
		if now == h.expireAt {
			atomic.StoreInt64(&h.unsync, 0)
		}
		atomic.StoreInt64(&h.synced, local)
		h.qps.Add(float64(local) / now.Sub(h.syncedAt).Seconds())
		h.syncedAt = now
		return h.expireAt, nil
	}

	defer func() {
		interval := time.Now().Sub(next)
		if h.enableMaxSync && interval > h.maxSyncInterval {
			interval = h.maxSyncInterval
		}
		if h.enableMinSync && interval < h.minSyncInterval {
			interval = h.minSyncInterval
		}
		if time.Now().Add(interval).After(h.expireAt) {
			interval = h.expireAt.Sub(time.Now())
		}
		h.timer.Reset(interval)
	}()

	var keySuffix string
	if h.keySuffixFormat != "" {
		keySuffix = h.expireAt.Format(h.keySuffixFormat)
	} else {
		keySuffix = ":" + strconv.FormatInt(h.expireAt.UnixMilli(), 10)
	}
	key := h.keyPrefix + h.key + keySuffix
	remote, err := h.redis.IncrBy(context.Background(), key, local).Result()
	if err != nil {
		return now, err
	}
	h.redis.Expire(context.Background(), key, time.Now().Sub(h.expireAt)+h.followInterval)
	atomic.StoreInt64(&h.synced, remote)
	atomic.AddInt64(&h.unsync, remote-unsync)

	keyQPS := h.keyPrefix + h.key + ":" + h.nodeID + h.keyQPSSuffix
	if now.After(h.syncedAt) {
		h.qps.Add(float64(local) / now.Sub(h.syncedAt).Seconds())
		h.redis.Set(context.Background(), keyQPS, h.qps.Value(), time.Now().Sub(h.expireAt)+h.followInterval)
	}
	h.syncedAt = now

	if EnableLimit && h.enableLimit && remote >= h.limit {
		return h.expireAt, nil
	}
	qps := h.clusterQPS()
	if qps <= 100/float64(h.limit) {
		return time.Now().Add(time.Duration(float64(now.Sub(h.expireAt)) * h.qpsStableRate)), nil
	}
	return time.Now().Add(time.Duration(float64(h.limit-remote) / qps * float64(time.Second) * h.qpsStableRate)), nil
}
func (h *Hitter) Sync(now time.Time) (next time.Time, err error) {
	if h.Node != nil {
		defer h.Node.Heartbeat()
	}
	if now.After(h.expireAt) {
		if _, err := h.sync(h.expireAt); err != nil {
			return h.expireAt, err
		}
		if h.expiredHook != nil {
			h.expiredHook(h)
		}
		h.expireAt = h.sched.Next(now)
	}
	if h.syncedHook != nil {
		defer h.syncedHook(h)
	}
	return h.sync(now)
}
func (h *Hitter) Hit() (ok bool, err error) {
	defer func() {
		if ok {
			h.hittedAt = time.Now()
		}
	}()
	if h.Node != nil {
		select {
		case <-h.timer.C:
			if _, err := h.Sync(time.Now()); err != nil {
				return false, err
			}
		default:
		}
	}
	if EnableLimit && h.enableLimit {
		if atomic.LoadInt64(&h.unsync) >= h.limit {
			return false, nil
		}
		if unsync := atomic.AddInt64(&h.unsync, 1); unsync <= h.limit {
			return true, nil
		}
		defer atomic.AddInt64(&h.unsync, -1)
		if _, err := h.Sync(time.Now()); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func (h *Hitter) Background() {
	if h.Node == nil {
		return
	}
	go h.Node.Background()
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
func (h *Hitter) clusterQPS() float64 {
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
func (h *Hitter) QPS() float64 {
	h.Sync(time.Now())
	if h.Node != nil {
		return h.clusterQPS()
	}
	return h.qps.Value()
}
func (h *Hitter) Close() error {
	defer h.timer.Stop()
	// if h.Node != nil {
	// 	h.Node.Close()
	// }
	_, err := h.Sync(time.Now())
	return err
}
