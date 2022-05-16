package ghit

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func ExampleHitter() {
	hitter, err := NewHitter(&HitterOption{
		NodeOption: &NodeOption{
			Redis:     redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
			KeyPrefix: "ghit:",
		},
		ExpireSpec:     "@midnight",
		QPSShakeCoeff:  2,
		FollowInterval: time.Hour * 24,
		Key:            "run",
		EnableLimit:    true,
		Limit:          1,
	})
	if err != nil {
		panic(err)
	}
	defer hitter.Close()
	if ok, err := hitter.Hit(); err != nil {
		// handle error
	} else if !ok {
		return
	}
}

func TestHitter(t *testing.T) {
	address := os.Getenv("REDIS_ADDRESS")
	if address == "" {
		t.Skip()
	}
	keyPrefix := strconv.FormatUint(rand.Uint64(), 16) + ":"
	key := strconv.FormatUint(rand.Uint64(), 16)

	nodeID := strconv.FormatUint(rand.Uint64(), 16)

	expireInterval := time.Second * 3

	EnableLimit = true
	hitter, err := NewHitter(&HitterOption{
		NodeOption: &NodeOption{
			Redis:          redis.NewClient(&redis.Options{Addr: address}),
			KeyPrefix:      keyPrefix,
			NodeID:         nodeID,
			ExpireInterval: expireInterval,
		},
		ExpireSpec:     "@every " + expireInterval.String(),
		FollowInterval: time.Minute,
		Key:            key,
		EnableLimit:    true,
		Limit:          5,
	})
	defer hitter.Close()
	assert.NoError(t, err)
	assert.NotNil(t, hitter)

	now := time.Now()
	for ; now.Before(hitter.expireAt); now = time.Now() {
	}
	assert.Equal(t, int64(0), hitter.Hitted())
	for i := int64(0); i < hitter.limit; i += 1 {
		ok, err := hitter.Hit()
		assert.NoError(t, err)
		assert.True(t, ok, []interface{}{hitter.synced, hitter.unsync, hitter.limit})
	}
	assert.Equal(t, hitter.limit, hitter.Hitted())
	ok, err := hitter.Hit()
	assert.NoError(t, err)
	assert.False(t, ok, []interface{}{hitter.synced, hitter.unsync, hitter.limit})

	for j := 0; j < 2; j += 1 {
		now := time.Now()
		for ; now.Before(hitter.expireAt); now = time.Now() {
		}

		assert.True(t, now.After(hitter.hittedAt), []time.Time{hitter.hittedAt, now})
		assert.True(t, now.After(hitter.expireAt), []time.Time{hitter.expireAt, now})

		assert.Equal(t, int64(0), hitter.Hitted())
		ok, err = hitter.Hit()
		assert.NoError(t, err)
		assert.True(t, ok, []interface{}{hitter.synced, hitter.unsync, hitter.limit})

		assert.True(t, hitter.hittedAt.After(now), []time.Time{now, hitter.hittedAt})
		assert.True(t, hitter.hittedAt.Before(hitter.expireAt), []time.Time{hitter.hittedAt, hitter.expireAt})
		assert.Equal(t, int64(1), hitter.Hitted())
	}
}
func TestHitterSingleNode(t *testing.T) {
	key := strconv.FormatUint(rand.Uint64(), 16)

	expireInterval := time.Second * 3

	EnableLimit = true
	hitter, err := NewHitter(&HitterOption{
		ExpireSpec:     "@every " + expireInterval.String(),
		FollowInterval: time.Minute,
		Key:            key,
		EnableLimit:    true,
		Limit:          5,
	})
	defer hitter.Close()
	assert.NoError(t, err)
	assert.NotNil(t, hitter)

	now := time.Now()
	for ; now.Before(hitter.expireAt); now = time.Now() {
	}
	assert.Equal(t, int64(0), hitter.Hitted())
	for i := int64(0); i < hitter.limit; i += 1 {
		ok, err := hitter.Hit()
		assert.NoError(t, err)
		assert.True(t, ok, []interface{}{hitter.synced, hitter.unsync, hitter.limit})
	}
	assert.Equal(t, hitter.limit, hitter.Hitted())
	ok, err := hitter.Hit()
	assert.NoError(t, err)
	assert.False(t, ok, []interface{}{hitter.synced, hitter.unsync, hitter.limit})

	for j := 0; j < 2; j += 1 {
		now := time.Now()
		for ; now.Before(hitter.expireAt); now = time.Now() {
		}

		assert.True(t, now.After(hitter.hittedAt), []time.Time{hitter.hittedAt, now})
		assert.True(t, now.After(hitter.expireAt), []time.Time{hitter.expireAt, now})

		assert.Equal(t, int64(0), hitter.Hitted())
		ok, err = hitter.Hit()
		assert.NoError(t, err)
		assert.True(t, ok, []interface{}{hitter.synced, hitter.unsync, hitter.limit})

		assert.True(t, hitter.hittedAt.After(now), []time.Time{now, hitter.hittedAt})
		assert.True(t, hitter.hittedAt.Before(hitter.expireAt), []time.Time{hitter.hittedAt, hitter.expireAt})
		assert.Equal(t, int64(1), hitter.Hitted())
	}
}
