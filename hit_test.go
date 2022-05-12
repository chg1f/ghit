package ghit

import (
	"time"

	"github.com/go-redis/redis/v8"
)

func ExampleHitter() {
	hitter, err := NewHitter(&HitterOption{
		NodeOption: NodeOption{
			Redis:          redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
			KeyPrefix:      "ghit:",
			ExpireInterval: time.Hour,
		},
		ExpireSpec:      "@midnight",
		ShakeRate:       0.5,
		EnableMaxSync:   true,
		MaxSyncInterval: time.Minute,
		EnableMinSync:   true,
		MinSyncInterval: time.Second,
		FollowInterval:  time.Hour * 24 * 30,
		Key:             "hitted",
		EnableLimit:     true,
		Limit:           1000,
	})
	if err != nil {
		panic(err)
	}
	if ok, err := hitter.Hit(); err != nil {
		panic(err)
	} else if !ok {
		return // XXX: OVERHIT
	}
	// DO ANYTHING
}
