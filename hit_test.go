package ghit

import "github.com/go-redis/redis/v8"

func ExampleHitter() {
	EnableLimit = true
	hitter, err := NewHitter(&Config{
		Redis:      redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		Key:        "hitted",
		ExpireSpec: "@midnight",
		Limit:      1000,
	})
	if err != nil {
		panic(err)
	}
	if err := hitter.Hit(); err != nil {
		panic(err) // overhit
	}
	// DO ANYTHING
}
