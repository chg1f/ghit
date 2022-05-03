GHit
---

## Intro

Low-deviation, Lazy-loading, Lock-free, Smart, Distributed counter and limiter

## Example

```go
import (
	"github.com/go-redis/redis/v8"
	"github.com/chg1f/ghit"
)

var (
	hitter *ghit.Hitter
)

func init() {
	ghit.EnableLimit = true
	var (
		err error
	)
	hitter, err = ghit.NewHitter(&ghit.Config{
		Redis:       redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		Key:         "hitted",
		ExpireSpec:  "@midnight",
		EnableLimit: true,
		Limit:       1000,
	})
	if err != nil {
		panic(err)
	}
}

func run() error {
	if err := hitter.Hit(); err != nil {
		return err // ErrOverhit
	}
	// DO ANYTHING
	return nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
```

> Document: https://pkg.go.dev/github.com/chg1f/ghit
