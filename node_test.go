package ghit

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestNode(t *testing.T) {
	address := os.Getenv("REDIS_ADDRESS")
	if address == "" {
		t.Skip()
	}
	keyPrefix := strconv.FormatUint(rand.Uint64(), 16) + ":"
	nodesKey := keyPrefix + DefaultNodesKeySuffix

	nodeID := strconv.FormatUint(rand.Uint64(), 16)

	expireInterval := time.Second * 2

	node, err := NewNode(&NodeOption{
		Redis:          redis.NewClient(&redis.Options{Addr: address}),
		KeyPrefix:      keyPrefix,
		NodeID:         nodeID,
		ExpireInterval: expireInterval,
	})
	defer node.Close()
	assert.NoError(t, err)
	assert.NotNil(t, node)

	assert.Equal(t, 0, int(node.redis.ZCard(context.Background(), nodesKey).Val()))

	assert.NoError(t, node.Heartbeat())
	assert.Equal(t, 1, int(node.redis.ZCard(context.Background(), nodesKey).Val()))
	heartbeat := time.UnixMilli(int64(node.redis.ZScore(context.Background(), nodesKey, nodeID).Val()))
	assert.True(t, time.Now().After(heartbeat) && time.Now().Add(-expireInterval).Before(heartbeat), []time.Time{
		time.Now().Add(-expireInterval),
		heartbeat,
		time.Now(),
	})

	time.Sleep(expireInterval)
	assert.Equal(t, 0, int(node.redis.ZCard(context.Background(), nodesKey).Val()))

	assert.NoError(t, node.Heartbeat())
	assert.Equal(t, 1, int(node.redis.ZCard(context.Background(), nodesKey).Val()))
	heartbeat = time.UnixMilli(int64(node.redis.ZScore(context.Background(), nodesKey, nodeID).Val()))
	assert.True(t, time.Now().After(heartbeat) && time.Now().Add(-expireInterval).Before(heartbeat), []time.Time{
		time.Now().Add(-expireInterval),
		heartbeat,
		time.Now(),
	})

	assert.NoError(t, node.Dead())
	assert.Equal(t, 0, int(node.redis.ZCard(context.Background(), nodesKey).Val()))

	assert.NoError(t, node.Heartbeat())
	assert.Equal(t, 1, int(node.redis.ZCard(context.Background(), nodesKey).Val()))
	heartbeat0 := time.UnixMilli(int64(node.redis.ZScore(context.Background(), nodesKey, nodeID).Val()))
	assert.True(t, time.Now().After(heartbeat0) && time.Now().Add(-expireInterval).Before(heartbeat0), []time.Time{
		time.Now().Add(-expireInterval),
		heartbeat0,
		time.Now(),
	})
	assert.NoError(t, node.Heartbeat())
	assert.Equal(t, 1, int(node.redis.ZCard(context.Background(), nodesKey).Val()))
	heartbeat1 := time.UnixMilli(int64(node.redis.ZScore(context.Background(), nodesKey, nodeID).Val()))
	assert.True(t, time.Now().After(heartbeat1) && time.Now().Add(-expireInterval).Before(heartbeat1), []time.Time{
		time.Now().Add(-expireInterval),
		heartbeat1,
		time.Now(),
	})
	assert.True(t, heartbeat1.After(heartbeat0), []time.Time{heartbeat0, heartbeat1})
}

func ExampleNode() {
	expireInterval := time.Hour
	node, err := NewNode(&NodeOption{
		Redis:          redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		KeyPrefix:      "ghit:",
		ExpireInterval: expireInterval,
	})
	if err != nil {
		panic(err)
	}
	defer node.Close()
	node.Heartbeat()
	defer node.Dead()
	for {
		select {
		case <-time.After(expireInterval):
			node.Heartbeat()
		}
	}
}
