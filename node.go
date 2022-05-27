package ghit

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

const (
	DefaultNodesKeySuffix = "NODES"
	DefaultNodeExpire     = time.Hour
)

var (
	NodeID string
)

func init() {
	NodeID = hex.EncodeToString(uuid.NodeID())
}

type NodeOption struct {
	Redis          redis.Cmdable
	KeyPrefix      string
	NodesKeySuffix string
	NodeID         string
	ExpireInterval time.Duration

	HeartbeatHook func(*Node)
	DeadHook      func(*Node)
}
type Node struct {
	redis          redis.Cmdable
	keyPrefix      string
	nodesKey       string
	nodeID         string
	expireInterval time.Duration
	timer          *time.Timer

	heartbeatHook func(*Node)
	deadHook      func(*Node)
}

func NewNode(opt *NodeOption) (*Node, error) {
	nodesKey := opt.KeyPrefix + DefaultNodesKeySuffix
	if opt.NodesKeySuffix != "" {
		nodesKey = opt.KeyPrefix + opt.NodesKeySuffix
	}
	expireInterval := DefaultNodeExpire
	if opt.ExpireInterval != 0 {
		expireInterval = opt.ExpireInterval
	}
	n := Node{
		redis:          opt.Redis,
		keyPrefix:      opt.KeyPrefix,
		nodesKey:       nodesKey,
		nodeID:         opt.NodeID,
		expireInterval: expireInterval,
		timer:          time.NewTimer(time.Duration(float64(opt.ExpireInterval) * 0.9)),

		heartbeatHook: opt.HeartbeatHook,
		deadHook:      opt.DeadHook,
	}
	return &n, n.redis.Ping(context.Background()).Err()
}
func (n *Node) NodeID() string {
	if n.nodeID != "" {
		return n.nodeID
	}
	return NodeID
}

func (n *Node) Heartbeat() error {
	defer n.redis.Expire(
		context.Background(),
		n.nodesKey,
		n.expireInterval,
	)
	n.timer.Reset(time.Duration(float64(n.expireInterval) * 0.9))
	if n.heartbeatHook != nil {
		defer n.heartbeatHook(n)
	}
	return n.redis.ZAdd(
		context.Background(),
		n.nodesKey,
		&redis.Z{
			Member: n.NodeID(),
			Score:  float64(time.Now().UnixMilli()),
		},
	).Err()
}
func (n *Node) Dead() error {
	defer n.redis.Expire(
		context.Background(),
		n.nodesKey,
		n.expireInterval,
	)
	if n.deadHook != nil {
		defer n.deadHook(n)
	}
	return n.redis.ZRem(
		context.Background(),
		n.nodesKey,
		n.NodeID(),
	).Err()
}
func (n *Node) Clean() error {
	defer n.redis.Expire(
		context.Background(),
		n.nodesKey,
		n.expireInterval,
	)
	return n.redis.ZRemRangeByScore(
		context.Background(),
		n.nodesKey,
		"0",
		strconv.FormatInt(time.Now().Add(-n.expireInterval).UnixMilli(), 10),
	).Err()
}
func (n *Node) NodeIDs() ([]string, error) {
	defer n.redis.Expire(
		context.Background(),
		n.nodesKey,
		n.expireInterval,
	)
	return n.redis.ZRangeByScore(
		context.Background(),
		n.nodesKey,
		&redis.ZRangeBy{
			Min: strconv.FormatInt(time.Now().Add(-n.expireInterval).UnixMilli(), 10),
			Max: strconv.FormatInt(time.Now().UnixMilli(), 10),
		},
	).Result()
}
func (n *Node) Background() {
	for {
		select {
		case _, ok := <-n.timer.C:
			if !ok {
				return
			}
			n.Heartbeat()
			n.Clean()
		}
	}
}
func (n *Node) Close() error {
	n.timer.Stop()
	defer n.Clean()
	return n.Dead()
}
