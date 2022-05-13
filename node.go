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
	DefaultNodesKeySuffix = ":NODES"
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
}
type Node struct {
	redis          redis.Cmdable
	keyPrefix      string
	nodesKey       string
	nodeID         string
	expireInterval time.Duration
	timer          *time.Timer
}

func NewNode(opt *NodeOption) *Node {
	nodesKey := opt.KeyPrefix + DefaultNodesKeySuffix
	if opt.NodesKeySuffix != "" {
		nodesKey = opt.KeyPrefix + opt.NodesKeySuffix
	}
	n := Node{
		redis:          opt.Redis,
		keyPrefix:      opt.KeyPrefix,
		nodesKey:       nodesKey,
		nodeID:         opt.NodeID,
		expireInterval: opt.ExpireInterval,
		timer:          time.NewTimer(time.Duration(float64(opt.ExpireInterval) * 0.9)),
	}
	return &n
}
func (n *Node) clean() error {
	return n.redis.ZRemRangeByScore(
		context.Background(),
		n.nodesKey,
		"0",
		strconv.FormatInt(time.Now().Add(-n.expireInterval).Unix(), 10),
	).Err()
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
	n.clean()
	n.timer.Reset(time.Duration(float64(n.expireInterval) * 0.9))
	return n.redis.ZAdd(
		context.Background(),
		n.nodesKey,
		&redis.Z{
			Member: n.NodeID(),
			Score:  float64(time.Now().Unix()),
		},
	).Err()
}
func (n *Node) Dead() error {
	defer n.redis.Expire(
		context.Background(),
		n.nodesKey,
		n.expireInterval,
	)
	return n.redis.ZRem(
		context.Background(),
		n.nodesKey,
		&redis.Z{
			Member: n.NodeID(),
		},
	).Err()
}
func (n *Node) NodeIDs() ([]string, error) {
	defer n.clean()
	return n.redis.ZRangeByScore(
		context.Background(),
		n.nodesKey,
		&redis.ZRangeBy{
			Min: strconv.FormatInt(time.Now().Add(-n.expireInterval).Unix(), 10),
			Max: strconv.FormatInt(time.Now().Unix(), 10),
		},
	).Result()
}
func (h *Node) Background() {
	for {
		select {
		case _, ok := <-h.timer.C:
			if !ok {
				return
			}
			h.Heartbeat()
		}
	}
}
func (n *Node) Close() error {
	n.timer.Stop()
	return n.Dead()
}
