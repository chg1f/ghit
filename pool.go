package ghit

import (
	"sync"

	"github.com/chg1f/errorx"
)

type PoolOption struct {
	*NodeOption
}
type Pool struct {
	*Node
	m sync.Map
}

func NewPool(opt *PoolOption) (*Pool, error) {
	p := Pool{}
	node, err := NewNode(opt.NodeOption)
	if err != nil {
		return nil, err
	}
	p.Node = node
	return &p, nil
}

func (p *Pool) Store(opt *HitterOption) (*Hitter, error) {
	opt.NodeOption = nil
	h, err := NewHitter(opt)
	if err != nil {
		return nil, err
	}
	h.Node = p.Node
	p.m.Store(h.key, h)
	return h, nil
}
func (p *Pool) LoadOrStore(opt *HitterOption) (*Hitter, error) {
	if v, ok := p.m.Load(opt.Key); ok {
		if h, ok := v.(*Hitter); ok {
			return h, nil
		}
	}
	opt.NodeOption = nil
	h, err := NewHitter(opt)
	if err != nil {
		return nil, err
	}
	h.Node = p.Node
	p.m.Store(h.key, h)
	return h, nil
}
func (p *Pool) Hit(opt *HitterOption) (bool, error) {
	h, err := p.LoadOrStore(opt)
	if err != nil {
		return false, err
	}
	return h.Hit()
}

// func (p *Pool) Sync(opt *HitterOption, now time.Time) (time.Time, error) {
// 	h, err := p.LoadOrStore(opt)
// 	if err != nil {
// 		return now, err
// 	}
// 	return h.Sync(now)
// }

func (p *Pool) ForEach(f func(string, *Hitter) bool) {
	p.m.Range(func(_, v any) bool {
		if h, ok := v.(*Hitter); ok {
			return f(h.key, h)
		}
		return true
	})
}
func (p *Pool) Close() error {
	var ex error
	p.ForEach(func(_ string, h *Hitter) bool {
		ex = errorx.Compress(ex, h.Close())
		return true
	})
	return ex
}
