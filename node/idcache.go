package node

import (
	"sync"

	"github.com/wuyazero/Elastos.ELA.SideChain/protocol"

	. "github.com/wuyazero/Elastos.ELA.Utility/common"
)

type idCache struct {
	sync.RWMutex
	lastid    Uint256
	index     int
	idarray   []Uint256
	idmaplsit map[Uint256]int
}

func (c *idCache) init() {
	c.index = 0
	c.idmaplsit = make(map[Uint256]int, protocol.MaxIdCached)
	c.idarray = make([]Uint256, protocol.MaxIdCached)
}

func (c *idCache) add(id Uint256) {
	oldid := c.idarray[c.index]
	delete(c.idmaplsit, oldid)
	c.idarray[c.index] = id
	c.idmaplsit[id] = c.index
	c.index++
	c.lastid = id
	c.index = c.index % protocol.MaxIdCached
}

func (c *idCache) ExistedID(id Uint256) bool {
	// TODO
	c.Lock()
	defer c.Unlock()
	if id == c.lastid {
		return true
	}
	if _, ok := c.idmaplsit[id]; ok {
		return true
	} else {
		c.add(id)
	}
	return false
}
