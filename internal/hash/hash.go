package hash

import (
	"strconv"
	"strings"
	"sync"

	"stathat.com/c/consistent"
)

type Hash interface {
	Get(key string) int
}

type ConsistentHash struct {
	consistent *consistent.Consistent
	cache      sync.Map
}

func NewConsistentHash(nodeTotal int) *ConsistentHash {
	ch := &ConsistentHash{
		consistent: consistent.New(),
	}
	ch.consistent.NumberOfReplicas = 256
	for idx := 0; idx < nodeTotal; idx++ {
		str := strconv.Itoa(idx)
		ch.consistent.Add(str)
	}
	return ch
}

func (ch *ConsistentHash) Get(key string) int {
	if idx, ok := ch.cache.Load(key); ok {
		return idx.(int)
	}
	str, _ := ch.consistent.Get(key)
	idx, _ := strconv.Atoi(str)
	ch.cache.Store(key, idx)
	return idx
}

func GetKey(db string, meas []byte) string {
	var b strings.Builder
	b.Grow(len(db) + len(meas) + 1)
	b.WriteString(db)
	b.WriteString(",")
	b.Write(meas)
	return b.String()
}
