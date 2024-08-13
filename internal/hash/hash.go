package hash

import (
	"strconv"
	"strings"
	"sync"

	"stathat.com/c/consistent"
)

var (
	HashKeyIdx    = "idx"
	HashKeyExi    = "exi"
	HashKeyVarIdx = "%idx"
)

type Hash interface {
	Get(key string) int
}

type ConsistentHash struct {
	consistent *consistent.Consistent
	mapToIdx   map[string]int
	cache      sync.Map
}

func NewConsistentHash(nodeTotal int, hashKey string) *ConsistentHash {
	ch := &ConsistentHash{
		consistent: consistent.New(),
		mapToIdx:   make(map[string]int),
	}
	ch.consistent.NumberOfReplicas = 256
	for idx := 0; idx < nodeTotal; idx++ {
		var key string
		switch hashKey {
		case HashKeyExi:
			// exi: extended index, no hash collision will occur before idx <= 100000, which has been tested
			key = "|" + strconv.Itoa(idx)
		case HashKeyIdx:
			// idx: index, each additional backend causes 10% hash collision from 11th backend
			key = strconv.Itoa(idx)
		default:
			// %idx: custom template like "backend-%idx"
			key = strings.ReplaceAll(hashKey, HashKeyVarIdx, strconv.Itoa(idx))
		}
		ch.consistent.Add(key)
		ch.mapToIdx[key] = idx
	}
	return ch
}

func (ch *ConsistentHash) Get(key string) int {
	if idx, ok := ch.cache.Load(key); ok {
		return idx.(int)
	}
	str, _ := ch.consistent.Get(key)
	idx := ch.mapToIdx[str]
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
