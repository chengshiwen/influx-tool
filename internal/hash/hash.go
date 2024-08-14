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
	ShardKeyVarDb = "%db"
	ShardKeyVarMm = "%mm"
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

type Shard interface {
	GetKey(db string, mm []byte) string
}

type ShardTpl struct {
	tpl   string
	parts []string
	dbCnt int
	mmCnt int
}

func NewShardTpl(tpl string) *ShardTpl {
	st := &ShardTpl{tpl: tpl}
	for i := 0; i < len(tpl); {
		for j := i; j < len(tpl); {
			if j <= len(tpl)-3 && (tpl[j:j+3] == ShardKeyVarDb || tpl[j:j+3] == ShardKeyVarMm) {
				if j > i {
					st.parts = append(st.parts, tpl[i:j])
				}
				st.parts = append(st.parts, tpl[j:j+3])
				if tpl[j:j+3] == ShardKeyVarDb {
					st.dbCnt++
				} else if tpl[j:j+3] == ShardKeyVarMm {
					st.mmCnt++
				}
				i, j = j+3, j+3
				continue
			}
			j++
			if j == len(tpl) {
				st.parts = append(st.parts, tpl[i:j])
				i = j
				break
			}
		}
	}
	return st
}

func (st *ShardTpl) GetKey(db string, mm []byte) string {
	var b strings.Builder
	b.Grow(len(st.tpl) + (len(db)-len(ShardKeyVarDb))*st.dbCnt + (len(mm)-len(ShardKeyVarMm))*st.mmCnt)
	for _, part := range st.parts {
		if part == ShardKeyVarDb {
			b.WriteString(db)
		} else if part == ShardKeyVarMm {
			b.Write(mm)
		} else {
			b.WriteString(part)
		}
	}
	return b.String()
}
