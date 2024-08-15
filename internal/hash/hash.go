package hash

import (
	"strconv"
	"strings"
	"sync"

	"stathat.com/c/consistent"
)

var (
	HashKeyIdx      = "idx"
	HashKeyExi      = "exi"
	HashKeyVarIdx   = "%idx"
	ShardKeyVarOrg  = "%org"
	ShardKeyVarBk   = "%bk"
	ShardKeyVarDb   = "%db"
	ShardKeyVarMm   = "%mm"
	ShardKeyOrgBkMm = "%org,%bk,%mm"
	ShardKeyDbMm    = "%db,%mm"
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
	freq  map[string]int
}

var ShardKeyVar = []string{ShardKeyVarOrg, ShardKeyVarBk, ShardKeyVarDb, ShardKeyVarMm}

func NewShardTpl(tpl string) *ShardTpl {
	st := &ShardTpl{tpl: tpl, freq: make(map[string]int)}
	for _, v := range ShardKeyVar {
		st.freq[v] = 0
	}
	for i := 0; i < len(tpl); {
		for j := i; j < len(tpl); {
			found := false
			for _, v := range ShardKeyVar {
				n := len(v)
				if j <= len(tpl)-n && tpl[j:j+n] == v {
					if j > i {
						st.parts = append(st.parts, tpl[i:j])
					}
					st.parts = append(st.parts, tpl[j:j+n])
					st.freq[tpl[j:j+n]]++
					i, j = j+n, j+n
					found = true
					break
				}
			}
			if found {
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
	b.Grow(len(st.tpl) + st.varDiffLen(db, ShardKeyVarDb) + st.varByteDiffLen(mm, ShardKeyVarMm))
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

func (st *ShardTpl) GetKeyV2(org, bk, mm string) string {
	var b strings.Builder
	b.Grow(len(st.tpl) + st.varDiffLen(org, ShardKeyVarOrg) + st.varDiffLen(bk, ShardKeyVarBk) + st.varDiffLen(mm, ShardKeyVarMm))
	for _, part := range st.parts {
		if part == ShardKeyVarOrg {
			b.WriteString(org)
		} else if part == ShardKeyVarBk {
			b.WriteString(bk)
		} else if part == ShardKeyVarMm {
			b.WriteString(mm)
		} else {
			b.WriteString(part)
		}
	}
	return b.String()
}

func (st *ShardTpl) varDiffLen(r string, v string) int {
	return (len(r) - len(v)) * st.freq[v]
}

func (st *ShardTpl) varByteDiffLen(r []byte, v string) int {
	return (len(r) - len(v)) * st.freq[v]
}
