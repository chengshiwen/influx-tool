package transfer

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/chengshiwen/influx-tool/internal/binary"
	"github.com/chengshiwen/influx-tool/internal/escape"
	"github.com/chengshiwen/influx-tool/internal/hash"
	"github.com/chengshiwen/influx-tool/internal/server"
	"github.com/chengshiwen/influx-tool/internal/storage"
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

type exporter struct {
	tsdbConfig   tsdb.Config
	db, rp       string
	sd           time.Duration
	sourceGroups []meta.ShardGroupInfo
	targetGroups []meta.ShardGroupInfo
}

func newExporter(svr *server.Server, db, rp string, sd time.Duration, start, end int64) (*exporter, error) {
	client := svr.MetaClient()

	dbi := client.Database(db)
	if dbi == nil {
		return nil, fmt.Errorf("database '%s' does not exist", db)
	}

	if rp == "" {
		// select default rp
		rp = dbi.DefaultRetentionPolicy
	}

	rpi, err := client.RetentionPolicy(db, rp)
	if rpi == nil || err != nil {
		return nil, fmt.Errorf("retention policy '%s' does not exist", rp)
	}

	e := &exporter{
		tsdbConfig: svr.TSDBConfig(),
		db:         db,
		rp:         rp,
		sd:         sd,
	}

	// load shard groups
	min := time.Unix(0, models.MinNanoTime)
	max := time.Unix(0, models.MaxNanoTime)
	groups, err := client.ShardGroupsByTimeRange(db, rp, min, max)
	if err != nil {
		return nil, err
	}
	if len(groups) > 0 {
		sort.Sort(meta.ShardGroupInfos(groups))
		e.sourceGroups = groups
		e.targetGroups = planShardGroups(groups, sd, start, end)
	}

	return e, nil
}

func (e *exporter) SourceShardGroups() []meta.ShardGroupInfo { return e.sourceGroups }
func (e *exporter) TargetShardGroups() []meta.ShardGroupInfo { return e.targetGroups }

func (e *exporter) WriteTo(prChans map[int]chan *nio.PipeReader, nodeTotal int, hashKey string, worker int) {
	log.Printf("total shard groups: %d", len(e.targetGroups))
	limit := make(chan struct{}, worker)
	ch := hash.NewConsistentHash(nodeTotal, hashKey)
	wg := &sync.WaitGroup{}
	for _, g := range e.targetGroups {
		g := g
		min, max := g.StartTime, g.EndTime
		wg.Add(1)
		go func() {
			if worker > 0 {
				limit <- struct{}{}
			}
			defer func() {
				wg.Done()
				if worker > 0 {
					<-limit
				}
			}()

			ew := newExportWorker(e)
			err := ew.Open()
			if err != nil {
				log.Printf("export worker open error: %s, shard group: %d, min: %d, max: %d", err, g.ID, min.Unix(), max.Unix())
				return
			}
			defer ew.Close()
			rs, err := ew.read(min, max.Add(-1))
			if err != nil {
				log.Printf("export worker read error: %s, shard group: %d, min: %d, max: %d", err, g.ID, min.Unix(), max.Unix())
				return
			}
			if rs == nil {
				log.Printf("export worker read rs is nil, shard group: %d, min: %d, max: %d", g.ID, min.Unix(), max.Unix())
				return
			}
			defer rs.Close()

			err = e.writeBucket(prChans, rs, min, max, ch)
			if err != nil {
				log.Printf("export worker write error: %s, shard group: %d, min: %d, max: %d", err, g.ID, min.Unix(), max.Unix())
			}
			log.Printf("shard group done: %d", g.ID)
		}()
	}
	wg.Wait()
	log.Print("all shard groups done")
}

func (e *exporter) writeBucket(prChans map[int]chan *nio.PipeReader, rs *storage.ResultSet, min, max time.Time, h hash.Hash) error {
	pws := make(map[int]*nio.PipeWriter)
	wrs := make(map[int]*binary.Writer)
	bws := make(map[int]*binary.BucketWriter)
	defer func() {
		for _, bw := range bws {
			bw.Close()
		}
		for _, wr := range wrs {
			wr.Close()
		}
		for _, pw := range pws {
			pw.Close()
		}
	}()

	for rs.Next() {
		if escape.NeedEscape(rs.Name(), rs.Tags()) {
			log.Printf("discard escaped measurement: %s, tags: %s", rs.Name(), rs.Tags())
			continue
		}
		nodeIndex := h.Get(hash.GetKey(e.db, rs.Name()))
		if prChan, pok := prChans[nodeIndex]; pok {
			if _, bok := bws[nodeIndex]; !bok {
				buf := buffer.New(int64(4 * 1024 * 1024))
				pr, pw := nio.Pipe(buf)
				pws[nodeIndex] = pw
				wr := binary.NewWriter(pw, e.db, e.rp, e.sd)
				wrs[nodeIndex] = wr
				bw, err := wr.NewBucket(min.UnixNano(), max.UnixNano())
				if err != nil {
					return err
				}
				bws[nodeIndex] = bw
				prChan <- pr
			}
			bw := bws[nodeIndex]
			err := bw.WriteSeries(rs.Name(), rs.Field(), rs.FieldType(), rs.Tags(), rs.CursorIterator())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type exportWorker struct {
	*exporter
	tsdbStore *tsdb.Store
	store     *storage.Store
}

func newExportWorker(exporter *exporter) *exportWorker {
	tsdbConfig := exporter.tsdbConfig
	store := tsdb.NewStore(tsdbConfig.Dir)
	store.EngineOptions.MonitorDisabled = true
	store.EngineOptions.CompactionDisabled = true
	store.EngineOptions.Config = tsdbConfig
	store.EngineOptions.EngineVersion = tsdbConfig.Engine
	store.EngineOptions.IndexVersion = tsdbConfig.Index
	store.EngineOptions.DatabaseFilter = func(database string) bool {
		return database == exporter.db
	}
	store.EngineOptions.RetentionPolicyFilter = func(_, rp string) bool {
		return rp == exporter.rp
	}
	store.EngineOptions.ShardFilter = func(_, _ string, _ uint64) bool {
		return false
	}

	return &exportWorker{
		exporter:  exporter,
		tsdbStore: store,
		store:     &storage.Store{TSDBStore: store},
	}
}

func (e *exportWorker) Open() (err error) {
	err = e.tsdbStore.Open()
	if err != nil {
		return err
	}
	return nil
}

func (e *exportWorker) Close() error {
	return e.tsdbStore.Close()
}

// Read creates a ResultSet that reads all points with a timestamp ts, such that start ≤ ts < end.
func (e *exportWorker) read(min, max time.Time) (*storage.ResultSet, error) {
	shards, err := e.getShards(min, max)
	if err != nil {
		return nil, err
	}

	req := storage.ReadRequest{
		Database: e.db,
		RP:       e.rp,
		Shards:   shards,
		Start:    min.UnixNano(),
		End:      max.UnixNano(),
	}

	return e.store.Read(context.Background(), &req)
}

func (e *exportWorker) getShards(min, max time.Time) ([]*tsdb.Shard, error) {
	groups := e.shardsGroupsByTimeRange(min, max)
	var ids []uint64
	for _, g := range groups {
		for _, s := range g.Shards {
			ids = append(ids, s.ID)
		}
	}

	shards := e.tsdbStore.Shards(ids)
	if len(shards) == len(ids) {
		return shards, nil
	}

	return e.openStoreWithShardsIDs(ids)
}

func (e *exportWorker) shardsGroupsByTimeRange(min, max time.Time) []meta.ShardGroupInfo {
	groups := make([]meta.ShardGroupInfo, 0, len(e.sourceGroups))
	for _, g := range e.sourceGroups {
		if !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups
}

func (e *exportWorker) openStoreWithShardsIDs(ids []uint64) ([]*tsdb.Shard, error) {
	e.tsdbStore.Close()
	e.tsdbStore.EngineOptions.ShardFilter = func(_, _ string, id uint64) bool {
		for i := range ids {
			if id == ids[i] {
				return true
			}
		}
		return false
	}
	if err := e.tsdbStore.Open(); err != nil {
		return nil, err
	}
	return e.tsdbStore.Shards(ids), nil
}
