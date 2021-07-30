package transfer

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chengshiwen/influx-tool/internal/binary"
	"github.com/chengshiwen/influx-tool/internal/server"
	"github.com/djherbis/nio/v3"
	"github.com/spf13/cobra"
)

type command struct {
	cobraCmd        *cobra.Command
	sourceDir       string
	targetDir       string
	database        string
	retentionPolicy string
	duration        time.Duration
	shardDuration   time.Duration
	startTime       int64
	endTime         int64
	worker          int
	skipTsi         bool
	nodeTotal       int
	nodeIndex       intSet
	hashKey         string
}

func NewCommand() *cobra.Command {
	var start, end string
	cmd := &command{nodeIndex: make(intSet)}
	cmd.cobraCmd = &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "transfer",
		Short:         "Transfer influxdb persist data on disk from one to another",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.runE(start, end)
		},
	}
	flags := cmd.cobraCmd.Flags()
	flags.SortFlags = false
	flags.StringVarP(&cmd.sourceDir, "source-dir", "s", "", "source influxdb directory containing meta, data and wal (required)")
	flags.StringVarP(&cmd.targetDir, "target-dir", "t", "", "target influxdb directory containing meta, data and wal (required)")
	flags.StringVarP(&cmd.database, "database", "d", "", "database name (required)")
	flags.StringVarP(&cmd.retentionPolicy, "retention-policy", "r", "autogen", "retention policy")
	flags.DurationVar(&cmd.duration, "duration", time.Hour*0, "retention policy duration (default: 0)")
	flags.DurationVar(&cmd.shardDuration, "shard-duration", time.Hour*24*7, "retention policy shard duration")
	flags.StringVarP(&start, "start", "S", "", "start time to transfer (RFC3339 format, optional)")
	flags.StringVarP(&end, "end", "E", "", "end time to transfer (RFC3339 format, optional)")
	flags.IntVarP(&cmd.worker, "worker", "w", 0, "number of concurrent workers to transfer (default: 0, unlimited)")
	flags.BoolVar(&cmd.skipTsi, "skip-tsi", false, "skip building TSI index on disk (default: false)")
	flags.IntVarP(&cmd.nodeTotal, "node-total", "n", 1, "total number of node in target circle")
	flags.VarP(&cmd.nodeIndex, "node-index", "i", "index of node in target circle delimited by comma, [0, node-total) (default: all)")
	flags.StringVarP(&cmd.hashKey, "hash-key", "k", "idx", "hash key for influx proxy, valid options are idx or exi")
	cmd.cobraCmd.MarkFlagRequired("source-dir")
	cmd.cobraCmd.MarkFlagRequired("target-dir")
	cmd.cobraCmd.MarkFlagRequired("database")
	return cmd.cobraCmd
}

func (cmd *command) validate(start, end string) {
	if start != "" {
		s, err := time.Parse(time.RFC3339, start)
		if err != nil {
			log.Fatal("start time is invalid")
		}
		cmd.startTime = s.UnixNano()
	} else {
		cmd.startTime = math.MinInt64
	}
	if end != "" {
		e, err := time.Parse(time.RFC3339, end)
		if err != nil {
			log.Fatal("end time is invalid")
		}
		cmd.endTime = e.UnixNano()
	} else {
		cmd.endTime = math.MaxInt64
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		log.Fatal("end time before start time")
	}

	if cmd.worker < 0 {
		log.Fatal("worker is invalid")
	}
	if cmd.nodeTotal <= 0 {
		log.Fatal("node-total is invalid")
	}
	for idx := range cmd.nodeIndex {
		if idx < 0 || idx >= cmd.nodeTotal {
			log.Fatal("node-index is invalid")
		}
	}
	if len(cmd.nodeIndex) == 0 {
		for idx := 0; idx < cmd.nodeTotal; idx++ {
			cmd.nodeIndex[idx] = struct{}{}
		}
	}
	if cmd.hashKey != "idx" && cmd.hashKey != "exi" {
		log.Fatal("hash-key is invalid")
	}
}

func (cmd *command) runE(start, end string) (err error) {
	cmd.validate(start, end)

	exportServer, err := server.NewServer(cmd.sourceDir, !cmd.skipTsi)
	if err != nil {
		return
	}
	defer exportServer.Close()
	exp, err := newExporter(exportServer, cmd.database, cmd.retentionPolicy, cmd.shardDuration, cmd.startTime, cmd.endTime)
	if err != nil {
		return
	}

	svrs := make(map[int]*server.Server)
	imps := make(map[int]*importer)
	defer func() {
		for _, imp := range imps {
			imp.Close()
		}
		for _, svr := range svrs {
			svr.Close()
		}
	}()
	for idx := range cmd.nodeIndex {
		importServer, err := server.NewServer(fmt.Sprintf("%s-%d", cmd.targetDir, idx), !cmd.skipTsi)
		if err != nil {
			return err
		}
		svrs[idx] = importServer
		imp, err := newImporter(importServer, cmd.database, cmd.retentionPolicy, cmd.shardDuration, cmd.duration, !cmd.skipTsi)
		if err != nil {
			return err
		}
		imps[idx] = imp
	}

	cmd.transfer(exp, imps)
	return
}

func (cmd *command) transfer(exp *exporter, imps map[int]*importer) {
	log.SetFlags(log.LstdFlags)
	log.Printf("transfer node total: %d, node index: %s, hash key: %s", cmd.nodeTotal, cmd.nodeIndex, cmd.hashKey)
	start := time.Now().UTC()
	defer func() {
		elapsed := time.Since(start)
		if elapsed.Minutes() > 10 {
			log.Printf("total time: %0.1f minutes", elapsed.Minutes())
		} else {
			log.Printf("total time: %0.1f seconds", elapsed.Seconds())
		}
	}()

	prChans := make(map[int]chan *nio.PipeReader)
	for idx := range cmd.nodeIndex {
		prChans[idx] = make(chan *nio.PipeReader, 4)
	}

	go func() {
		defer func() {
			for _, prChan := range prChans {
				close(prChan)
			}
		}()
		exp.WriteTo(prChans, cmd.nodeTotal, cmd.hashKey, cmd.worker)
	}()

	wg := &sync.WaitGroup{}
	for idx := range imps {
		wg.Add(1)
		idx := idx
		go func() {
			defer wg.Done()
			cmd.transferNode(imps[idx], prChans[idx], idx)
		}()
	}
	wg.Wait()
	log.Print("transfer done")
}

func (cmd *command) transferNode(imp *importer, prChan chan *nio.PipeReader, idx int) {
	log.Printf("node index %d transfer start", idx)
	wg := &sync.WaitGroup{}
	for pr := range prChan {
		wg.Add(1)
		pr := pr
		go func() {
			defer wg.Done()
			defer pr.Close()

			iw := newImportWorker(imp)

			reader := binary.NewReader(pr)
			_, err := reader.ReadHeader()
			if err != nil {
				log.Printf("read header error: %s", err)
				return
			}

			var bh *binary.BucketHeader
			for bh, err = reader.NextBucket(); (bh != nil) && (err == nil); bh, err = reader.NextBucket() {
				err = iw.ImportShard(reader, bh.Start, bh.End)
				if err != nil {
					log.Printf("import shard error: %s, idx: %d", err, idx)
					return
				}
			}
			if err != nil {
				log.Printf("next bucket error: %s", err)
				return
			}
		}()
	}
	wg.Wait()
	log.Printf("node index %d transfer done", idx)
}

type intSet map[int]struct{}

func (is intSet) Type() string {
	return "intset"
}

func (is intSet) String() string {
	values := make([]int, 0, len(is))
	for k := range is {
		values = append(values, k)
	}
	sort.Ints(values)
	return strings.Trim(fmt.Sprint(values), "[]")
}

func (is intSet) Set(v string) error {
	v = strings.Trim(v, ", ")
	if v != "" {
		splits := strings.Split(v, ",")
		for _, s := range splits {
			i, err := strconv.Atoi(s)
			if err != nil {
				return err
			}
			is[i] = struct{}{}
		}
	}
	return nil
}
