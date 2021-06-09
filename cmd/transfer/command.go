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

type flagpole struct {
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
	flags := &flagpole{nodeIndex: make(intSet)}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "transfer",
		Short:         "Transfer influxdb persist data on disk from one to another",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			processFlags(flags, start, end)
			return runE(flags)
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVarP(&flags.sourceDir, "source-dir", "s", "", "source influxdb directory containing meta, data and wal (required)")
	cmd.Flags().StringVarP(&flags.targetDir, "target-dir", "t", "", "target influxdb directory containing meta, data and wal (required)")
	cmd.Flags().StringVarP(&flags.database, "database", "d", "", "database name (required)")
	cmd.Flags().StringVarP(&flags.retentionPolicy, "retention-policy", "r", "autogen", "retention policy")
	cmd.Flags().DurationVar(&flags.duration, "duration", time.Hour*0, "retention policy duration (default: 0)")
	cmd.Flags().DurationVar(&flags.shardDuration, "shard-duration", time.Hour*24*7, "retention policy shard duration")
	cmd.Flags().StringVarP(&start, "start", "S", "", "start time to transfer (RFC3339 format, optional)")
	cmd.Flags().StringVarP(&end, "end", "E", "", "end time to transfer (RFC3339 format, optional)")
	cmd.Flags().IntVarP(&flags.worker, "worker", "w", 0, "number of concurrent workers to transfer (default: 0, unlimited)")
	cmd.Flags().BoolVar(&flags.skipTsi, "skip-tsi", false, "skip building TSI index on disk (default: false)")
	cmd.Flags().IntVarP(&flags.nodeTotal, "node-total", "n", 1, "total number of node in target circle")
	cmd.Flags().VarP(&flags.nodeIndex, "node-index", "i", "index of node in target circle delimited by comma, [0, node-total) (default: all)")
	cmd.Flags().StringVarP(&flags.hashKey, "hash-key", "k", "idx", "hash key for influx proxy, valid options are idx or exi")
	cmd.MarkFlagRequired("source-dir")
	cmd.MarkFlagRequired("target-dir")
	cmd.MarkFlagRequired("database")
	return cmd
}

func processFlags(flags *flagpole, start, end string) {
	if start != "" {
		s, err := time.Parse(time.RFC3339, start)
		if err != nil {
			log.Fatal("start time is invalid")
		}
		flags.startTime = s.UnixNano()
	} else {
		flags.startTime = math.MinInt64
	}
	if end != "" {
		e, err := time.Parse(time.RFC3339, end)
		if err != nil {
			log.Fatal("end time is invalid")
		}
		flags.endTime = e.UnixNano()
	} else {
		flags.endTime = math.MaxInt64
	}
	if flags.startTime != 0 && flags.endTime != 0 && flags.endTime < flags.startTime {
		log.Fatal("end time before start time")
	}

	if flags.worker < 0 {
		log.Fatal("worker is invalid")
	}
	if flags.nodeTotal <= 0 {
		log.Fatal("node-total is invalid")
	}
	for idx := range flags.nodeIndex {
		if idx < 0 || idx >= flags.nodeTotal {
			log.Fatal("node-index is invalid")
		}
	}
	if len(flags.nodeIndex) == 0 {
		for idx := 0; idx < flags.nodeTotal; idx++ {
			flags.nodeIndex[idx] = struct{}{}
		}
	}
	if flags.hashKey != "idx" && flags.hashKey != "exi" {
		log.Fatal("hash-key is invalid")
	}
}

func runE(flags *flagpole) (err error) {
	exportServer, err := server.NewServer(flags.sourceDir, !flags.skipTsi)
	if err != nil {
		return
	}
	defer exportServer.Close()
	exp, err := newExporter(exportServer, flags.database, flags.retentionPolicy, flags.shardDuration, flags.startTime, flags.endTime)
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
	for idx := range flags.nodeIndex {
		importServer, err := server.NewServer(fmt.Sprintf("%s-%d", flags.targetDir, idx), !flags.skipTsi)
		if err != nil {
			return err
		}
		svrs[idx] = importServer
		imp, err := newImporter(importServer, flags.database, flags.retentionPolicy, flags.shardDuration, flags.duration, !flags.skipTsi)
		if err != nil {
			return err
		}
		imps[idx] = imp
	}

	transfer(flags, exp, imps)
	return
}

func transfer(flags *flagpole, exp *exporter, imps map[int]*importer) {
	log.SetFlags(log.LstdFlags)
	log.Printf("transfer node total: %d, node index: %s, hash key: %s", flags.nodeTotal, flags.nodeIndex, flags.hashKey)
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
	for idx := range flags.nodeIndex {
		prChans[idx] = make(chan *nio.PipeReader, 4)
	}

	go func() {
		defer func() {
			for _, prChan := range prChans {
				close(prChan)
			}
		}()
		exp.WriteTo(prChans, flags.nodeTotal, flags.hashKey, flags.worker)
	}()

	wg := &sync.WaitGroup{}
	for idx := range imps {
		wg.Add(1)
		idx := idx
		go func() {
			defer wg.Done()
			transferNode(imps[idx], prChans[idx], idx)
		}()
	}
	wg.Wait()
	log.Print("transfer done")
}

func transferNode(imp *importer, prChan chan *nio.PipeReader, idx int) {
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
