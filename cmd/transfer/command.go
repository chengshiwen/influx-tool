package transfer

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

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
	sleepInterval   int
}

func NewCommand() *cobra.Command {
	var start, end string
	flags := &flagpole{nodeIndex: make(intSet)}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "transfer",
		Short:         "transfer influxdb persist data on disk from one to another",
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
	cmd.Flags().IntVarP(&flags.sleepInterval, "sleep-interval", "p", 0, "sleep interval seconds per shard transfer, require worker > 0 (default: 0)")
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
	if flags.sleepInterval < 0 {
		log.Fatal("sleep-interval is invalid")
	}
}

func runE(flags *flagpole) (err error) {
	log.Printf("node-total: %d", flags.nodeTotal)
	log.Printf("node-index: %s", flags.nodeIndex)
	return nil
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
