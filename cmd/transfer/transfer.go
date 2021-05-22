package transfer

import (
	"log"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

type flags struct {
	mode            string
	sourceDir       []string
	targetDir       string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
}

func NewCommand() *cobra.Command {
	var startTime, endTime string
	flags := &flags{}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "transfer",
		Short:         "Transfer influxdb persist data on disk from one or more to targets",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			processFlags(flags, startTime, endTime)
			return runE(flags)
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVarP(&flags.mode, "mode", "m", "hash", "transfer mode like hash, prefix, glob")
	cmd.Flags().StringSliceVarP(&flags.sourceDir, "source-dir", "s", []string{}, "one or more source dirs to transfer delimited by comma (required)")
	cmd.Flags().StringVarP(&flags.targetDir, "target-dir", "t", "", "target dir to transfer (required)")
	cmd.Flags().StringVarP(&flags.database, "database", "d", "", "database name to transfer (optional)")
	cmd.Flags().StringVarP(&flags.retentionPolicy, "retention-policy", "r", "", "retention policy to transfer (optional, requires -database)")
	cmd.Flags().StringVarP(&startTime, "start-time", "S", "", "start time to transfer (RFC3339 format, optional)")
	cmd.Flags().StringVarP(&endTime, "end-time", "E", "", "end time to transfer (RFC3339 format, optional)")
	cmd.MarkFlagRequired("source-dir")
	cmd.MarkFlagRequired("target-dir")
	return cmd
}

func processFlags(flags *flags, startTime, endTime string) {
	if startTime != "" {
		s, err := time.Parse(time.RFC3339, startTime)
		if err != nil {
			log.Fatal("invalid start-time")
		}
		flags.startTime = s.UnixNano()
	} else {
		flags.startTime = math.MinInt64
	}
	if endTime != "" {
		e, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			log.Fatal("invalid end-time")
		}
		flags.endTime = e.UnixNano()
	} else {
		flags.endTime = math.MaxInt64
	}
	if flags.startTime != 0 && flags.endTime != 0 && flags.endTime < flags.startTime {
		log.Fatal("end-time before start-time")
	}
	if flags.retentionPolicy != "" && flags.database == "" {
		log.Fatal("must specify a database")
	}
	for _, sourceDir := range flags.sourceDir {
		if _, err := os.Stat(filepath.Join(sourceDir, "data")); os.IsNotExist(err) {
			log.Fatalf("source-dir not exist: %s", sourceDir)
		}
	}
	if _, err := os.Stat(filepath.Join(flags.targetDir, "meta", "meta.db")); err == nil {
		log.Fatalf("target-dir already exist: %s", flags.targetDir)
	}
}

func runE(flags *flags) (err error) {
	return nil
}
