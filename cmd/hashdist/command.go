package hashdist

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/chengshiwen/influx-tool/internal/hash"
	"github.com/spf13/cobra"
)

type command struct {
	cobraCmd    *cobra.Command
	version     string
	nodeTotal   int
	hashKey     string
	shardKey    string
	org         string
	bucket      string
	database    string
	measurement string
	separator   string
	file        string
	dist        string
}

const stdoutMark = "-"

var (
	version1 = "v1"
	version2 = "v2"
)

func NewCommand() *cobra.Command {
	cmd := &command{}
	cmd.cobraCmd = &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "hashdist",
		Short:         "Hash distribution calculation",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.runE()
		},
	}
	flags := cmd.cobraCmd.Flags()
	flags.SortFlags = false
	flags.StringVarP(&cmd.version, "version", "v", "v1", "influxdb version: v1, v2")
	flags.IntVarP(&cmd.nodeTotal, "node-total", "n", 1, "total number of node in a circle")
	flags.StringVarP(&cmd.hashKey, "hash-key", "k", "", "hash key for influx proxy: idx, exi or template containing %idx (v1 default \"idx\", v2 default \"%idx\")")
	flags.StringVarP(&cmd.shardKey, "shard-key", "K", "", "shard key for influx proxy, which containing %org, %bk, %db or %mm (v1 default \"%db,%mm\", v2 default \"%org,%bk,%mm\")")
	flags.StringVarP(&cmd.org, "org", "o", "", "org name under influxdb v2, note that --file cannot be specified when --org specified")
	flags.StringVarP(&cmd.bucket, "bucket", "b", "", "bucket name under influxdb v2, note that --file cannot be specified when --bucket specified")
	flags.StringVarP(&cmd.database, "database", "d", "", "database name under influxdb v1, note that --file cannot be specified when --database specified")
	flags.StringVarP(&cmd.measurement, "measurement", "m", "", "measurement name, note that --file cannot be specified when --measurement specified")
	flags.StringVarP(&cmd.separator, "separator", "s", ",", "separator character to separate each line in the file")
	flags.StringVarP(&cmd.file, "file", "f", "", "path to the file to read, format of each line is like 'db,mm' separated by a separator")
	flags.StringVarP(&cmd.dist, "dist", "D", "./dist", "'-' for standard out or the distribution file to write to when --file specified")
	return cmd.cobraCmd
}

func (cmd *command) validate() error {
	if cmd.version != version1 && cmd.version != version2 {
		return errors.New("version is invalid, require either v1 or v2")
	}
	if cmd.nodeTotal <= 0 {
		return errors.New("node-total is invalid")
	}
	if cmd.version == version1 {
		if !cmd.cobraCmd.Flags().Changed("hash-key") {
			cmd.hashKey = hash.HashKeyIdx
		}
		if !cmd.cobraCmd.Flags().Changed("shard-key") {
			cmd.shardKey = hash.ShardKeyDbMm
		}
		if cmd.hashKey != hash.HashKeyIdx && cmd.hashKey != hash.HashKeyExi && !strings.Contains(cmd.hashKey, hash.HashKeyVarIdx) {
			return errors.New("hash-key is invalid, require idx, exi or template containing %idx")
		}
		if !strings.Contains(cmd.shardKey, hash.ShardKeyVarDb) && !strings.Contains(cmd.shardKey, hash.ShardKeyVarMm) {
			return errors.New("shard-key is invalid, require template containing %db or %mm")
		}
		if (cmd.database != "" || cmd.measurement != "") && cmd.file != "" {
			return errors.New("--file cannot be specified when --database or --measurement specified")
		}
		if cmd.database == "" && cmd.measurement == "" && cmd.file == "" {
			return errors.New("--database, --measurement or --file flag required")
		}
	} else {
		if !cmd.cobraCmd.Flags().Changed("hash-key") {
			cmd.hashKey = hash.HashKeyVarIdx
		}
		if !cmd.cobraCmd.Flags().Changed("shard-key") {
			cmd.shardKey = hash.ShardKeyOrgBkMm
		}
		if !strings.Contains(cmd.hashKey, hash.HashKeyVarIdx) {
			return errors.New("hash-key is invalid, require template containing %idx")
		}
		if !strings.Contains(cmd.shardKey, hash.ShardKeyVarOrg) && !strings.Contains(cmd.shardKey, hash.ShardKeyVarBk) && !strings.Contains(cmd.shardKey, hash.ShardKeyVarMm) {
			return errors.New("shard-key is invalid, require template containing %org, %bk or %mm")
		}
		if (cmd.org != "" || cmd.bucket != "" || cmd.measurement != "") && cmd.file != "" {
			return errors.New("--file cannot be specified when --org, --bucket or --measurement specified")
		}
		if cmd.org == "" && cmd.bucket == "" && cmd.measurement == "" && cmd.file == "" {
			return errors.New("--org, --bucket, --measurement or --file flag required")
		}
	}
	if cmd.file != "" {
		info, err := os.Stat(cmd.file)
		if os.IsNotExist(err) {
			return fmt.Errorf("file '%s' does not exist", cmd.file)
		}
		if info.IsDir() {
			return fmt.Errorf("file '%s' is a directory", cmd.file)
		}
		if cmd.separator == "" {
			return errors.New("--separator flag required")
		}
		if cmd.dist == "" {
			return errors.New("--dist flag required")
		}
	}
	return nil
}

func (cmd *command) runE() error {
	if err := cmd.validate(); err != nil {
		return err
	}
	return cmd.hashdist()
}

func (cmd *command) hashdist() error {
	ch := hash.NewConsistentHash(cmd.nodeTotal, cmd.hashKey)
	st := hash.NewShardTpl(cmd.shardKey)
	if cmd.version == version1 {
		if cmd.database != "" || cmd.measurement != "" {
			log.Printf("node total: %d, hash key: %s, shard key: %s, database: %s, measurement: %s", cmd.nodeTotal, cmd.hashKey, cmd.shardKey, cmd.database, cmd.measurement)
			log.Printf("node index: %d", ch.Get(st.GetKey(cmd.database, []byte(cmd.measurement))))
			return nil
		}
	} else {
		if cmd.org != "" || cmd.bucket != "" || cmd.measurement != "" {
			log.Printf("node total: %d, hash key: %s, shard key: %s, org: %s, bucket: %s, measurement: %s", cmd.nodeTotal, cmd.hashKey, cmd.shardKey, cmd.org, cmd.bucket, cmd.measurement)
			log.Printf("node index: %d", ch.Get(st.GetKeyV2(cmd.org, cmd.bucket, cmd.measurement)))
			return nil
		}
	}

	var w io.Writer
	if cmd.dist == stdoutMark {
		w = os.Stdout
	} else {
		f, err := os.Create(cmd.dist)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	bw := bufio.NewWriterSize(w, 1024*1024)
	defer bw.Flush()
	w = bw

	f, err := os.Open(cmd.file)
	if err != nil {
		return err
	}
	defer f.Close()

	dist := make(map[int]int)
	warn := 0
	tHits := 0

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if cmd.version == version1 {
			db, mm, ok := strings.Cut(line, cmd.separator)
			if !ok {
				warn += 1
				if _, err := w.Write([]byte(fmt.Sprintf("warning: '%s' ignored since separator '%s' not found\n", line, cmd.separator))); err != nil {
					return err
				}
				continue
			}
			dist[ch.Get(st.GetKey(db, []byte(mm)))] += 1
		} else {
			items := strings.Split(line, cmd.separator)
			if len(items) == 0 || len(items) != 3 {
				warn += 1
				if _, err := w.Write([]byte(fmt.Sprintf("warning: '%s' ignored since separator '%s' not found or inaccurate\n", line, cmd.separator))); err != nil {
					return err
				}
				continue
			}
			dist[ch.Get(st.GetKeyV2(items[0], items[1], items[2]))] += 1
		}
		tHits += 1
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if warn > 0 {
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte(fmt.Sprintf("node total: %d, hash key: %s, shard key: %s, total hits: %d\n", cmd.nodeTotal, cmd.hashKey, cmd.shardKey, tHits))); err != nil {
		return err
	}
	for i := 0; i < cmd.nodeTotal; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("node index: %d, hits: %d, percent: %4.1f%%, expect: %4.1f%%\n", i, dist[i], float64(dist[i])*100/float64(tHits), 100/float64(cmd.nodeTotal)))); err != nil {
			return err
		}
	}
	return nil
}
