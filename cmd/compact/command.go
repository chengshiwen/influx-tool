package compact

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/chengshiwen/influx-tool/internal/errlist"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type command struct {
	cobraCmd *cobra.Command
	path     string
	force    bool
	worker   int
}

func NewCommand() *cobra.Command {
	cmd := &command{}
	cmd.cobraCmd = &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "compact",
		Short:         "Compact the all shards fully",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.runE()
		},
	}
	flags := cmd.cobraCmd.Flags()
	flags.SortFlags = false
	flags.StringVarP(&cmd.path, "path", "p", "", "path of shard to be compacted like /path/to/influxdb/data/db/rp (required)")
	flags.BoolVarP(&cmd.force, "force", "f", false, "force compaction without prompting (default: false)")
	flags.IntVarP(&cmd.worker, "worker", "w", 0, "number of concurrent workers to compact (default: 0, unlimited)")
	cmd.cobraCmd.MarkFlagRequired("path")
	return cmd.cobraCmd
}

func (cmd *command) validate() error {
	if cmd.worker < 0 {
		return errors.New("worker is invalid")
	}
	return nil
}

func (cmd *command) runE() error {
	if err := cmd.validate(); err != nil {
		return err
	}
	files, err := os.ReadDir(cmd.path)
	if err != nil {
		return err
	}
	reg := regexp.MustCompile(`\d+`)
	paths := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() || !reg.MatchString(file.Name()) {
			return errors.New("shard-path is invalid, it should be like /path/to/influxdb/data/db/rp")
		}
		paths = append(paths, filepath.Join(cmd.path, file.Name()))
	}

	log.SetFlags(0)
	log.Printf("opening shard at path %q", cmd.path)

	if !cmd.force {
		fmt.Print("proceed? [N] ")
		scan := bufio.NewScanner(os.Stdin)
		scan.Scan()
		if scan.Err() != nil {
			return fmt.Errorf("error reading stdin: %v", scan.Err())
		}

		if strings.ToLower(scan.Text()) != "y" {
			return nil
		}
	}

	log.Print("compacting shard")

	limit := make(chan struct{}, cmd.worker)
	wg := &sync.WaitGroup{}
	for _, path := range paths {
		wg.Add(1)
		path := path
		go func() {
			if cmd.worker > 0 {
				limit <- struct{}{}
			}
			defer func() {
				wg.Done()
				if cmd.worker > 0 {
					<-limit
				}
			}()

			sc, err := newShardCompactor(path)
			if err != nil {
				log.Printf("newShardCompactor %s error: %v", path, err)
				return
			}
			err = sc.CompactShard()
			if err != nil {
				log.Printf("compaction %s failed: %v", path, err)
				return
			}
			newTSM := make([]string, len(sc.newTSM))
			for i := range sc.newTSM {
				newTSM[i] = filepath.Base(sc.newTSM[i])
			}
			log.Printf("compaction %s succeeded with new tsm files: %s", path, strings.Join(newTSM, " "))
		}()
	}
	wg.Wait()
	log.Print("compaction shard done")
	return nil
}

type shardCompactor struct {
	path      string
	tsm       []string
	tombstone []string
	readers   []*tsm1.TSMReader
	files     map[string]*tsm1.TSMReader
	newTSM    []string
}

func newShardCompactor(path string) (sc *shardCompactor, err error) {
	sc = &shardCompactor{
		path:  path,
		files: make(map[string]*tsm1.TSMReader),
	}

	sc.tsm, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		return nil, fmt.Errorf("newFileStore: error reading tsm files at path %q: %v", path, err)
	}
	if len(sc.tsm) == 0 {
		return nil, fmt.Errorf("newFileStore: no tsm files at path %q", path)
	}
	sort.Strings(sc.tsm)

	sc.tombstone, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", tsm1.TombstoneFileExtension)))
	if err != nil {
		return nil, fmt.Errorf("error reading tombstone files: %v", err)
	}

	if err := sc.openFiles(); err != nil {
		return nil, err
	}

	return sc, nil
}

func (sc *shardCompactor) openFiles() error {
	sc.readers = make([]*tsm1.TSMReader, 0, len(sc.tsm))

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		r   *tsm1.TSMReader
		err error
	}

	lim := limiter.NewFixed(runtime.GOMAXPROCS(0))

	badTSM := make([]bool, len(sc.tsm))
	readerC := make(chan *res)
	for i, fn := range sc.tsm {
		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("newFileStore: failed to open file %q: %v", fn, err)
		}

		go func(idx int, file *os.File) {
			// Ensure a limited number of TSM files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of TSM files which can cause extremely long load times.
			lim.Take()
			defer lim.Release()

			df, err := tsm1.NewTSMReader(file)

			// If we are unable to read a TSM file then log the error, remove
			// the file, and continue loading the shard without it.
			if err != nil {
				if e := os.Remove(file.Name()); e != nil {
					log.Printf("cannot remove corrupt tsm file: %s, error: %v", file.Name(), e)
					readerC <- &res{r: df, err: fmt.Errorf("cannot remove corrupt file %s: %v", file.Name(), e)}
					return
				}
				badTSM[idx] = true
			}

			readerC <- &res{r: df}
		}(i, file)
	}

	for range sc.tsm {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.r == nil {
			continue
		}
		sc.readers = append(sc.readers, res.r)
		sc.files[res.r.Path()] = res.r
	}
	close(readerC)
	sort.Sort(tsmReaders(sc.readers))

	goodTSM := make([]string, 0)
	for idx, bad := range badTSM {
		if !bad {
			goodTSM = append(goodTSM, sc.tsm[idx])
		}
	}
	if len(goodTSM) == 0 {
		return fmt.Errorf("newFileStore: no good tsm files at path %q", filepath.Dir(sc.tsm[0]))
	}
	sc.tsm = goodTSM

	return nil
}

func (sc *shardCompactor) CompactShard() (err error) {
	c := tsm1.NewCompactor()
	c.Dir = sc.path
	c.Size = tsm1.DefaultSegmentSize
	c.FileStore = sc
	c.Open()

	tsmFiles, err := c.CompactFull(sc.tsm)
	if err == nil {
		sc.newTSM, err = sc.replace(tsmFiles)
	}
	return err
}

// replace replaces the existing shard files with temporary tsmFiles
func (sc *shardCompactor) replace(tsmFiles []string) ([]string, error) {
	// rename .tsm.tmp → .tsm
	var newNames []string
	for _, file := range tsmFiles {
		var newName = file[:len(file)-4] // remove extension
		if err := os.Rename(file, newName); err != nil {
			return nil, err
		}
		newNames = append(newNames, newName)
	}

	var errs errlist.ErrorList

	// close all readers
	for _, r := range sc.readers {
		r.Close()
	}

	sc.readers = nil
	sc.files = nil

	// remove existing .tsm and .tombstone
	for _, file := range sc.tsm {
		errs.Add(os.Remove(file))
	}

	for _, file := range sc.tombstone {
		errs.Add(os.Remove(file))
	}

	return newNames, errs.Err()
}

func (sc *shardCompactor) NextGeneration() int {
	panic("not implemented")
}

func (sc *shardCompactor) TSMReader(path string) *tsm1.TSMReader {
	r := sc.files[path]
	if r != nil {
		r.Ref()
	}
	return r
}

type tsmReaders []*tsm1.TSMReader

func (a tsmReaders) Len() int           { return len(a) }
func (a tsmReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a tsmReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
