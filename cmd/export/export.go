package export

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"
	"github.com/spf13/cobra"
)

type flagpole struct {
	dataDir         string
	walDir          string
	out             string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	compress        bool
	lponly          bool
}

var (
	manifest = make(map[string]struct{})
	tsmFiles = make(map[string][]string)
	walFiles = make(map[string][]string)
)

const stdoutMark = "-"

func NewCommand() *cobra.Command {
	var start, end string
	flags := &flagpole{}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "export",
		Short:         "Export TSM files into InfluxDB line protocol format",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			processFlags(flags, start, end)
			return runE(flags)
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVarP(&flags.dataDir, "datadir", "D", "", "data storage path (required)")
	cmd.Flags().StringVarP(&flags.walDir, "waldir", "W", "", "wal storage path (required)")
	cmd.Flags().StringVarP(&flags.out, "out", "o", "./export", "'-' for standard out or the destination file to export to")
	cmd.Flags().StringVarP(&flags.database, "database", "d", "", "database to export")
	cmd.Flags().StringVarP(&flags.retentionPolicy, "retention-policy", "r", "", "retention policy to export (require -database)")
	cmd.Flags().StringVarP(&start, "start", "S", "", "start time to export (RFC3339 format, optional)")
	cmd.Flags().StringVarP(&end, "end", "E", "", "end time to export (RFC3339 format, optional)")
	cmd.Flags().BoolVarP(&flags.lponly, "lponly", "l", false, "only export line protocol (default: false)")
	cmd.Flags().BoolVarP(&flags.compress, "compress", "c", false, "compress the output (default: false)")
	cmd.MarkFlagRequired("datadir")
	cmd.MarkFlagRequired("waldir")
	return cmd
}

func (flags *flagpole) usingStdOut() bool {
	return flags.out == stdoutMark
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
	if flags.database == "_internal" {
		log.Fatal("database cannot be _internal")
	}
	if flags.retentionPolicy != "" && flags.database == "" {
		log.Fatal("must specify a database")
	}
}

func runE(flags *flagpole) error {
	if err := walkTSMFiles(flags); err != nil {
		return err
	}
	if err := walkWALFiles(flags); err != nil {
		return err
	}

	return write(flags)
}

func walkTSMFiles(flags *flagpole) error {
	return filepath.Walk(flags.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(flags.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] != "_internal" && (dirs[0] == flags.database || flags.database == "") {
			if dirs[1] == flags.retentionPolicy || flags.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				manifest[key] = struct{}{}
				tsmFiles[key] = append(tsmFiles[key], path)
			}
		}
		return nil
	})
}

func walkWALFiles(flags *flagpole) error {
	return filepath.Walk(flags.walDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a wal file
		fileName := filepath.Base(path)
		if filepath.Ext(path) != "."+tsm1.WALFileExtension || !strings.HasPrefix(fileName, tsm1.WALFilePrefix) {
			return nil
		}

		relPath, err := filepath.Rel(flags.walDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] != "_internal" && (dirs[0] == flags.database || flags.database == "") {
			if dirs[1] == flags.retentionPolicy || flags.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				manifest[key] = struct{}{}
				walFiles[key] = append(walFiles[key], path)
			}
		}
		return nil
	})
}

func writeDDL(flags *flagpole, mw io.Writer, w io.Writer) error {
	// Write out all the DDL
	fmt.Fprintln(mw, "# DDL")
	for key := range manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		db, rp := influxql.QuoteIdent(keys[0]), influxql.QuoteIdent(keys[1])
		fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME %s\n", db, rp)
	}

	return nil
}

func writeDML(flags *flagpole, mw io.Writer, w io.Writer) error {
	fmt.Fprintln(mw, "# DML")
	var msgOut io.Writer
	if flags.usingStdOut() {
		msgOut = os.Stderr
	} else {
		msgOut = os.Stdout
	}
	for key := range manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		fmt.Fprintf(mw, "# CONTEXT-DATABASE:%s\n", keys[0])
		fmt.Fprintf(mw, "# CONTEXT-RETENTION-POLICY:%s\n", keys[1])
		if files, ok := tsmFiles[key]; ok {
			fmt.Fprintf(msgOut, "writing out tsm file data for %s...", key)
			if err := writeTsmFiles(flags, mw, w, files); err != nil {
				return err
			}
			fmt.Fprintln(msgOut, "complete.")
		}
		if _, ok := walFiles[key]; ok {
			fmt.Fprintf(msgOut, "writing out wal file data for %s...", key)
			if err := writeWALFiles(flags, mw, w, walFiles[key], key); err != nil {
				return err
			}
			fmt.Fprintln(msgOut, "complete.")
		}
	}

	return nil
}

// writeFull writes the full DML and DDL to the supplied io.Writers.  mw is the
// "meta" writer where comments and other informational writes go and w is for
// the actual payload of the writes -- DML and DDL.
//
// Typically mw and w are the same but if we'd like to, for example, filter out
// comments and other meta data, we can pass ioutil.Discard to mw to only
// include the raw data that writeFull() generates.
func writeFull(flags *flagpole, mw io.Writer, w io.Writer) error {
	s, e := time.Unix(0, flags.startTime).Format(time.RFC3339), time.Unix(0, flags.endTime).Format(time.RFC3339)

	fmt.Fprintf(mw, "# INFLUXDB EXPORT: %s - %s\n", s, e)

	if shouldWriteDDL := !flags.lponly; shouldWriteDDL {
		if err := writeDDL(flags, mw, w); err != nil {
			return err
		}
	}

	if err := writeDML(flags, mw, w); err != nil {
		return err
	}

	return nil
}

func write(flags *flagpole) error {
	var w io.Writer
	if flags.usingStdOut() {
		w = os.Stdout
	} else {
		// open our output file and create an output buffer
		f, err := os.Create(flags.out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	// Because calling (*os.File).Write is relatively expensive,
	// and we don't *need* to sync to disk on every written line of export,
	// use a sized buffered writer so that we only sync the file every megabyte.
	bw := bufio.NewWriterSize(w, 1024*1024)
	defer bw.Flush()
	w = bw

	if flags.compress {
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		w = gzw
	}

	// mw is our "meta writer" -- the io.Writer to which meta/out-of-band data
	// like comments will be sent.  If the lponly flag is set, mw will be
	// ioutil.Discard which effectively filters out comments and any other
	// non-line protocol data.
	//
	// Otherwise, mw is set to the same writer as the actual DDL and line
	// protocol DML which will cause the comments to be intermixed with the
	// data..
	//
	mw := w
	if flags.lponly {
		mw = ioutil.Discard
	}

	return writeFull(flags, mw, w)
}

func writeTsmFiles(flags *flagpole, mw io.Writer, w io.Writer, files []string) error {
	fmt.Fprintln(mw, "# writing tsm data")

	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		if err := exportTSMFile(flags, f, w); err != nil {
			return err
		}
	}

	return nil
}

func exportTSMFile(flags *flagpole, tsmFilePath string, w io.Writer) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", tsmFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to read %s, skipping: %s\n", tsmFilePath, err.Error())
		return nil
	}
	defer r.Close()

	if sgStart, sgEnd := r.TimeRange(); sgStart > flags.endTime || sgEnd < flags.startTime {
		return nil
	}

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
			continue
		}
		measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		field = escape.Bytes(field)

		if err := writeValues(flags, w, measurement, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}
	return nil
}

func writeWALFiles(flags *flagpole, mw io.Writer, w io.Writer, files []string, key string) error {
	fmt.Fprintln(mw, "# writing wal data")

	// we need to make sure we write the same order that the wal received the data
	sort.Strings(files)

	var once sync.Once
	warnDelete := func() {
		once.Do(func() {
			msg := fmt.Sprintf(`WARNING: detected deletes in wal file.
Some series for %q may be brought back by replaying this data.
To resolve, you can either let the shard snapshot prior to exporting the data
or manually editing the exported file.
			`, key)
			fmt.Fprintln(os.Stderr, msg)
		})
	}

	for _, f := range files {
		if err := exportWALFile(flags, f, w, warnDelete); err != nil {
			return err
		}
	}

	return nil
}

// exportWAL reads every WAL entry from r and exports it to w.
func exportWALFile(flags *flagpole, walFilePath string, w io.Writer, warnDelete func()) error {
	f, err := os.Open(walFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", walFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			fmt.Fprintf(os.Stderr, "file %s corrupt at position %d: %v", walFilePath, n, err)
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			warnDelete()
			continue
		case *tsm1.WriteWALEntry:
			for key, values := range t.Values {
				measurement, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				// measurements are stored escaped, field names are not
				field = escape.Bytes(field)

				if err := writeValues(flags, w, measurement, string(field), values); err != nil {
					// An error from writeValues indicates an IO error, which should be returned.
					return err
				}
			}
		}
	}
	return nil
}

// writeValues writes every value in values to w, using the given series key and field name.
// If any call to w.Write fails, that error is returned.
func writeValues(flags *flagpole, w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if (ts < flags.startTime) || (ts > flags.endTime) {
			continue
		}

		// Re-slice buf to be "<series_key> <field>=".
		buf = buf[:prefixLen]

		// Append the correct representation of the value.
		switch v := value.Value().(type) {
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, 'i')
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
			buf = append(buf, 'u')
		case bool:
			buf = strconv.AppendBool(buf, v)
		case string:
			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(v)...)
			buf = append(buf, '"')
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}
	}

	return nil
}
