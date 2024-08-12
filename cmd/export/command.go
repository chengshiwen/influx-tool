package exporter

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
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

type command struct {
	cobraCmd          *cobra.Command
	dataDir           string
	walDir            string
	out               string
	database          string
	retentionPolicy   string
	measurement       map[string]struct{}
	regexpMeasurement []*regexp.Regexp
	startTime         int64
	endTime           int64
	compress          bool
	lponly            bool

	manifest map[string]struct{}
	tsmFiles map[string][]string
	walFiles map[string][]string
}

type tempflag struct {
	start             string
	end               string
	measurement       []string
	regexpMeasurement []string
}

const stdoutMark = "-"

func NewCommand() *cobra.Command {
	tf := &tempflag{}
	cmd := &command{
		measurement:       make(map[string]struct{}),
		regexpMeasurement: make([]*regexp.Regexp, 0),
		manifest:          make(map[string]struct{}),
		tsmFiles:          make(map[string][]string),
		walFiles:          make(map[string][]string),
	}
	cmd.cobraCmd = &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "export",
		Short:         "Export tsm files into InfluxDB line protocol format",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.runE(tf)
		},
	}
	flags := cmd.cobraCmd.Flags()
	flags.SortFlags = false
	flags.StringVarP(&cmd.dataDir, "datadir", "D", "", "data storage path (required)")
	flags.StringVarP(&cmd.walDir, "waldir", "W", "", "wal storage path (required)")
	flags.StringVarP(&cmd.out, "out", "o", "./export", "'-' for standard out or the destination file to export to")
	flags.StringVarP(&cmd.database, "database", "d", "", "database to export without _internal (default: all)")
	flags.StringVarP(&cmd.retentionPolicy, "retention-policy", "r", "", "retention policy to export (require database)")
	flags.StringArrayVarP(&tf.measurement, "measurement", "m", []string{}, "measurement to export, can be set multiple times (require database, default: all)")
	flags.StringArrayVarP(&tf.regexpMeasurement, "regexp-measurement", "M", []string{}, "regexp measurement to export, can be set multiple times (require database, default: all)")
	flags.StringVarP(&tf.start, "start", "S", "", "start time to export (RFC3339 format, optional)")
	flags.StringVarP(&tf.end, "end", "E", "", "end time to export (RFC3339 format, optional)")
	flags.BoolVarP(&cmd.lponly, "lponly", "l", false, "only export line protocol (default: false)")
	flags.BoolVarP(&cmd.compress, "compress", "c", false, "compress the output (default: false)")
	cmd.cobraCmd.MarkFlagRequired("datadir")
	cmd.cobraCmd.MarkFlagRequired("waldir")
	return cmd.cobraCmd
}

func (cmd *command) validate(tf *tempflag) error {
	if tf.start != "" {
		s, err := time.Parse(time.RFC3339, tf.start)
		if err != nil {
			return errors.New("start time is invalid")
		}
		cmd.startTime = s.UnixNano()
	} else {
		cmd.startTime = math.MinInt64
	}
	if tf.end != "" {
		e, err := time.Parse(time.RFC3339, tf.end)
		if err != nil {
			return errors.New("end time is invalid")
		}
		cmd.endTime = e.UnixNano()
	} else {
		cmd.endTime = math.MaxInt64
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return errors.New("end time before start time")
	}
	if cmd.database == "_internal" {
		return errors.New("database cannot be _internal")
	}
	if cmd.retentionPolicy != "" && cmd.database == "" {
		return errors.New("must specify a database when retention policy given")
	}
	if len(cmd.measurement) > 0 && cmd.database == "" {
		return errors.New("must specify a database when measurement given")
	}
	for _, str := range tf.measurement {
		cmd.measurement[str] = struct{}{}
	}
	if len(cmd.regexpMeasurement) > 0 && cmd.database == "" {
		return errors.New("must specify a database when regexp measurement given")
	}
	for _, str := range tf.regexpMeasurement {
		if rem, err := regexp.Compile(str); err == nil {
			cmd.regexpMeasurement = append(cmd.regexpMeasurement, rem)
		} else {
			return fmt.Errorf("regexp measurement: %s, compile error: %v", str, err)
		}
	}
	return nil
}

func (cmd *command) runE(tf *tempflag) error {
	if err := cmd.validate(tf); err != nil {
		return err
	}
	if err := cmd.walkTSMFiles(); err != nil {
		return err
	}
	if err := cmd.walkWALFiles(); err != nil {
		return err
	}

	return cmd.write()
}

func (cmd *command) walkTSMFiles() error {
	return filepath.Walk(cmd.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(cmd.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] != "_internal" && (dirs[0] == cmd.database || cmd.database == "") {
			if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.manifest[key] = struct{}{}
				cmd.tsmFiles[key] = append(cmd.tsmFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd *command) walkWALFiles() error {
	return filepath.Walk(cmd.walDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a wal file
		fileName := filepath.Base(path)
		if filepath.Ext(path) != "."+tsm1.WALFileExtension || !strings.HasPrefix(fileName, tsm1.WALFilePrefix) {
			return nil
		}

		relPath, err := filepath.Rel(cmd.walDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] != "_internal" && (dirs[0] == cmd.database || cmd.database == "") {
			if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.manifest[key] = struct{}{}
				cmd.walFiles[key] = append(cmd.walFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd *command) writeDDL(mw io.Writer, w io.Writer) error {
	// Write out all the DDL
	fmt.Fprintln(mw, "# DDL")
	manifest := make(map[string][]string)
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		db, rp := influxql.QuoteIdent(keys[0]), influxql.QuoteIdent(keys[1])
		manifest[db] = append(manifest[db], rp)
	}
	for db, rps := range manifest {
		if len(rps) > 1 {
			fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME autogen\n", db)
			for _, rp := range rps {
				if rp != "autogen" {
					fmt.Fprintf(w, "CREATE RETENTION POLICY %s ON %s DURATION 0s REPLICATION 1\n", rp, db)
				}
			}
		} else {
			fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME %s\n", db, rps[0])
		}
	}

	return nil
}

func (cmd *command) writeDML(mw io.Writer, w io.Writer) error {
	fmt.Fprintln(mw, "# DML")
	var msgOut io.Writer
	if cmd.usingStdOut() {
		msgOut = os.Stderr
	} else {
		msgOut = os.Stdout
	}
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		fmt.Fprintf(mw, "# CONTEXT-DATABASE:%s\n", keys[0])
		fmt.Fprintf(mw, "# CONTEXT-RETENTION-POLICY:%s\n", keys[1])
		if files, ok := cmd.tsmFiles[key]; ok {
			fmt.Fprintf(msgOut, "writing out tsm file data for %s%s...", key, cmd.withMeasurement())
			if err := cmd.writeTsmFiles(mw, w, files); err != nil {
				return err
			}
			fmt.Fprintln(msgOut, "complete.")
		}
		if _, ok := cmd.walFiles[key]; ok {
			fmt.Fprintf(msgOut, "writing out wal file data for %s%s...", key, cmd.withMeasurement())
			if err := cmd.writeWALFiles(mw, w, cmd.walFiles[key], key); err != nil {
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
// comments and other meta data, we can pass io.Discard to mw to only
// include the raw data that writeFull() generates.
func (cmd *command) writeFull(mw io.Writer, w io.Writer) error {
	s, e := time.Unix(0, cmd.startTime).Format(time.RFC3339), time.Unix(0, cmd.endTime).Format(time.RFC3339)

	fmt.Fprintf(mw, "# INFLUXDB EXPORT: %s - %s\n", s, e)

	if shouldWriteDDL := !cmd.lponly; shouldWriteDDL {
		if err := cmd.writeDDL(mw, w); err != nil {
			return err
		}
	}

	if err := cmd.writeDML(mw, w); err != nil {
		return err
	}

	return nil
}

func (cmd *command) write() error {
	var w io.Writer
	if cmd.usingStdOut() {
		w = os.Stdout
	} else {
		// open our output file and create an output buffer
		f, err := os.Create(cmd.out)
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

	if cmd.compress {
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		w = gzw
	}

	// mw is our "meta writer" -- the io.Writer to which meta/out-of-band data
	// like comments will be sent.  If the lponly flag is set, mw will be
	// io.Discard which effectively filters out comments and any other
	// non-line protocol data.
	//
	// Otherwise, mw is set to the same writer as the actual DDL and line
	// protocol DML which will cause the comments to be intermixed with the
	// data..
	//
	mw := w
	if cmd.lponly {
		mw = io.Discard
	}

	return cmd.writeFull(mw, w)
}

func (cmd *command) writeTsmFiles(mw io.Writer, w io.Writer, files []string) error {
	fmt.Fprintln(mw, "# writing tsm data")

	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		if err := cmd.exportTSMFile(f, w); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *command) exportTSMFile(tsmFilePath string, w io.Writer) error {
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

	if sgStart, sgEnd := r.TimeRange(); sgStart > cmd.endTime || sgEnd < cmd.startTime {
		return nil
	}

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
			continue
		}
		seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		name := models.ParseName(seriesKey)
		if !cmd.matchMeasurement(string(name)) {
			continue
		}
		// seriesKey are stored escaped, field names are not
		field = escape.Bytes(field)
		if err := cmd.writeValues(w, seriesKey, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}
	return nil
}

func (cmd *command) writeWALFiles(mw io.Writer, w io.Writer, files []string, key string) error {
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
		if err := cmd.exportWALFile(f, w, warnDelete); err != nil {
			return err
		}
	}

	return nil
}

// exportWAL reads every WAL entry from r and exports it to w.
func (cmd *command) exportWALFile(walFilePath string, w io.Writer, warnDelete func()) error {
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
				seriesKey, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				name := models.ParseName(seriesKey)
				if !cmd.matchMeasurement(string(name)) {
					continue
				}
				// seriesKey are stored escaped, field names are not
				field = escape.Bytes(field)
				if err := cmd.writeValues(w, seriesKey, string(field), values); err != nil {
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
func (cmd *command) writeValues(w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if (ts < cmd.startTime) || (ts > cmd.endTime) {
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

func (cmd *command) usingStdOut() bool {
	return cmd.out == stdoutMark
}

func (cmd *command) matchMeasurement(m string) bool {
	if len(cmd.measurement) == 0 && len(cmd.regexpMeasurement) == 0 {
		return true
	}
	if len(cmd.measurement) > 0 {
		if _, ok := cmd.measurement[m]; ok {
			return true
		}
	}
	if len(cmd.regexpMeasurement) > 0 {
		for _, rem := range cmd.regexpMeasurement {
			if rem.MatchString(m) {
				return true
			}
		}
	}
	return false
}

func (cmd *command) withMeasurement() string {
	if len(cmd.measurement) > 0 && len(cmd.regexpMeasurement) > 0 {
		return fmt.Sprintf(" with %d measurements and %d regexp measurements", len(cmd.measurement), len(cmd.regexpMeasurement))
	} else if len(cmd.measurement) > 0 {
		return fmt.Sprintf(" with %d measurements", len(cmd.measurement))
	} else if len(cmd.regexpMeasurement) > 0 {
		return fmt.Sprintf(" with %d regexp measurements", len(cmd.regexpMeasurement))
	} else {
		return ""
	}
}
