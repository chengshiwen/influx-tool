package transfer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/chengshiwen/influx-tool/internal/binary"
	"github.com/chengshiwen/influx-tool/internal/errlist"
	"github.com/chengshiwen/influx-tool/internal/server"
	"github.com/chengshiwen/influx-tool/internal/shard"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type importer struct {
	MetaClient *meta.Client
	db         string
	dataDir    string
	rpi        *meta.RetentionPolicyInfo
	sfile      *tsdb.SeriesFile
	buildTsi   bool
}

const seriesBatchSize = 1000

func newImporter(svr *server.Server, db string, rp string, sd, d time.Duration, buildTsi bool) (*importer, error) {
	i := &importer{
		MetaClient: svr.MetaClient(),
		db:         db,
		dataDir:    svr.TSDBConfig().Dir,
		buildTsi:   buildTsi,
	}

	rps := &meta.RetentionPolicySpec{Name: rp, ShardGroupDuration: sd}
	if d >= time.Hour {
		rps.Duration = &d
	}
	err := i.createDatabase(rps)
	if err != nil {
		return i, err
	}

	sfile := tsdb.NewSeriesFile(filepath.Join(svr.TSDBConfig().Dir, db, tsdb.SeriesFileDirectory))
	if err = sfile.Open(); err != nil {
		return i, err
	}
	i.sfile = sfile

	return i, nil
}

func (i *importer) Close() error {
	el := errlist.NewErrorList()
	if i.sfile != nil {
		el.Add(i.sfile.Close())
	}
	return el.Err()
}

func (i *importer) createDatabase(rp *meta.RetentionPolicySpec) error {
	var rpi *meta.RetentionPolicyInfo
	dbInfo := i.MetaClient.Database(i.db)
	if dbInfo == nil {
		return i.createDatabaseWithRetentionPolicy(rp)
	}

	rpi, err := i.MetaClient.RetentionPolicy(i.db, rp.Name)
	if err != nil {
		return err
	}

	nonmatchingRp := (rpi != nil) && ((rp.Duration != nil && rpi.Duration != *rp.Duration) ||
		(rp.ReplicaN != nil && rpi.ReplicaN != *rp.ReplicaN) ||
		(rpi.ShardGroupDuration != rp.ShardGroupDuration))
	if nonmatchingRp {
		return fmt.Errorf("retention policy %v already exists with different parameters", rp.Name)
	}
	if _, err := i.MetaClient.CreateRetentionPolicy(i.db, rp, false); err != nil {
		return err
	}

	i.rpi, err = i.MetaClient.RetentionPolicy(i.db, rp.Name)
	return err
}

func (i *importer) createDatabaseWithRetentionPolicy(rp *meta.RetentionPolicySpec) error {
	var err error
	var dbInfo *meta.DatabaseInfo
	if len(rp.Name) == 0 {
		dbInfo, err = i.MetaClient.CreateDatabase(i.db)
	} else {
		dbInfo, err = i.MetaClient.CreateDatabaseWithRetentionPolicy(i.db, rp)
	}
	if err != nil {
		return err
	}
	i.rpi = dbInfo.RetentionPolicy(rp.Name)
	return nil
}

type importWorker struct {
	*importer
	currentShard uint64
	sh           *shard.Writer
	sw           *seriesWriter
	seriesBuf    []byte
}

func newImportWorker(importer *importer) *importWorker {
	i := &importWorker{
		importer: importer,
	}
	if !i.buildTsi {
		i.seriesBuf = make([]byte, 0, 2048)
	}
	return i
}

func (i *importWorker) ImportShard(reader *binary.Reader, start int64, end int64) error {
	err := i.StartShardGroup(i.sfile, start, end)
	if err != nil {
		return err
	}

	el := errlist.NewErrorList()
	var sh *binary.SeriesHeader
	var next bool
	for sh, err = reader.NextSeries(); (sh != nil) && (err == nil); sh, err = reader.NextSeries() {
		i.AddSeries(sh.SeriesKey)
		pr := reader.Points()
		seriesFieldKey := tsm1.SeriesFieldKeyBytes(string(sh.SeriesKey), string(sh.Field))

		for next, err = pr.Next(); next && (err == nil); next, err = pr.Next() {
			err = i.Write(seriesFieldKey, pr.Values())
			if err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}

	el.Add(err)
	el.Add(i.CloseShardGroup())

	return el.Err()
}

func (i *importWorker) StartShardGroup(sfile *tsdb.SeriesFile, start int64, end int64) error {
	existingSg, err := i.MetaClient.ShardGroupsByTimeRange(i.db, i.rpi.Name, time.Unix(0, start), time.Unix(0, end-1))
	if err != nil {
		return err
	}

	var sgi *meta.ShardGroupInfo
	var shardID uint64

	shardsPath := i.shardPath(i.rpi.Name)
	var shardPath string
	if len(existingSg) > 0 {
		sgi = &existingSg[0]
		if len(sgi.Shards) > 1 {
			return fmt.Errorf("multiple shards for the same owner %v and time range %v to %v", sgi.Shards[0].Owners, start, end)
		}

		shardID = sgi.Shards[0].ID

		shardPath = filepath.Join(shardsPath, strconv.Itoa(int(shardID)))
		_, err = os.Stat(shardPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	} else {
		sgi, err = i.MetaClient.CreateShardGroup(i.db, i.rpi.Name, time.Unix(0, start))
		if err != nil {
			return err
		}
		shardID = sgi.Shards[0].ID
	}

	shardPath = filepath.Join(shardsPath, strconv.Itoa(int(shardID)))
	if err = os.MkdirAll(shardPath, 0777); err != nil {
		return err
	}

	i.sh = shard.NewWriter(shardID, shardsPath, shard.AutoNumber())
	i.currentShard = shardID

	err = i.startSeriesFile(sfile)
	return err
}

func (i *importWorker) shardPath(rp string) string {
	return filepath.Join(i.dataDir, i.db, rp)
}

func (i *importWorker) removeShardGroup(rp string, shardID uint64) error {
	shardPath := i.shardPath(rp)
	err := os.RemoveAll(filepath.Join(shardPath, strconv.Itoa(int(shardID))))
	return err
}

func (i *importWorker) Write(key []byte, values tsm1.Values) error {
	if i.sh == nil {
		return errors.New("importer not currently writing a shard")
	}
	i.sh.Write(key, values)
	if i.sh.Err() != nil {
		el := errlist.NewErrorList()
		el.Add(i.sh.Err())
		el.Add(i.CloseShardGroup())
		el.Add(i.removeShardGroup(i.rpi.Name, i.currentShard))
		i.sh = nil
		i.currentShard = 0
		return el.Err()
	}
	return nil
}

func (i *importWorker) Close() error {
	el := errlist.NewErrorList()
	if i.sh != nil {
		el.Add(i.CloseShardGroup())
	}
	return el.Err()
}

func (i *importWorker) CloseShardGroup() error {
	el := errlist.NewErrorList()
	el.Add(i.closeSeriesFile())
	i.sh.Close()
	if i.sh.Err() != nil {
		el.Add(i.sh.Err())
	}
	i.sh = nil
	return el.Err()
}

func (i *importWorker) startSeriesFile(sfile *tsdb.SeriesFile) error {
	dataPath := filepath.Join(i.dataDir, i.db)
	shardPath := filepath.Join(i.dataDir, i.db, i.rpi.Name)

	var err error
	if i.buildTsi {
		i.sw, err = newTSI1SeriesWriter(sfile, i.db, dataPath, shardPath, int(i.sh.ShardID()))
	} else {
		i.sw, err = newInMemSeriesWriter(sfile, i.db, dataPath, shardPath, int(i.sh.ShardID()), i.seriesBuf)
	}

	if err != nil {
		return err
	}
	return nil
}

func (i *importWorker) AddSeries(seriesKey []byte) error {
	return i.sw.AddSeries(seriesKey)
}

func (i *importWorker) closeSeriesFile() error {
	return i.sw.Close()
}
