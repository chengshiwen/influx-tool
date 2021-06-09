package server

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

type Server struct {
	config   *run.Config
	noClient bool
	client   *meta.Client
}

func NewServer(dir string, tsi bool) (s *Server, err error) {
	s = &Server{}
	s.config = run.NewConfig()
	s.config.Meta.Dir = filepath.Join(dir, "meta")
	s.config.Data.Dir = filepath.Join(dir, "data")
	s.config.Data.WALDir = filepath.Join(dir, "wal")
	if tsi {
		s.config.Data.Index = tsdb.TSI1IndexName
	}

	if err = os.MkdirAll(s.config.Meta.Dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("meta dir %s mkdir error: %s", s.config.Meta.Dir, err)
	}

	// Validate the configuration.
	if err = s.config.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %s", err)
	}

	if s.noClient {
		return s, nil
	}

	s.client = meta.NewClient(s.config.Meta)
	if err = s.client.Open(); err != nil {
		s.client = nil
		return nil, err
	}
	return s, nil
}

func (s *Server) Close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *Server) MetaClient() *meta.Client { return s.client }
func (s *Server) TSDBConfig() tsdb.Config  { return s.config.Data }
