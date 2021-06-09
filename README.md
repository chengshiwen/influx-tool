# Influx Tool

[![Go Report Card](https://goreportcard.com/badge/chengshiwen/influx-tool)](https://goreportcard.com/report/chengshiwen/influx-tool)
[![LICENSE](https://img.shields.io/github/license/chengshiwen/influx-tool.svg)](https://github.com/chengshiwen/influx-tool/blob/master/LICENSE)
[![Releases](https://img.shields.io/github/release-pre/chengshiwen/influx-tool.svg)](https://github.com/chengshiwen/influx-tool/releases)
![GitHub stars](https://img.shields.io/github/stars/chengshiwen/influx-tool.svg?label=github%20stars&logo=github)

influx-tool - Influx Tool for [InfluxDB](https://docs.influxdata.com/influxdb/v1.8/) and [Influx Proxy](https://github.com/chengshiwen/influx-proxy)

## Usage

```
$ influx-tool --help

influx tool for influxdb and influx-proxy

Usage:
  influx-tool [command]

Available Commands:
  cleanup     Cleanup measurements with regexp
  compact     Compact the all shards fully
  help        Help about any command
  transfer    Transfer influxdb persist data on disk from one to another

Flags:
  -h, --help      help for influx-tool
  -v, --version   version for influx-tool

Use "influx-tool [command] --help" for more information about a command
```

### Cleanup

```
$ influx-tool cleanup --help

Cleanup measurements with regexp

Usage:
  influx-tool cleanup [flags]

Flags:
  -H, --host string       host to connect to (default "127.0.0.1")
  -P, --port int          port to connect to (default 8086)
  -d, --database string   database to connect to the server (required)
  -u, --username string   username to connect to the server
  -p, --password string   password to connect to the server
  -s, --ssl               use https for requests
  -r, --regexp string     regular expression of measurements to clean (default "", all)
  -m, --max-limit int     max limit to show measurements (default 0, no limit)
  -S, --show-num int      measurement number to show when show measurements (default 10)
  -D, --drop-num int      measurement number to drop per worker (default 1)
  -w, --worker int        number of concurrent workers to cleanup (default 10)
  -n, --progress int      print progress after every <n> measurements cleanup (default 10)
  -C, --cleanup           confirm cleanup the measurements (be cautious before doing it)
  -h, --help              help for cleanup
```

### Compact

```
$ influx-tool compact --help

Compact the all shards fully

Usage:
  influx-tool compact [flags]

Flags:
  -p, --path string   path of shard to be compacted like /path/to/influxdb/data/db/rp (required)
  -f, --force         force compaction without prompting (default: false)
  -w, --worker int    number of concurrent workers to compact (default: 0, unlimited)
  -h, --help          help for compact
```

### Transfer

```
$ influx-tool transfer --help

Transfer influxdb persist data on disk from one to another

Usage:
  influx-tool transfer [flags]

Flags:
  -s, --source-dir string         source influxdb directory containing meta, data and wal (required)
  -t, --target-dir string         target influxdb directory containing meta, data and wal (required)
  -d, --database string           database name (required)
  -r, --retention-policy string   retention policy (default "autogen")
      --duration duration         retention policy duration (default: 0)
      --shard-duration duration   retention policy shard duration (default 168h0m0s)
  -S, --start string              start time to transfer (RFC3339 format, optional)
  -E, --end string                end time to transfer (RFC3339 format, optional)
  -w, --worker int                number of concurrent workers to transfer (default: 0, unlimited)
      --skip-tsi                  skip building TSI index on disk (default: false)
  -n, --node-total int            total number of node in target circle (default 1)
  -i, --node-index intset         index of node in target circle delimited by comma, [0, node-total) (default: all)
  -k, --hash-key string           hash key for influx proxy, valid options are idx or exi (default "idx")
  -h, --help                      help for transfer
```

If you are using [Influx Proxy](https://github.com/chengshiwen/influx-proxy) v2.4.7+, and you need to transfer InfluxDB data, such as scaling, rebalancing and recovering.
This command is high-performance and doesn't need to start InfluxDB, and it's a direct transformation of disk data.

![image](https://raw.githubusercontent.com/wiki/chengshiwen/influx-tool/image/influx-tool-transfer.png)

If the original Influx Proxy cluster has 3 InfluxDB instances, corresponding to the following 3 data persistence directories
(the following directories are for reference only, and the actual directories shall prevail):

```
/data/source-1/influxdb
/data/source-2/influxdb
/data/source-3/influxdb
```

They need to be transferred to a new InfluxDB cluster. Assuming that one circle has 4 InfluxDB, the database name is `db`, the default retention policy is `autogen`,
all data need to be transferred，and we can use 8 worker threads, so the following commands should be executed:

```bash
./influx-tools transfer --source-dir /data/source-1/influxdb --target-dir /data/target/influxdb --database db --node-total 4 --worker 8
./influx-tools transfer --source-dir /data/source-2/influxdb --target-dir /data/target/influxdb --database db --node-total 4 --worker 8
./influx-tools transfer --source-dir /data/source-3/influxdb --target-dir /data/target/influxdb --database db --node-total 4 --worker 8

./influx-tools compact --path /data/target/influxdb-0/data/db/autogen --force --worker 8
./influx-tools compact --path /data/target/influxdb-1/data/db/autogen --force --worker 8
./influx-tools compact --path /data/target/influxdb-2/data/db/autogen --force --worker 8
./influx-tools compact --path /data/target/influxdb-3/data/db/autogen --force --worker 8
```

The first 3 commands transfer the old 3 InfluxDB data to the `/data/target/influxdb` directory, and generate the following 4 new influxdb data directories,
the order of which is the same as the order of backends in new `proxy.json`:

```
/data/target/influxdb-0
/data/target/influxdb-1
/data/target/influxdb-2
/data/target/influxdb-3
```

The last 4 commands are to compact and optimize the transferred data, such as the optimization of duplicate data and error data.
Of course, there will be no problems if they are not executed.
