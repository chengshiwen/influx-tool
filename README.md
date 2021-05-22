# Influx Tool

[![Go Report Card](https://goreportcard.com/badge/chengshiwen/influx-tool)](https://goreportcard.com/report/chengshiwen/influx-tool)
[![LICENSE](https://img.shields.io/github/license/chengshiwen/influx-tool.svg)](https://github.com/chengshiwen/influx-tool/blob/master/LICENSE)
[![Releases](https://img.shields.io/github/release-pre/chengshiwen/influx-tool.svg)](https://github.com/chengshiwen/influx-tool/releases)
![GitHub stars](https://img.shields.io/github/stars/chengshiwen/influx-tool.svg?label=github%20stars&logo=github)

influx-tool - Influx Tool for [InfluxDB](https://docs.influxdata.com/influxdb/v1.8/) and [Influx Proxy](https://github.com/chengshiwen/influx-proxy)

## Usage

```bash
$ influx-tool --help

influx tool for influxdb and influx-proxy

Usage:
  influx-tool [command]

Available Commands:
  cleanup     Cleanup unused measurements
  help        Help about any command
  transfer    Transfer influxdb persist data on disk from one or more to targets

Flags:
  -h, --help      help for influx-tool
  -v, --version   version for influx-tool

Use "influx-tool [command] --help" for more information about a command
```
