package cleanup

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/spf13/cobra"
)

type command struct {
	cobraCmd *cobra.Command
	host     string
	port     int
	database string
	username string
	password string
	ssl      bool
	regexp   string
	maxLimit int
	showNum  int
	dropNum  int
	worker   int
	progress int
	cleanup  bool
}

func NewCommand() *cobra.Command {
	cmd := &command{}
	cmd.cobraCmd = &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "cleanup",
		Short:         "Cleanup measurements with regexp",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.runE()
		},
	}
	flags := cmd.cobraCmd.Flags()
	flags.SortFlags = false
	flags.StringVarP(&cmd.host, "host", "H", "127.0.0.1", "host to connect to")
	flags.IntVarP(&cmd.port, "port", "P", 8086, "port to connect to")
	flags.StringVarP(&cmd.database, "database", "d", "", "database to connect to the server (required)")
	flags.StringVarP(&cmd.username, "username", "u", "", "username to connect to the server")
	flags.StringVarP(&cmd.password, "password", "p", "", "password to connect to the server")
	flags.BoolVarP(&cmd.ssl, "ssl", "s", false, "use https for requests (default: false)")
	flags.StringVarP(&cmd.regexp, "regexp", "r", "", "regular expression of measurements to clean (default \"\", all)")
	flags.IntVarP(&cmd.maxLimit, "max-limit", "m", 0, "max limit to show measurements (default 0, no limit)")
	flags.IntVarP(&cmd.showNum, "show-num", "S", 10, "measurement number to show when show measurements")
	flags.IntVarP(&cmd.dropNum, "drop-num", "D", 1, "measurement number to drop per worker")
	flags.IntVarP(&cmd.worker, "worker", "w", 10, "number of concurrent workers to cleanup")
	flags.IntVarP(&cmd.progress, "progress", "n", 10, "print progress after every <n> measurements cleanup")
	flags.BoolVarP(&cmd.cleanup, "cleanup", "C", false, "confirm cleanup the measurements (be cautious before doing it, default: false)")
	cmd.cobraCmd.MarkFlagRequired("database")
	return cmd.cobraCmd
}

func (cmd *command) validate() {
	if cmd.maxLimit < 0 {
		log.Fatal("max-limit is invalid")
	}
	if cmd.showNum <= 0 {
		log.Fatal("show-num is invalid")
	}
	if cmd.dropNum <= 0 {
		log.Fatal("drop-num is invalid")
	}
	if cmd.worker <= 0 {
		log.Fatal("worker is invalid")
	}
	if cmd.progress <= 0 {
		log.Fatal("progress is invalid")
	}
}

func (cmd *command) runE() (err error) {
	cmd.validate()

	addr := fmt.Sprintf("http://%s:%d", cmd.host, cmd.port)
	if cmd.ssl {
		addr = fmt.Sprintf("https://%s:%d", cmd.host, cmd.port)
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:               addr,
		Username:           cmd.username,
		Password:           cmd.password,
		InsecureSkipVerify: cmd.ssl,
	})
	if err != nil {
		log.Printf("creating influxdb client error: %v", err)
		return
	}
	defer c.Close()

	var measurements []string
	query := "SHOW MEASUREMENTS"
	if cmd.regexp != "" {
		query = fmt.Sprintf("%s WITH MEASUREMENT =~ /%s/", query, cmd.regexp)
	}
	if cmd.maxLimit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, cmd.maxLimit)
	}
	log.Printf("query: %s", query)
	q := client.NewQuery(query, cmd.database, "")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		results := response.Results
		if len(results) > 0 {
			if len(results[0].Series) > 0 {
				if len(results[0].Series[0].Values) > 0 {
					measurements = make([]string, len(results[0].Series[0].Values))
					for i, v := range results[0].Series[0].Values {
						measurements[i] = v[0].(string)
					}
				}
			}
		}
	}
	if len(measurements) > cmd.showNum {
		log.Printf("measurements: %v ... (total %d)", strings.Join(measurements[:cmd.showNum], " "), len(measurements))
	} else if len(measurements) > 0 {
		log.Printf("measurements: %v (total %d)", strings.Join(measurements, " "), len(measurements))
	} else {
		log.Print("measurements: empty (total 0)")
		return
	}

	cmd.dropMeasurements(c, measurements)
	return
}

func (cmd *command) dropMeasurements(c client.Client, measurements []string) {
	if cmd.cleanup {
		log.Print("cleanup measurements ...")
		limit := make(chan struct{}, cmd.worker)
		wg := &sync.WaitGroup{}
		var done int64
		cycle := (len(measurements)-1)/cmd.dropNum + 1
		for i := 0; i < cycle; i++ {
			queries := make([]string, 0, cmd.dropNum)
			start := i * cmd.dropNum
			end := (i + 1) * cmd.dropNum
			if end > len(measurements) {
				end = len(measurements)
			}
			for _, measurement := range measurements[start:end] {
				query := fmt.Sprintf("DROP MEASUREMENT \"%s\"", measurement)
				queries = append(queries, query)
			}
			query := strings.Join(queries, "; ")
			wg.Add(1)
			go func() {
				limit <- struct{}{}
				defer func() {
					wg.Done()
					<-limit
				}()

				q := client.NewQuery(query, cmd.database, "")
				if response, err := c.Query(q); err == nil && response.Error() == nil {
					atomic.AddInt64(&done, int64(len(response.Results)))
					if atomic.LoadInt64(&done)%int64(cmd.progress) == 0 {
						log.Printf("%d/%d cleanup done", done, len(measurements))
					}
				} else if err != nil {
					log.Printf("cleanup error: %v", err)
				} else {
					log.Printf("cleanup response error: %v", response.Error())
				}
			}()
		}
		wg.Wait()
		if done%int64(cmd.progress) != 0 {
			log.Printf("%d/%d cleanup done", done, len(measurements))
		}
		log.Print("cleanup measurements done")
	}
}
