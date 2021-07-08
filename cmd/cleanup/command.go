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

type flagpole struct {
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
	flags := &flagpole{}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "cleanup",
		Short:         "Cleanup measurements with regexp",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			processFlags(flags)
			return runE(flags)
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVarP(&flags.host, "host", "H", "127.0.0.1", "host to connect to")
	cmd.Flags().IntVarP(&flags.port, "port", "P", 8086, "port to connect to")
	cmd.Flags().StringVarP(&flags.database, "database", "d", "", "database to connect to the server (required)")
	cmd.Flags().StringVarP(&flags.username, "username", "u", "", "username to connect to the server")
	cmd.Flags().StringVarP(&flags.password, "password", "p", "", "password to connect to the server")
	cmd.Flags().BoolVarP(&flags.ssl, "ssl", "s", false, "use https for requests (default: false)")
	cmd.Flags().StringVarP(&flags.regexp, "regexp", "r", "", "regular expression of measurements to clean (default \"\", all)")
	cmd.Flags().IntVarP(&flags.maxLimit, "max-limit", "m", 0, "max limit to show measurements (default 0, no limit)")
	cmd.Flags().IntVarP(&flags.showNum, "show-num", "S", 10, "measurement number to show when show measurements")
	cmd.Flags().IntVarP(&flags.dropNum, "drop-num", "D", 1, "measurement number to drop per worker")
	cmd.Flags().IntVarP(&flags.worker, "worker", "w", 10, "number of concurrent workers to cleanup")
	cmd.Flags().IntVarP(&flags.progress, "progress", "n", 10, "print progress after every <n> measurements cleanup")
	cmd.Flags().BoolVarP(&flags.cleanup, "cleanup", "C", false, "confirm cleanup the measurements (be cautious before doing it, default: false)")
	cmd.MarkFlagRequired("database")
	return cmd
}

func processFlags(flags *flagpole) {
	if flags.maxLimit < 0 {
		log.Fatal("max-limit is invalid")
	}
	if flags.showNum <= 0 {
		log.Fatal("show-num is invalid")
	}
	if flags.dropNum <= 0 {
		log.Fatal("drop-num is invalid")
	}
	if flags.worker <= 0 {
		log.Fatal("worker is invalid")
	}
	if flags.progress <= 0 {
		log.Fatal("progress is invalid")
	}
}

func runE(flags *flagpole) (err error) {
	addr := fmt.Sprintf("http://%s:%d", flags.host, flags.port)
	if flags.ssl {
		addr = fmt.Sprintf("https://%s:%d", flags.host, flags.port)
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:               addr,
		Username:           flags.username,
		Password:           flags.password,
		InsecureSkipVerify: flags.ssl,
	})
	if err != nil {
		log.Printf("creating influxdb client error: %v", err)
		return
	}
	defer c.Close()

	var measurements []string
	query := "SHOW MEASUREMENTS"
	if flags.regexp != "" {
		query = fmt.Sprintf("%s WITH MEASUREMENT =~ /%s/", query, flags.regexp)
	}
	if flags.maxLimit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, flags.maxLimit)
	}
	log.Printf("query: %s", query)
	q := client.NewQuery(query, flags.database, "")
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
	if len(measurements) > flags.showNum {
		log.Printf("measurements: %v ... (total %d)", strings.Join(measurements[:flags.showNum], " "), len(measurements))
	} else if len(measurements) > 0 {
		log.Printf("measurements: %v (total %d)", strings.Join(measurements, " "), len(measurements))
	} else {
		log.Print("measurements: empty (total 0)")
		return
	}

	cleanup(flags, c, measurements)
	return
}

func cleanup(flags *flagpole, c client.Client, measurements []string) {
	if flags.cleanup {
		log.Print("cleanup measurements ...")
		limit := make(chan struct{}, flags.worker)
		wg := &sync.WaitGroup{}
		var done int64
		cycle := (len(measurements)-1)/flags.dropNum + 1
		for i := 0; i < cycle; i++ {
			queries := make([]string, 0, flags.dropNum)
			start := i * flags.dropNum
			end := (i + 1) * flags.dropNum
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

				q := client.NewQuery(query, flags.database, "")
				if response, err := c.Query(q); err == nil && response.Error() == nil {
					atomic.AddInt64(&done, int64(len(response.Results)))
					if atomic.LoadInt64(&done)%int64(flags.progress) == 0 {
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
		if done%int64(flags.progress) != 0 {
			log.Printf("%d/%d cleanup done", done, len(measurements))
		}
		log.Print("cleanup measurements done")
	}
}
