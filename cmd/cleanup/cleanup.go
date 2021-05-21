package cleanup

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/cobra"
)

type flags struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Ssl      bool
	Regexp   string
	MaxLimit int
	ShowNum  int
	DropNum  int
	Worker   int
	Progress int
	Cleanup  bool
}

func NewCommand() *cobra.Command {
	flags := &flags{}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "cleanup",
		Short:         "Cleanup unused measurements",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			processFlags(flags)
			return runE(flags)
		},
	}
	cmd.Flags().SortFlags = false
	cmd.Flags().StringVarP(&flags.Host, "host", "H", "127.0.0.1", "host to connect to")
	cmd.Flags().IntVarP(&flags.Port, "port", "P", 8086, "port to connect to")
	cmd.Flags().StringVarP(&flags.Database, "database", "d", "", "database to connect to the server (required)")
	cmd.Flags().StringVarP(&flags.Username, "username", "u", "", "username to connect to the server")
	cmd.Flags().StringVarP(&flags.Password, "password", "p", "", "password to connect to the server")
	cmd.Flags().BoolVarP(&flags.Ssl, "ssl", "s", false, "use https for requests")
	cmd.Flags().StringVarP(&flags.Regexp, "regexp", "r", "", "regular expression of measurements to clean (default \"\", all)")
	cmd.Flags().IntVarP(&flags.MaxLimit, "max-limit", "m", 0, "max limit to show measurements (default 0, no limit)")
	cmd.Flags().IntVarP(&flags.ShowNum, "show-num", "S", 5, "measurement number to show when show measurements")
	cmd.Flags().IntVarP(&flags.DropNum, "drop-num", "D", 10, "measurement number to drop per worker")
	cmd.Flags().IntVarP(&flags.Worker, "worker", "w", 10, "number of concurrent workers to cleanup")
	cmd.Flags().IntVarP(&flags.Progress, "progress", "n", 100, "print progress after every <n> measurements cleanup")
	cmd.Flags().BoolVarP(&flags.Cleanup, "cleanup", "C", false, "confirm cleanup the measurements (be cautious before doing it)")
	cmd.MarkFlagRequired("database")
	return cmd
}

func processFlags(flags *flags) {
	if flags.MaxLimit < 0 {
		log.Fatal("invalid max-limit")
	}
	if flags.ShowNum <= 0 {
		log.Fatal("invalid show-num")
	}
	if flags.DropNum <= 0 {
		log.Fatal("invalid drop-num")
	}
	if flags.Worker <= 0 {
		log.Fatal("invalid worker")
	}
	if flags.Progress <= 0 {
		log.Fatal("invalid progress")
	}
}

func runE(flags *flags) (err error) {
	addr := fmt.Sprintf("http://%s:%d", flags.Host, flags.Port)
	if flags.Ssl {
		addr = fmt.Sprintf("https://%s:%d", flags.Host, flags.Port)
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:               addr,
		Username:           flags.Username,
		Password:           flags.Password,
		InsecureSkipVerify: flags.Ssl,
	})
	if err != nil {
		log.Println("creating influxdb client error: ", err.Error())
		return
	}
	defer c.Close()

	var measurements []string
	query := "SHOW MEASUREMENTS"
	if flags.Regexp != "" {
		query = fmt.Sprintf("%s WITH MEASUREMENT =~ /%s/", query, flags.Regexp)
	}
	if flags.MaxLimit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, flags.MaxLimit)
	}
	log.Printf("query: %s", query)
	q := client.NewQuery(query, flags.Database, "")
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
	if len(measurements) > flags.ShowNum {
		log.Printf("measurements: %v ... (total %d)", strings.Join(measurements[:flags.ShowNum], " "), len(measurements))
	} else if len(measurements) > 0 {
		log.Printf("measurements: %v (total %d)", strings.Join(measurements, " "), len(measurements))
	} else {
		log.Println("measurements: empty (total 0)")
		return
	}

	cleanup(flags, c, measurements)
	return
}

func cleanup(flags *flags, c client.Client, measurements []string) {
	if flags.Cleanup {
		log.Println("cleanup measurements ...")
		pool, _ := ants.NewPool(flags.Worker)
		defer pool.Release()
		wg := &sync.WaitGroup{}
		var done int64
		cycle := (len(measurements)-1)/flags.DropNum + 1
		for i := 0; i < cycle; i++ {
			queries := make([]string, 0, flags.DropNum)
			start := i * flags.DropNum
			end := (i + 1) * flags.DropNum
			if end > len(measurements) {
				end = len(measurements)
			}
			for _, measurement := range measurements[start:end] {
				query := fmt.Sprintf("DROP MEASUREMENT \"%s\"", measurement)
				queries = append(queries, query)
			}
			query := strings.Join(queries, "; ")
			wg.Add(1)
			pool.Submit(func() {
				defer wg.Done()
				q := client.NewQuery(query, flags.Database, "")
				if response, err := c.Query(q); err == nil && response.Error() == nil {
					atomic.AddInt64(&done, int64(len(response.Results)))
					if atomic.LoadInt64(&done)%int64(flags.Progress) == 0 {
						log.Printf("%d/%d cleanup done", done, len(measurements))
					}
				} else if err != nil {
					log.Printf("cleanup error: %s", err)
				} else {
					log.Printf("cleanup response error: %s", response.Error())
				}
			})
		}
		wg.Wait()
		if done%int64(flags.Progress) != 0 {
			log.Printf("%d/%d cleanup done", done, len(measurements))
		}
		log.Println("cleanup measurements done")
	}
}
