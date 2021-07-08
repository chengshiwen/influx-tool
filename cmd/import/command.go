package importer

import (
	"fmt"
	"log"

	"github.com/influxdata/influxdb/client"
	v8 "github.com/influxdata/influxdb/importer/v8"
	"github.com/spf13/cobra"
)

type flagpole struct {
	host         string
	port         int
	ssl          bool
	config       v8.Config
	clientConfig client.Config
}

func NewCommand() *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "import",
		Short:         "Import a previous export from file",
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
	cmd.Flags().StringVarP(&flags.clientConfig.Username, "username", "u", "", "username to connect to the server")
	cmd.Flags().StringVarP(&flags.clientConfig.Password, "password", "p", "", "password to connect to the server")
	cmd.Flags().BoolVarP(&flags.ssl, "ssl", "s", false, "use https for requests (default: false)")
	cmd.Flags().StringVarP(&flags.config.Path, "path", "f", "", "path to the file to import (required)")
	cmd.Flags().BoolVarP(&flags.config.Compressed, "compressed", "c", false, "set to true if the import file is compressed (default: false)")
	cmd.Flags().IntVar(&flags.config.PPS, "pps", 0, "points per second the import will allow (default: 0, unlimited)")
	cmd.MarkFlagRequired("path")
	return cmd
}

func processFlags(flags *flagpole) {
	addr := fmt.Sprintf("%s:%d", flags.host, flags.port)
	url, err := client.ParseConnectionString(addr, flags.ssl)
	if err != nil {
		log.Fatalf("parse url error: %s", err)
	}
	flags.clientConfig.URL = url
	flags.clientConfig.UnsafeSsl = flags.ssl
}

func runE(flags *flagpole) error {
	config := flags.config
	config.Config = flags.clientConfig
	i := v8.NewImporter(config)
	if err := i.Import(); err != nil {
		return err
	}
	return nil
}
