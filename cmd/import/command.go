package importer

import (
	"fmt"

	"github.com/influxdata/influxdb/client"
	v8 "github.com/influxdata/influxdb/importer/v8"
	"github.com/spf13/cobra"
)

type command struct {
	cobraCmd     *cobra.Command
	host         string
	port         int
	ssl          bool
	config       v8.Config
	clientConfig client.Config
}

func NewCommand() *cobra.Command {
	cmd := &command{}
	cmd.cobraCmd = &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "import",
		Short:         "Import a previous export from file",
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
	flags.StringVarP(&cmd.clientConfig.Username, "username", "u", "", "username to connect to the server")
	flags.StringVarP(&cmd.clientConfig.Password, "password", "p", "", "password to connect to the server")
	flags.BoolVarP(&cmd.ssl, "ssl", "s", false, "use https for requests (default: false)")
	flags.StringVarP(&cmd.config.Path, "path", "f", "", "path to the file to import (required)")
	flags.BoolVarP(&cmd.config.Compressed, "compressed", "c", false, "set to true if the import file is compressed (default: false)")
	flags.IntVar(&cmd.config.PPS, "pps", 0, "points per second the import will allow (default: 0, unlimited)")
	cmd.cobraCmd.MarkFlagRequired("path")
	return cmd.cobraCmd
}

func (cmd *command) validate() error {
	addr := fmt.Sprintf("%s:%d", cmd.host, cmd.port)
	url, err := client.ParseConnectionString(addr, cmd.ssl)
	if err != nil {
		return fmt.Errorf("parse url error: %s", err)
	}
	cmd.clientConfig.URL = url
	cmd.clientConfig.UnsafeSsl = cmd.ssl
	return nil
}

func (cmd *command) runE() error {
	if err := cmd.validate(); err != nil {
		return err
	}
	config := cmd.config
	config.Config = cmd.clientConfig
	i := v8.NewImporter(config)
	if err := i.Import(); err != nil {
		return err
	}
	return nil
}
