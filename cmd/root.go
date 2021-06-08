package cmd

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/chengshiwen/influx-tool/cmd/cleanup"
	"github.com/chengshiwen/influx-tool/cmd/compact"
	"github.com/chengshiwen/influx-tool/cmd/transfer"
	"github.com/spf13/cobra"
)

var (
	Version   = "unknown"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func init() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
}

func Execute() {
	cmd := NewCommand()
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Args:          cobra.NoArgs,
		Use:           "influx-tool",
		Short:         "influx tool for influxdb and influx-proxy",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version(),
	}
	cmd.SetVersionTemplate(`{{.Version}}`)
	cmd.AddCommand(cleanup.NewCommand())
	cmd.AddCommand(compact.NewCommand())
	cmd.AddCommand(transfer.NewCommand())
	return cmd
}

func version() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Version:    %s\n", Version))
	sb.WriteString(fmt.Sprintf("Git commit: %s\n", GitCommit))
	sb.WriteString(fmt.Sprintf("Build time: %s\n", BuildTime))
	sb.WriteString(fmt.Sprintf("Go version: %s\n", runtime.Version()))
	sb.WriteString(fmt.Sprintf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH))
	return sb.String()
}
