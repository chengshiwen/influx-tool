package deletetsm

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type command struct {
	cobraCmd    *cobra.Command
	measurement string // measurement to delete
	sanitize    bool   // remove all keys with non-printable unicode
	verbose     bool   // verbose logging
}

func NewCommand() *cobra.Command {
	cmd := &command{}
	cmd.cobraCmd = &cobra.Command{
		Args: func(c *cobra.Command, args []string) error {
			if err := cobra.MinimumNArgs(1)(c, args); err != nil {
				return errors.New("path required")
			}
			return nil
		},
		Use:           "deletetsm [flags] path...",
		Short:         "Delete a measurement from a raw tsm file",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.runE(args)
		},
	}
	flags := cmd.cobraCmd.Flags()
	flags.SortFlags = false
	flags.StringVarP(&cmd.measurement, "measurement", "m", "", "the name of the measurement to remove")
	flags.BoolVarP(&cmd.sanitize, "sanitize", "s", false, "remove all keys with non-printable unicode characters (default: false)")
	flags.BoolVarP(&cmd.verbose, "verbose", "v", false, "enable verbose logging (default: false)")
	return cmd.cobraCmd
}

func (cmd *command) validate() error {
	// Validate measurement or sanitize flag.
	if cmd.measurement == "" && !cmd.sanitize {
		return fmt.Errorf("--measurement or --sanitize flag required")
	}
	return nil
}

func (cmd *command) runE(args []string) error {
	if err := cmd.validate(); err != nil {
		return err
	}
	if !cmd.verbose {
		log.SetOutput(io.Discard)
	}

	// Process each TSM file.
	for _, path := range args {
		log.Printf("processing: %s", path)
		if err := cmd.process(path); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *command) process(path string) (retErr error) {
	// Open TSM reader.
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()

	r, err := tsm1.NewTSMReader(input)
	if err != nil {
		return fmt.Errorf("unable to read %s: %s", path, err)
	}
	defer r.Close()

	// Remove previous temporary files.
	outputPath := path + ".rewriting.tmp"
	if err := os.RemoveAll(outputPath); err != nil {
		return err
	} else if err := os.RemoveAll(outputPath + ".idx.tmp"); err != nil {
		return err
	}

	// Create TSMWriter to temporary location.
	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer output.Close()

	w, err := tsm1.NewTSMWriter(output)
	if err != nil {
		return err
	}
	defer w.Close()

	// Iterate over the input blocks.
	itr := r.BlockIterator()
	for itr.Next() {
		// Read key & time range.
		key, minTime, maxTime, _, _, block, err := itr.Read()
		if err != nil {
			return err
		}

		// Skip block if this is the measurement and time range we are deleting.
		series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(series)
		if string(measurement) == cmd.measurement || (cmd.sanitize && !models.ValidKeyTokens(measurement, tags)) {
			log.Printf("deleting block: %s (%s-%s) sz=%d",
				key,
				time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
				time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
				len(block),
			)
			continue
		}

		if err := w.WriteBlock(key, minTime, maxTime, block); err != nil {
			return err
		}
	}

	// Write index & close.
	if err := w.WriteIndex(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Replace original file with new file.
	return os.Rename(outputPath, path)
}
