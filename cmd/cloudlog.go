package cmd

import (
	"encoding/json"

	"github.com/mshindle/zlg"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mshindle/datagen/events"
)

var loggerCmd = &cobra.Command{
	Use:   "logger",
	Short: "publish generated data to log out",
	Long: `
Generates dice rolls for shooting craps. The result of two six-sided die (2d6) are 
logged into application's log output.'`,
	RunE: runLogger,
}

// cloudCmd represents the cloudlog command
var cloudCmd = &cobra.Command{
	Use:   "cloud",
	Short: "push generated data to Google Cloud Logging",
	Long: `
Generates dice rolls for shooting craps. The result of two six-sided die (2d6) are 
logged to Google Cloud Logging.`,
	RunE: runCloud,
}

func init() {
	rootCmd.AddCommand(loggerCmd)
	loggerCmd.AddCommand(cloudCmd)
	cloudCmd.Flags().String("project", "", "specify the google project id to receive logs")
	cloudCmd.Flags().String("name", "sample-log", "sets the name of the log to write to")

	_ = viper.BindPFlag("cloudlog.parent", cloudCmd.Flags().Lookup("project"))
	_ = viper.BindPFlag("cloudlog.logID", cloudCmd.Flags().Lookup("name"))
}

func runLogger(cmd *cobra.Command, args []string) error {
	logger := publishAdapter{logger: log.Logger}
	e := events.New(shootCraps(), logger).WithGenerators(cfgApp.Generators).WithPublishers(cfgApp.Publishers)
	return signalEngine(e)
}

func runCloud(cmd *cobra.Command, args []string) error {
	// attach it to zerolog
	lw, err := zlg.NewWriter(cmd.Context(), cfgApp.CloudLog.Parent, cfgApp.CloudLog.LogID)
	if err != nil {
		return err
	}
	defer zlg.Close()
	cpa := publishAdapter{logger: log.Output(lw)}
	e := events.New(shootCraps(), cpa).WithGenerators(cfgApp.Generators).WithPublishers(cfgApp.Publishers)
	log.Debug().Msg("attaching cloud logging")
	return signalEngine(e)
}

type publishAdapter struct {
	logger zerolog.Logger
}

func (c publishAdapter) Publish(b []byte) {
	c.logger.Info().Interface("throw", json.RawMessage(b)).Msg("dice rolled")
}
