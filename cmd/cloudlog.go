package cmd

import (
	"os"

	zlg "github.com/mark-ignacio/zerolog-gcp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mshindle/datagen/events"
)

type cloudConfig struct {
	appConfig // inherit global config structure
	ProjectID string
	LogName   string
}

var cloudCfg cloudConfig
var loggerCfg appConfig

var loggerCmd = &cobra.Command{
	Use:   "logger",
	Short: "publish generated data to log out",
	Long: `
Generates dice rolls for shooting craps. The result of two six-sided die (2d6) are 
logged into application's log output.'`,
	RunE: runLogger,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// parse configuration
		err := viper.Unmarshal(&loggerCfg)
		if err != nil {
			return err
		}
		return nil
	},
}

// cloudCmd represents the cloudlog command
var cloudCmd = &cobra.Command{
	Use:   "cloud",
	Short: "push generated data to Google Cloud Logging",
	Long: `
Generates dice rolls for shooting craps. The result of two six-sided die (2d6) are 
logged to Google Cloud Logging.`,
	RunE: runCloud,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// parse configuration
		err := viper.Unmarshal(&cloudCfg)
		if err != nil {
			log.Error().Msg("unable to decode configuration parameters")
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(loggerCmd)
	loggerCmd.AddCommand(cloudCmd)
	cloudCmd.Flags().String("project", "", "specify the google project id to receive logs")
	cloudCmd.Flags().String("name", "sample-log", "sets the name of the log to write to")

	_ = viper.BindPFlag("projectID", cloudCmd.Flags().Lookup("project"))
	_ = viper.BindPFlag("logName", cloudCmd.Flags().Lookup("name"))
}

func runLogger(cmd *cobra.Command, args []string) error {
	logger := loggerPublishAdapter{msg: "dice roll received", logger: log.Logger}
	e := events.New(shootCraps(), logger).WithPublishers(loggerCfg.Publishers).WithGenerators(loggerCfg.Generators)
	return signalEngine(e)
}

func runCloud(cmd *cobra.Command, args []string) error {
	// create a logging client
	w, err := zlg.NewCloudLoggingWriter(cmd.Context(), cloudCfg.ProjectID, cloudCfg.LogName, zlg.CloudLoggingOptions{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create client")
		return err
	}
	defer zlg.Flush()

	// attach it to zerolog
	log.Debug().Msg("attaching cloud logging")
	multi := zerolog.MultiLevelWriter(w, os.Stderr)
	gcpLogger := loggerPublishAdapter{msg: "dice roll received", logger: log.Output(multi)}

	e := events.New(shootCraps(), gcpLogger).WithPublishers(cloudCfg.Publishers).WithGenerators(cloudCfg.Generators)
	return signalEngine(e)
}

type loggerPublishAdapter struct {
	msg    string
	logger zerolog.Logger
}

func (l loggerPublishAdapter) Publish(b []byte) {
	l.logger.Info().Bytes("data", b).Msg(l.msg)
}
