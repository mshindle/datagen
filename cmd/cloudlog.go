package cmd

import (
	zlg "github.com/mark-ignacio/zerolog-gcp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mshindle/datagen/events"
)

type clogConfig struct {
	appConfig // inherit global config structure
	ProjectID string
	LogName   string
}

var clogCfg clogConfig

// clogCmd represents the cloudlog command
var clogCmd = &cobra.Command{
	Use:   "cloudlog",
	Short: "push generated data to Google Cloud Logging",
	Long: `
Generates fake mobile log data and sends it to Google Cloud Logging.`,
	RunE: clog,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// parse configuration
		err := viper.Unmarshal(&clogCfg)
		if err != nil {
			log.Error().Msg("unable to decode configuration parameters")
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(clogCmd)
	clogCmd.Flags().String("project", "", "specify the google project id to receive logs")
	clogCmd.Flags().String("name", "sample-log", " Sets the name of the log to write to")

	_ = viper.BindPFlag("projectID", clogCmd.Flags().Lookup("project"))
	_ = viper.BindPFlag("logName", clogCmd.Flags().Lookup("name"))
}

func clog(cmd *cobra.Command, args []string) error {
	// create a logging client
	w, err := zlg.NewCloudLoggingWriter(cmd.Context(), clogCfg.ProjectID, clogCfg.LogName, zlg.CloudLoggingOptions{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create client")
		return err
	}
	defer zlg.Flush()

	// attach it to zerolog
	log.Logger = log.Output(zerolog.MultiLevelWriter(
		zerolog.NewConsoleWriter(),
		w,
	))

	g := events.GeneratorFunc(generateMobileLog)
	e := events.New(g, p)
	return signalEngine(e)
}

type logPublisher struct{}

func (l logPublisher) Publish(b []byte) {
	log.Info().Str("data", string(b)).Msg("mobile log received")
}
