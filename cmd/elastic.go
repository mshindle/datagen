package cmd

import (
	"runtime"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mshindle/datagen/elastic"
	"github.com/mshindle/datagen/events"
)

// elasticCmd represents the api command
var elasticCmd = &cobra.Command{
	Use:   "elastic",
	Short: "publish data directly to Elastic / Open Search",
	RunE:  runElastic,
	PostRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func init() {
	rootCmd.AddCommand(elasticCmd)
	viper.SetDefault("elastic.flushbytes", 5e+6)
	viper.SetDefault("elastic.flushinterval", 3*time.Second)
	viper.SetDefault("elastic.numworkers", runtime.NumCPU())
}

func runElastic(cmd *cobra.Command, args []string) error {
	p, err := elastic.New(cmd.Context(), cfgApp.Elastic)
	if err != nil {
		log.Error().Msg("unable to create elastic client")
		return err
	}
	err = p.ListIndices()
	if err != nil {
		log.Error().Msg("could not pull index alias")
		return err
	}

	g := events.GeneratorFunc(generateMobileLog)
	e, _ := events.NewEngine(g, p,
		events.WithNumGenerators(cfgApp.Generators),
		events.WithNumPublishers(cfgApp.Publishers),
	)
	return signalEngine(e)
}
