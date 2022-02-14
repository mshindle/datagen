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

type elasticConfig struct {
	appConfig // inherit global config structure
	Elastic   elastic.Config
}

var elasticCfg elasticConfig // elasticCfg contains elastic execution configuration

// elasticCmd represents the api command
var elasticCmd = &cobra.Command{
	Use:   "elastic",
	Short: "publish data directly to Elastic / Open Search",
	RunE:  runElastic,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// parse configuration
		err := viper.Unmarshal(&elasticCfg)
		if err != nil {
			log.Error().Msg("unable to decode configuration parameters")
			return err
		}
		return nil
	},
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
	p, err := elastic.New(cmd.Context(), elasticCfg.Elastic)
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
	e := events.New(g, p).WithGenerators(1).WithPublishers(1)
	return signalEngine(e)
}
