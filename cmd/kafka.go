package cmd

import (
	"github.com/mshindle/datagen/events"
	"github.com/mshindle/datagen/kafka"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type kafkaConfig struct {
	appConfig
	Kafka kafka.Config
}

var kafkaCfg kafkaConfig
var p *kafka.Service

// kafkaCmd represents the kafka command
var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "publish data directly to a kafka topic",
	RunE:  runKafka,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// parse configuration
		err := viper.Unmarshal(&kafkaCfg)
		if err != nil {
			log.Error().Msg("unable to decode configuration parameters")
			return err
		}
		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		if p != nil {
			return p.Close()
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(kafkaCmd)
}

func runKafka(cmd *cobra.Command, args []string) error {
	var err error

	p, err = kafka.New(kafkaCfg.Kafka)
	if err != nil {
		log.Error().Msg("unable to create kafka client")
		return err
	}
	g := events.GeneratorFunc(generateMobileLog)
	e := events.New(g, p).WithGenerators(kafkaCfg.Generators).WithPublishers(kafkaCfg.Publishers)
	return signalEngine(e)
}
