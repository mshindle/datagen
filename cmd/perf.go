package cmd

import (
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mshindle/datagen/kafka"
)

var msgSize, numMessages int

// perfCmd represents the perf command
var perfCmd = &cobra.Command{
	Use:   "perf",
	Short: "run a simple performance test against kafka",
	Long: `
Runs a load performance test against kafka by inserting n messages of msgsize bytes into the given topic.
Perf bypasses all of the data generation routines and sends data directly to the Kafka endpoint.
`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		msgSize = viper.GetInt("size")
		numMessages = viper.GetInt("msgs")
		return nil
	},
	RunE: kafkaPerf,
}

func init() {
	kafkaCmd.AddCommand(perfCmd)
	perfCmd.Flags().Int("size", 64, "message size in bytes")
	perfCmd.Flags().Int("msgs", 10_000_000, "the number of messages")

	_ = viper.BindPFlag("size", perfCmd.Flags().Lookup("size"))
	_ = viper.BindPFlag("msgs", perfCmd.Flags().Lookup("msgs"))
}

func kafkaPerf(cmd *cobra.Command, args []string) error {
	ap, err := kafka.NewAsyncProducer(cfgApp.Kafka)
	if err != nil {
		log.Error().Msg("unable to create kafka client")
		return err
	}

	var value = make([]byte, msgSize)
	_, _ = rand.Read(value)

	done := make(chan bool)
	go func() {
		var n int
		for _ = range ap.Successes() {
			n++
			if n%numMessages == 0 {
				done <- true
			}
		}
	}()

	go func() {
		for err := range ap.Errors() {
			log.Fatal().Err(err).Msg("failed to deliver message")
		}
	}()

	defer func() {
		err := ap.Close()
		if err != nil {
			log.Error().Err(err).Msg("failed to close producer")
		}
	}()

	ack := numMessages / 10
	var start = time.Now()
	for j := 0; j < numMessages; j++ {
		if j%ack == 0 {
			log.Debug().Int("msgs", j).Msg("messages sent")
		}
		ap.Input() <- &sarama.ProducerMessage{
			Topic: cfgApp.Kafka.Topic,
			Value: sarama.ByteEncoder(value),
		}
	}

	<-done
	elapsed := time.Since(start)

	log.Info().
		Float64("seconds", elapsed.Seconds()).
		Float64("msg/s", float64(numMessages)/elapsed.Seconds()).
		Msg("sarama producer")

	return nil
}
