package kafka

import (
	"crypto/tls"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
)

type Config struct {
	BootstrapServers []string
	Username         string
	Password         string
	Topic            string
}

// Service wraps a sarama.AsyncProducer to create an events.Publisher that can be used
// with events.Engine
type Service struct {
	topic    string
	producer sarama.AsyncProducer
}

// New returns a Service object which can be used as a datagen.Publisher
func New(c Config) (*Service, error) {
	var err error

	s := Service{topic: c.Topic}
	s.producer, err = NewAsyncProducer(c)
	if err != nil {
		return nil, err
	}

	// just count the number of successful messages published...
	go func() {
		var n int
		for _ = range s.producer.Successes() {
			n++
			if n%10000 == 0 {
				log.Debug().Int("messages", n).Msg("messages sent")
			}
		}
	}()

	// log any errors talking to kafka
	go func() {
		for err := range s.producer.Errors() {
			log.Error().Err(err).Msg("failed to deliver message")
		}
	}()

	return &s, nil
}

func (s *Service) Publish(b []byte) {
	var msg = &sarama.ProducerMessage{
		Topic: s.topic,
		Value: sarama.ByteEncoder(b),
	}
	s.producer.Input() <- msg
}

func (s *Service) Close() error {
	return s.producer.Close()
}

// NewAsyncProducer returns an asynchronous kafka producer using the sarama package.
func NewAsyncProducer(c Config) (sarama.AsyncProducer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_8_1_0
	conf.Net.TLS.Enable = true
	conf.Net.TLS.Config = &tls.Config{}
	conf.Net.SASL.Enable = true
	conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	conf.Net.SASL.User = c.Username
	conf.Net.SASL.Password = c.Password
	conf.Producer.Partitioner = sarama.NewRandomPartitioner
	conf.Producer.RequiredAcks = sarama.NoResponse
	conf.Producer.Return.Successes = true
	conf.Producer.Flush.Frequency = time.Duration(100) * time.Millisecond
	// the default 1000000 results in request-too-large errors.
	sarama.MaxRequestSize = 999000

	return sarama.NewAsyncProducer(c.BootstrapServers, conf)
}
