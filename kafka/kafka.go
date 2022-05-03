package kafka

import (
	"crypto/tls"
	"time"

	"github.com/Shopify/sarama"
)

type Config struct {
	BootstrapServers []string
	Username         string
	Password         string
	Topic            string
}

// Service wraps a sarama.AsyncProducer to create a generators.Publisher that can be used
// with generators.Engine
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

// Successes is the success output channel back to the user when Return.Successes is
// enabled. If Return.Successes is true, you MUST read from this channel or the
// Producer will deadlock. It is suggested that you send and read messages
// together in a single select statement.
func (s *Service) Successes() <-chan *sarama.ProducerMessage {
	return s.producer.Successes()
}

// Errors is the error output channel back to the user. You MUST read from this
// channel or the Producer will deadlock when the channel is full. Alternatively,
// you can set Producer.Return.Errors in your config to false, which prevents
// errors to be returned.
func (s *Service) Errors() <-chan *sarama.ProducerError {
	return s.producer.Errors()
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
