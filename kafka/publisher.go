package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

//Pub ...
type Pub struct {
	Writer *kafka.Writer
	Topic  string
}

//NewKafkaPublisher creates and returns a new kafka reader
func NewKafkaPublisher(brokerAddress, topic string) (*Pub, error) {
	if brokerAddress == "" {
		return &Pub{}, ErrNoBrokers
	}
	if topic == "" {
		return &Pub{}, ErrNoTopic
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})

	return &Pub{
		Writer: writer,
		Topic:  topic,
	}, nil
}

//PublishRaw starts the subscription of messages from kafka
func (p *Pub) PublishRaw(id string, data []byte) error {
	return p.Writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(id),
			Value: data,
		},
	)
}

// Stop will initiate a graceful shutdown of the subscriber connection.
func (p *Pub) Stop() error {
	return p.Writer.Close()
}
