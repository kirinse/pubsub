package kafka

import (
	"context"

	"pubsub"

	"github.com/segmentio/kafka-go"
)

//Sub holds the kafka reader, the topic being read, and the messages remaining until healthy
type Sub struct {
	Reader           *kafka.Reader
	QueueGroupMember bool
}

//NewKafkaSubscriber creates and returns a new kafka reader
func NewKafkaSubscriber(brokerAddress, topic, consumerGroupID string, startOffset, concurrentWrites int64) (*Sub, error) {
	if brokerAddress == "" {
		return &Sub{}, ErrNoBrokers
	}
	if topic == "" {
		return &Sub{}, ErrNoTopic
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		GroupID: consumerGroupID,
		Topic:   topic,
	})

	reader.SetOffset(startOffset)
	return &Sub{
		Reader:           reader,
		QueueGroupMember: consumerGroupID != "",
	}, nil
}

//Start starts the subscription of messages from kafka
func (s *Sub) Start() <-chan pubsub.SubscriberMessage {
	var lag int64
	var err error
	if !s.QueueGroupMember {
		lag, err = s.Reader.ReadLag(context.Background())
		if err != nil {
			return nil
		}
	}

	output := make(chan pubsub.SubscriberMessage)

	go func(
		s *Sub,
		output chan pubsub.SubscriberMessage,
	) {
		for {
			msg, err := s.Reader.ReadMessage(context.Background())
			if lag != int64(0) {
				lag--
			}
			if err != nil {
				return
			}

			output <- &subMessage{
				remaining: lag,
				message:   msg,
			}
		}
	}(s, output)

	return output
}

// Err will contain any errors returned from the consumer connection.
func (s *Sub) Err() error {
	return nil
}

// Stop will initiate a graceful shutdown of the subscriber connection.
func (s *Sub) Stop() error {
	return s.Reader.Close()
}
