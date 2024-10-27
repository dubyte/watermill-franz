package franz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed uint32
}

func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	config.OverwriteFranzOpts = append(config.OverwriteFranzOpts, kgo.SeedBrokers(config.Brokers...))
	if config.ConsumerGroup != "" {
		config.OverwriteFranzOpts = append(config.OverwriteFranzOpts, kgo.ConsumerGroup(config.ConsumerGroup))
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	// TODO: implement open telemetry on subscriber
	// if config.OTELEnabled && config.Tracer == nil {
	// 	config.Tracer = NewOTELSaramaTracer()
	// }

	return &Subscriber{
		config: config,
		logger: logger,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler Unmarshaler

	// OverwriteFranzOpts holds additional franz-go settings.
	OverwriteFranzOpts []kgo.Opt

	// Kafka consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration

	//TODO: remove or implement using franz-go
	// InitializeTopicDetails *sarama.TopicDetail

	//TODO: add open telemetry support for subscriber
	// // If true then each consumed message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	// //
	// // Deprecated: pass OTELSaramaTracer to Tracer field instead.
	// OTELEnabled bool

	// // Tracer is used to trace Kafka messages.
	// // If nil, then no tracing will be used.
	// Tracer SaramaTracer
}

// NoSleep can be set to SubscriberConfig.NackResendSleep and SubscriberConfig.ReconnectRetrySleep.
const NoSleep time.Duration = -1

func (c *SubscriberConfig) setDefaults() {
	if c.OverwriteFranzOpts == nil {
		c.OverwriteFranzOpts = DefaultFranzSubscriberOpts()
	}
	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaler")
	}

	return nil
}

// DefaultFranzSubscriberOpts creates default opts config used by Watermill.
func DefaultFranzSubscriberOpts() []kgo.Opt {
	var opts []kgo.Opt
	opts = append(opts, kgo.ClientID("watermill"))

	return opts
}

// Subscribe subscribers for messages in Kafka.
//
// There are multiple subscribers spawned
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, errors.New("subscriber closed")
	}

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message)

	s.logger.Info("Starting consuming", logFields)

	opts := s.config.OverwriteFranzOpts
	opts = append(opts, kgo.ConsumeTopics(topic))

	// Start with a client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cannot create new franz-go client: %w", err)
	}
	go s.consumeMessages(ctx, client, output, logFields)

	return output, nil
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	client *kgo.Client,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	s.logger.Info("Starting consuming", logFields)
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			lf := logFields.Add(watermill.LogFields{"topic": topic})
			lf = logFields.Add(watermill.LogFields{"partion": partition})
			s.logger.Error("fetch error", err, lf)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			msg, err := s.config.Unmarshaler.Unmarshal(record)
			if err != nil {
				s.logger.Error("fetch error", err, logFields)
				return
			}
			output <- msg
		})
	}
}

func (s *Subscriber) Close() error {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}

	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("Kafka subscriber closed", nil)

	return nil
}
