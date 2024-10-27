package franz

import (
	"errors"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	config PublisherConfig
	client *kgo.Client
	logger watermill.LoggerAdapter

	closed bool
}

func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	opts := config.OverwriteFranzOpts
	opts = append(opts, kgo.SeedBrokers(config.Brokers...))

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka franz client")
	}

	// TODO: add support for open telemetry
	// if config.OTELEnabled && config.Tracer == nil {
	// 	config.Tracer = NewOTELSaramaTracer()
	// }

	return &Publisher{
		config: config,
		client: cl,
		logger: logger,
	}, err
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteFranzOpts holds additional sarama settings.
	OverwriteFranzOpts []kgo.Opt
}

func DefaultFranzPublisherOpts() []kgo.Opt {
	var opts []kgo.Opt
	opts = append(opts, kgo.ClientID("watermill"))

	return opts
}

func (c *PublisherConfig) setDefaults() {
	if c.OverwriteFranzOpts == nil {
		c.OverwriteFranzOpts = DefaultFranzPublisherOpts()
	}
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range messages {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		record, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return fmt.Errorf("cannot marshal message: %w", err)
		}

		r, err := p.client.ProduceSync(msg.Context(), record).First()
		if err != nil {
			return fmt.Errorf("cannot produce message %w", err)
		}

		logFields["kafka_partition"] = r.Partition
		logFields["kafka_partition_offset"] = r.Offset

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	p.client.Close()

	return nil
}
