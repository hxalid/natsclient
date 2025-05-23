package natsclient

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Consumer struct {
	nc              *nats.Conn
	js              jetstream.JetStream
	cons            jetstream.Consumer
	stream          string
	topic           string
	batchSize       int
	recordLatencyFn func(time.Duration)
}

func NewConsumer(ctx context.Context, natsURL, domain string, connOpts *ConnOptions, streamCfg *StreamConfig, topic string, batchSize int, config ConsumerConfig, recordLatencyFn func(time.Duration)) (*Consumer, error) {
	nc, js, err := ConnectJetStream(ctx, natsURL, connOpts, domain)
	if err != nil {
		return nil, err
	}

	return NewConsumerWithJS(ctx, nc, js, streamCfg, topic, batchSize, config, recordLatencyFn)
}

// NewConsumerWithJS allows instantiating a Consumer with an existing connection and JS context.
func NewConsumerWithJS(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, streamCfg *StreamConfig, topic string, batchSize int, config ConsumerConfig, recordLatencyFn func(time.Duration)) (*Consumer, error) {
	if err := streamCfg.Validate(); err != nil {
		return nil, err
	}

	jsConfig := config.ToJetStreamConfig(topic)
	timeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cons, err := js.CreateOrUpdateConsumer(timeCtx, streamCfg.Name, jsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{
		nc:              nc,
		js:              js,
		cons:            cons,
		stream:          streamCfg.Name,
		topic:           topic,
		batchSize:       batchSize,
		recordLatencyFn: recordLatencyFn,
	}, nil
}

func (c *Consumer) Poll(ctx context.Context) error {
	msgs, err := c.cons.Fetch(c.batchSize)
	if err != nil {
		return fmt.Errorf("fetch failed: %w", err)
	}

	for msg := range msgs.Messages() {
		if c.recordLatencyFn != nil {
			if meta, err := msg.Metadata(); err == nil && meta != nil {
				c.recordLatencyFn(time.Since(meta.Timestamp))
			} else if sent := msg.Headers().Get("X-Sent-Time"); sent != "" {
				if t, err := time.Parse(time.RFC3339Nano, sent); err == nil {
					c.recordLatencyFn(time.Since(t))
				}
			}
		}
		_ = msg.Ack()
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.nc.Drain()
}
