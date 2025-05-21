package natsclient

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
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

func NewConsumer(ctx context.Context, natsURL, domain string, connOpts ConnOptions, streamCfg StreamConfig, topic string, batchSize int, config ConsumerConfig, recordLatencyFn func(time.Duration)) (*Consumer, error) {
	opts := []nats.Option{
		nats.Name(connOpts.Name),
		nats.ReconnectWait(connOpts.ReconnectWait),
		nats.MaxReconnects(connOpts.ReconnectRetries),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Warn().Err(err).Msg("NATS disconnected")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info().Msg("NATS reconnected")
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Error().Err(err).Str("subject", sub.Subject).Msg("NATS async error")
		}),
		nats.UserInfo(connOpts.User, connOpts.Pass),
	}

	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.NewWithDomain(nc, domain)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	return NewConsumerWithJS(ctx, nc, js, streamCfg, topic, batchSize, config, recordLatencyFn)
}

func NewConsumerWithJS(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, streamCfg StreamConfig, topic string, batchSize int, config ConsumerConfig, recordLatencyFn func(time.Duration)) (*Consumer, error) {
	if err := streamCfg.Validate(); err != nil {
		return nil, err
	}

	_, err := js.Stream(ctx, streamCfg.Name)
	if err != nil {
		_, err = js.CreateStream(ctx, streamCfg.ToJetStreamConfig())
		if err != nil {
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
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
