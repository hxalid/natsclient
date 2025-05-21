package natsclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

type asyncPublishTracker struct {
	sendTime  time.Time
	topic     string
	future    jetstream.PubAckFuture
	processed bool
}

type Publisher struct {
	nc              *nats.Conn
	js              jetstream.JetStream
	topic           string
	batchSize       int
	pendingMu       sync.Mutex
	pending         []*asyncPublishTracker
	recordLatencyFn func(time.Duration)
}

func NewPublisher(ctx context.Context, natsURL, domain string, connOpts ConnOptions, streamCfg StreamConfig, topic string, batchSize int, recordLatencyFn func(time.Duration)) (*Publisher, error) {
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

	return NewPublisherWithJS(ctx, nc, js, streamCfg, topic, batchSize, recordLatencyFn)
}

func NewPublisherWithJS(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, streamCfg StreamConfig, topic string, batchSize int, recordLatencyFn func(time.Duration)) (*Publisher, error) {
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

	return &Publisher{
		nc:              nc,
		js:              js,
		topic:           topic,
		batchSize:       batchSize,
		pending:         make([]*asyncPublishTracker, 0, batchSize),
		recordLatencyFn: recordLatencyFn,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, data []byte) error {
	sendTime := time.Now()
	msg := &nats.Msg{
		Subject: p.topic,
		Header:  nats.Header{"X-Sent-Time": []string{sendTime.Format(time.RFC3339Nano)}},
		Data:    data,
	}

	future, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return fmt.Errorf("async publish error: %w", err)
	}

	p.pendingMu.Lock()
	p.pending = append(p.pending, &asyncPublishTracker{sendTime, p.topic, future, false})
	flush := len(p.pending) >= p.batchSize
	p.pendingMu.Unlock()

	if flush {
		return p.flush(ctx)
	}
	return nil
}

func (p *Publisher) flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	select {
	case <-p.js.PublishAsyncComplete():
		p.pendingMu.Lock()

		for _, pub := range p.pending {
			if pub.processed {
				continue
			}
			select {
			case <-pub.future.Ok():
				if p.recordLatencyFn != nil {
					p.recordLatencyFn(time.Since(pub.sendTime))
				}
			case err := <-pub.future.Err():
				log.Error().Err(err).Str("topic", pub.topic).Msg("Async publish error")
			default:
			}
			pub.processed = true
		}

		p.pending = p.pending[:0]
		p.pendingMu.Unlock()

	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (p *Publisher) Close() error {
	p.pendingMu.Lock()

	if len(p.pending) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p.pendingMu.Unlock()
		defer cancel()

		_ = p.flush(ctx)
	} else {
		p.pendingMu.Unlock()
	}

	return p.nc.Drain()
}
