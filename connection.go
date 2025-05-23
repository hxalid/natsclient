package natsclient

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

// ConnectJetStream connects to NATS and initializes JetStream with a domain.
func ConnectJetStream(ctx context.Context, natsURL string, connOpts *ConnOptions, domain string) (*nats.Conn, jetstream.JetStream, error) {
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
		return nil, nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.NewWithDomain(nc, domain)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	return nc, js, nil
}
