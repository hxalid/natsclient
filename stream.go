package natsclient

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// EnsureStreamExists checks if a stream exists, and creates it if it doesn't.
func EnsureStreamExists(ctx context.Context, js jetstream.JetStream, cfg *StreamConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid stream config: %w", err)
	}

	_, err := js.Stream(ctx, cfg.Name)
	if err == nil {
		return nil // Stream already exists
	}

	if _, err := js.CreateStream(ctx, cfg.ToJetStreamConfig()); err != nil {
		return fmt.Errorf("failed to create stream %s: %w", cfg.Name, err)
	}

	return nil
}
