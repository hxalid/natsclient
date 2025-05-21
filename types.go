package natsclient

import (
	"context"
	"time"
)

type PublisherInterface interface {
	Publish(ctx context.Context, data []byte) error
	Close() error
}

type ConsumerInterface interface {
	Poll(ctx context.Context) error
	Close() error
}

type LatencyRecorder func(time.Duration)
