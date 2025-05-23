package natsclient

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type ConsumerConfig struct {
	Durable        string
	AckWait        time.Duration
	MaxDeliver     int
	DeliverPolicy  jetstream.DeliverPolicy
	ReplayPolicy   jetstream.ReplayPolicy
	MaxAckPending  int
	FilterSubjects []string
}

// ToJetStreamConfig converts the consumer config to a JetStream ConsumerConfig
func (c *ConsumerConfig) ToJetStreamConfig(defaultSubject string) jetstream.ConsumerConfig {
	subjects := c.FilterSubjects
	if len(subjects) == 0 {
		subjects = []string{defaultSubject}
	}

	return jetstream.ConsumerConfig{
		Durable:        c.Durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        c.AckWait,
		MaxDeliver:     c.MaxDeliver,
		DeliverPolicy:  c.DeliverPolicy,
		ReplayPolicy:   c.ReplayPolicy,
		FilterSubjects: subjects,
		MaxAckPending:  c.MaxAckPending,
	}
}

// DefaultConsumerConfig returns a ConsumerConfig with sensible defaults
func DefaultConsumerConfig(subject string) ConsumerConfig {
	return ConsumerConfig{
		AckWait:        30 * time.Second,
		MaxDeliver:     1,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
		MaxAckPending:  10000,
		FilterSubjects: []string{subject},
	}
}

// StreamConfig is a user-friendly config for creating streams
type StreamConfig struct {
	Name      string
	Subjects  []string
	Replicas  int
	MaxAge    time.Duration
	Storage   jetstream.StorageType
	Retention jetstream.RetentionPolicy
	Discard   jetstream.DiscardPolicy
}

func (s *StreamConfig) ToJetStreamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      s.Name,
		Subjects:  s.Subjects,
		Replicas:  s.Replicas,
		MaxAge:    s.MaxAge,
		Storage:   s.Storage,
		Retention: s.Retention,
		Discard:   s.Discard,
	}
}

func (s *StreamConfig) Validate() error {
	if s.Name == "" {
		return errors.New("stream name is required")
	}
	if len(s.Subjects) == 0 {
		return errors.New("at least one subject is required")
	}
	if s.Replicas < 1 {
		return errors.New("replicas must be at least 1")
	}
	return nil
}

// DefaultStreamConfig returns a StreamConfig with sensible defaults
func DefaultStreamConfig(name string, subjects []string) *StreamConfig {
	return &StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Replicas:  1,
		MaxAge:    0,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
	}
}

// ConnOptions configures how the library connects to NATS
type ConnOptions struct {
	Name             string
	ReconnectWait    time.Duration
	ReconnectRetries int
	User             string
	Pass             string
}

// DefaultConnOptions returns sensible defaults for NATS connection
func DefaultConnOptions(name, user, pass string) *ConnOptions {
	return &ConnOptions{
		Name:             name,
		ReconnectWait:    5 * time.Second,
		ReconnectRetries: 10,
		User:             user,
		Pass:             pass,
	}
}
