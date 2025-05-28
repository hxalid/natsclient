package natsclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hxalid/natsclient"
	testhelper "github.com/hxalid/natsclient/testhelpers"
)

func TestConsumer_Poll(t *testing.T) {
	nc, js, shutdown, err := testhelper.NewInProcessNATSServer()
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streamName := "TEST_STREAM"
	subject := "tenant.test.topic"

	streamCfg := natsclient.DefaultStreamConfig(streamName, []string{subject})
	consumerCfg := natsclient.DefaultConsumerConfig(subject)

	_, err = js.CreateOrUpdateStream(ctx, streamCfg.ToJetStreamConfig())
	assert.NoError(t, err)

	// Publish a message
	sentMsg := "hello"
	_, err = js.Publish(ctx, subject, []byte(sentMsg))
	assert.NoError(t, err)

	consumer, err := natsclient.NewConsumerWithJS(ctx, nc, js, streamCfg, subject, 5, consumerCfg, nil)
	assert.NoError(t, err)
	defer consumer.Close()

	receivedMsg := ""
	err = consumer.PollWithHandler(ctx, func(msg jetstream.Msg) error {
		receivedMsg = string(msg.Data())
		return nil
	})

	assert.Equal(t, sentMsg, receivedMsg)

	assert.NoError(t, err)
}
