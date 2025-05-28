package natsclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hxalid/natsclient"
	testhelper "github.com/hxalid/natsclient/testhelpers"
)

func TestPublisher_PublishAndFlush(t *testing.T) {
	nc, js, shutdown, err := testhelper.NewInProcessNATSServer()
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streamName := "TEST_STREAM"
	subject := "tenant.publish.test"

	streamCfg := natsclient.DefaultStreamConfig(streamName, []string{subject})
	err = natsclient.EnsureStreamExists(ctx, js, streamCfg)
	assert.NoError(t, err)

	_, err = js.Stream(ctx, streamName)
	assert.NoError(t, err)

	_, err = js.CreateOrUpdateStream(ctx, streamCfg.ToJetStreamConfig())
	assert.NoError(t, err)

	publisher, err := natsclient.NewPublisherWithJS(ctx, nc, js, streamCfg, subject, 2, nil)
	assert.NoError(t, err)
	defer publisher.Close()

	assert.NoError(t, publisher.Publish(ctx, []byte("hello1")))
	assert.NoError(t, publisher.Publish(ctx, []byte("hello2")))

	streams := js.ListStreams(ctx)
	for s := range streams.Info() {
		if s.Config.Name == streamName {
			assert.Contains(t, s.Config.Subjects, subject, "Expected subject to be part of the stream")
			assert.Equal(t, uint64(2), s.State.Msgs, "Expected 2 messages in the stream")
			break
		}
	}
}
