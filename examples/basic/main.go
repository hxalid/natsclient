package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hxalid/natsclient"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	natsURL := "nats://localhost:4222"
	user := "acc"
	pass := "acc"
	domain := "c0"
	streamName := "demo-stream"
	subject := "demo.subject"

	streamCfg := natsclient.DefaultStreamConfig(streamName, []string{subject})
	connOpts := natsclient.DefaultConnOptions("example-client", user, pass)

	// Ensure stream exists before creating publisher/consumer
	nc, js, err := natsclient.ConnectJetStream(ctx, natsURL, connOpts, domain)
	if err != nil {
		log.Fatalf("Failed to connect to JetStream: %v", err)
	}
	defer nc.Drain()

	if err := natsclient.EnsureStreamExists(ctx, js, streamCfg); err != nil {
		log.Fatalf("Failed to ensure stream exists: %v", err)
	}

	// Create publisher and consumer
	publisher, err := natsclient.NewPublisherWithJS(ctx, nc, js, streamCfg, subject, 10, func(latency time.Duration) {
		fmt.Printf("Write latency: %v\n", latency)
	})
	if err != nil {
		log.Fatalf("Publisher error: %v", err)
	}
	defer publisher.Close()

	consumerCfg := natsclient.DefaultConsumerConfig(subject)
	consumer, err := natsclient.NewConsumerWithJS(ctx, nc, js, streamCfg, subject, 10, consumerCfg, func(latency time.Duration) {
		fmt.Printf("Read latency: %v\n", latency)
	})
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
	defer consumer.Close()

	fmt.Println("Publishing...")
	for i := 0; i < 25; i++ {
		msg := fmt.Sprintf("Hello #%d", i)
		if err := publisher.Publish(ctx, []byte(msg)); err != nil {
			log.Printf("Publish error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Wait before polling to ensure delivery
	time.Sleep(500 * time.Millisecond)

	fmt.Println("Consuming...")
	if err := consumer.Poll(ctx); err != nil {
		log.Printf("Poll error: %v", err)
	}

	fmt.Println("Done.")
}
