package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hxalid/natsclient"
)

func main() {
	ctx := context.Background()

	// Settings
	natsURL := "nats://localhost:4222"
	user := "acc"
	pass := "acc"
	domain := "c0"
	streamName := "demo-stream"
	subject := "demo.subject"

	streamCfg := natsclient.DefaultStreamConfig(streamName, []string{subject})

	connOpts := natsclient.DefaultConnOptions("example-client", user, pass)

	publisher, err := natsclient.NewPublisher(ctx, natsURL, domain, connOpts, streamCfg, subject, 10, func(latency time.Duration) {
		fmt.Printf("Write latency: %v\n", latency)
	})
	if err != nil {
		log.Fatalf("Publisher error: %v", err)
	}
	defer publisher.Close()

	consumerCfg := natsclient.DefaultConsumerConfig(subject)
	consumer, err := natsclient.NewConsumer(ctx, natsURL, domain, connOpts, streamCfg, subject, 10, consumerCfg, func(latency time.Duration) {
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

	// Give time for server to process
	time.Sleep(1 * time.Second)

	fmt.Println("Consuming...")
	if err := consumer.Poll(ctx); err != nil {
		log.Printf("Poll error: %v", err)
	}

	fmt.Println("Done.")
}
