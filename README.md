# natsclient

A lightweight, production-ready Go library for **publishing and consuming messages** with **NATS JetStream**, supporting batch async publishing, latency tracking, and clean resource management.

---

## 🚀 Features

- Publish messages asynchronously in batches
- Track write/read latency (optional)
- Create or reuse NATS JetStream streams and consumers
- Configurable stream and consumer settings
- Supports multiple JetStream domains
- Graceful connection cleanup
- Simplified API to reduce boilerplate

---

## 📦 Installation

```bash
go get github.com/hxalid/natsclient
```

## 🛠️ Quick Start

```go
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

	natsURL := "nats://localhost:4222"
	user := "acc"
	pass := "acc"
	domain := "c0"
	stream := "demo-stream"
	subject := "demo.subject"

	streamCfg := natsclient.DefaultStreamConfig(stream, []string{subject})
	connOpts := natsclient.DefaultConnOptions("client-1", user, pass)

	publisher, err := natsclient.NewPublisher(ctx, natsURL, domain, connOpts, streamCfg, subject, 10, func(latency time.Duration) {
		fmt.Printf("Write latency: %v\n", latency)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	consumerCfg := natsclient.DefaultConsumerConfig(subject)
	consumer, err := natsclient.NewConsumer(ctx, natsURL, domain, connOpts, streamCfg, subject, 10, consumerCfg, func(latency time.Duration) {
		fmt.Printf("Read latency: %v\n", latency)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Publish 25 messages
	for i := 0; i < 25; i++ {
		publisher.Publish(ctx, []byte(fmt.Sprintf("Hello #%d", i)))
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)

	// Consume
	if err := consumer.Poll(ctx); err != nil {
		log.Printf("Poll error: %v", err)
	}
}

```

## 📚 API Overview

### Create a Publisher

```go
publisher, err := natsclient.NewPublisher(ctx, natsURL, domain, connOpts, streamCfg, subject, batchSize, recordLatencyFn)
```

### Create a Consumer

```go
consumer, err := natsclient.NewConsumer(ctx, natsURL, domain, connOpts, streamCfg, subject, batchSize, consumerCfg, recordLatencyFn)
```

### Publish Message
```go
err := publisher.Publish(ctx, []byte("Hello"))
```

### Poll Messages

```go
err := consumer.Poll(ctx)
```

### 🔧 Config Structs

#### StreamConfig

Configure storage, retention, replicas, etc. as follows:

```go
streamCfg := natsclient.DefaultStreamConfig("demo-stream", []string{"demo.subject"})
```

#### ConsumerConfig

Configure max deliver, ack wait, and delivery policy as follows:

```go

consumerCfg := natsclient.DefaultConsumerConfig("demo.subject")
```

## 🧪 Tests and Examples

See [example](./examples/basic/main.go)
