# natsclient

A lightweight and modular Go client library for working with [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). It provides streamlined APIs for publishing and consuming messages with built-in batching, latency tracking, and optional connection reuse.

---

## ✅ Features

- Auto-creates JetStream streams and consumers if not found
- Asynchronous publishing with batching and flush thresholds
- Latency recording hooks for writes and reads
- Clean shutdown handling with `Close()` and `Drain()`
- Optional NATS connection reuse
- Defaults provided for all configuration structs
