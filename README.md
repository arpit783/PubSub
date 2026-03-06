# In-Memory Pub/Sub System

A simplified in-memory Pub/Sub system with WebSocket and REST API support.

## Features

- **WebSocket endpoint** (`/ws`) for publish, subscribe, unsubscribe, and ping operations
- **REST API** (`/api/*`) for topic management and observability
- **API key authentication** via `X-API-Key` header for REST and WebSocket
- **Concurrency safety** for multiple publishers and subscribers
- **Fan-out delivery**: every subscriber receives each message once
- **Topic isolation**: no cross-topic message leakage
- **Backpressure handling**: bounded per-subscriber queues with configurable overflow policy
- **Graceful shutdown**: stops accepting new operations, flushes pending messages, closes sockets
- **Server-side keep-alive**: automatic pings every 30 seconds to prevent idle timeouts

## Running Locally

### Prerequisites
- Java 21
- Maven 3.9+

### Build and Run

```bash
mvn clean package
java -jar target/pubsub-1.0-SNAPSHOT.jar
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 8080 | Server port |
| `QUEUE_CAPACITY` | 100 | Per-subscriber message queue size |
| `BACKPRESSURE_POLICY` | DROP_OLDEST | `DROP_OLDEST` or `DISCONNECT_SLOW_CONSUMER` |
| `API_KEYS` | (empty) | Comma-separated list of valid API keys. If empty, auth is disabled |

## Running with Docker

### Build

```bash
docker build -t pubsub:latest .
```

### Run

```bash
docker run -p 8080:8080 pubsub:latest
```

With custom configuration:

```bash
docker run -p 8080:8080 \
  -e QUEUE_CAPACITY=200 \
  -e BACKPRESSURE_POLICY=DISCONNECT_SLOW_CONSUMER \
  pubsub:latest
```

## Authentication

The API supports optional authentication via the `X-API-Key` header.

### Enabling Authentication

Set the `API_KEYS` environment variable with one or more comma-separated API keys:

```bash
# Single key
export API_KEYS="my-secret-key"

# Multiple keys
export API_KEYS="key1,key2,key3"

java -jar target/pubsub-1.0-SNAPSHOT.jar
```

Or with Docker:

```bash
docker run -p 8080:8080 -e API_KEYS="my-secret-key" pubsub:latest
```

### Using Authentication

**REST API:**
```bash
curl -H "X-API-Key: my-secret-key" http://localhost:8080/api/topics
```

**WebSocket (wscat):**
```bash
wscat -c ws://localhost:8080/ws -H "X-API-Key: my-secret-key"
```

**WebSocket (websocat):**
```bash
websocat -H="X-API-Key: my-secret-key" ws://localhost:8080/ws
```

### Public Endpoints

The following endpoints do not require authentication:
- `GET /api/health` - Health check endpoint

### Authentication Errors

**Missing API key:**
```json
{"error": "Missing X-API-Key header"}
```

**Invalid API key:**
```json
{"error": "Invalid API key"}
```

For WebSocket connections with invalid/missing API keys, the upgrade request is rejected (HTTP 401/403).

## API Reference

### WebSocket Endpoint

Connect to `ws://localhost:8080/ws`

#### Client Messages

**Subscribe**
```json
{
  "type": "subscribe",
  "topic": "orders",
  "client_id": "client-1",
  "last_n": 10,
  "request_id": "req-123"
}
```

**Unsubscribe**
```json
{
  "type": "unsubscribe",
  "topic": "orders",
  "client_id": "client-1",
  "request_id": "req-124"
}
```

**Publish**
```json
{
  "type": "publish",
  "topic": "orders",
  "message": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "payload": "Order created"
  },
  "request_id": "req-125"
}
```

**Ping**
```json
{
  "type": "ping",
  "request_id": "req-126"
}
```

#### Server Messages

**Acknowledgment**
```json
{
  "type": "ack",
  "request_id": "req-123",
  "topic": "orders",
  "ts": "2025-08-25T10:00:00Z"
}
```

**Event (message delivery)**
```json
{
  "type": "event",
  "topic": "orders",
  "message": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "payload": "Order created"
  },
  "ts": "2025-08-25T10:00:00Z"
}
```

**Error**
```json
{
  "type": "error",
  "request_id": "req-123",
  "error": {
    "code": "TOPIC_NOT_FOUND",
    "message": "Topic not found: orders"
  },
  "ts": "2025-08-25T10:00:00Z"
}
```

**Pong**
```json
{
  "type": "pong",
  "request_id": "req-126",
  "ts": "2025-08-25T10:00:00Z"
}
```

### REST API

#### Health Check
```
GET /api/health
```

Response:
```json
{
  "status": "healthy",
  "uptime_seconds": 3600
}
```

#### System Stats
```
GET /api/stats
```

Response:
```json
{
  "topics": 5,
  "total_subscribers": 10,
  "total_messages_published": 1000,
  "total_subscriptions": 15,
  "active_connections": 8,
  "start_time": "2025-08-25T09:00:00Z",
  "shutting_down": false
}
```

#### List Topics
```
GET /api/topics
```

Response:
```json
{
  "topics": ["orders", "notifications", "events"]
}
```

#### Create Topic
```
POST /api/topics
Content-Type: application/json

{
  "name": "orders"
}
```

Response (201 Created):
```json
{
  "name": "orders",
  "created_at": "2025-08-25T10:00:00Z"
}
```

#### Get Topic
```
GET /api/topics/{name}
```

Response:
```json
{
  "name": "orders",
  "subscriberCount": 5,
  "publishedCount": 100,
  "historySize": 50,
  "createdAt": "2025-08-25T09:00:00Z"
}
```

#### Get Topic Stats (with subscribers)
```
GET /api/topics/{name}/stats
```

Response:
```json
{
  "topic": {
    "name": "orders",
    "subscriberCount": 2,
    "publishedCount": 100,
    "historySize": 50,
    "createdAt": "2025-08-25T09:00:00Z"
  },
  "subscribers": [
    {
      "clientId": "client-1",
      "topicName": "orders",
      "deliveredCount": 50,
      "droppedCount": 0,
      "queueSize": 5,
      "queueCapacity": 100,
      "slowConsumer": false,
      "subscribedAt": "2025-08-25T09:30:00Z"
    }
  ]
}
```

#### Delete Topic
```
DELETE /api/topics/{name}
```

Response: 204 No Content

## Error Codes

| Code | Description |
|------|-------------|
| `BAD_REQUEST` | Invalid request format or missing required fields |
| `TOPIC_NOT_FOUND` | Topic does not exist |
| `TOPIC_ALREADY_EXISTS` | Topic with the same name already exists |
| `INVALID_MESSAGE` | Message format is invalid |
| `SLOW_CONSUMER` | Consumer is too slow (disconnect policy) |
| `INTERNAL_ERROR` | Internal server error |
| `SHUTTING_DOWN` | Server is shutting down |
