# MaltaShop: Place Order Prototype

A small microservices prototype of the "place an order" flow for MaltaShop Ltd.

## What it is

- **Order service** (FastAPI, port 8001): exposes `POST /orders`, stores the order in its own SQLite database, and publishes an `OrderPlaced` event to RabbitMQ.
- **Notification service** (FastAPI): consumes `OrderPlaced` from a durable queue, ignores duplicates, and logs a confirmation email. It has its own SQLite database.
- **RabbitMQ**: the message broker (topic exchange `maltashop.events`).

Each service owns its own data store. Services talk only through the broker, never through each other's databases.

## How to run

Requires Docker and Docker Compose.

    docker compose up --build

Place an order:

    curl -X POST localhost:8001/orders \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: abc-1" \
      -H "X-Correlation-ID: demo-123" \
      -d '{"item":"kettle","qty":1}'

Check the correlation ID in both services:

    docker compose logs order notification | grep demo-123

Sending the same request again returns the same order and does not publish a second event.

## Environment variables

| Variable | Service | Purpose | Default |
|---|---|---|---|
| `RABBITMQ_URL` | order, notification | AMQP connection string | none (required) |
| `DB_PATH` | order, notification | SQLite file location | `/data/orders.db` or `/data/notifications.db` |
