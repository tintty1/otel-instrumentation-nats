# otel-instrumentation-nats

OpenTelemetry auto-instrumentation for [nats-py](https://github.com/nats-io/nats.py).

Automatically creates spans for NATS publish, subscribe, request-reply, and JetStream operations with distributed trace context propagation through message headers.

## Installation

```bash
pip install otel-instrumentation-nats
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add otel-instrumentation-nats
```

The instrumented library (`nats-py`) is an optional dependency. Install it separately if you haven't already:

```bash
pip install nats-py
```

## Quick Start

```python
import asyncio
import nats
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

from otel_instrumentation_nats import NatsInstrumentor

async def main():
    # Set up OpenTelemetry
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)

    # Instrument nats-py -- call this before using the NATS client
    NatsInstrumentor().instrument()

    nc = await nats.connect("nats://localhost:4222")

    # This publish creates a PRODUCER span and injects
    # trace context into the message headers automatically
    await nc.publish("orders.new", b'{"item": "widget"}')

    await nc.close()

asyncio.run(main())
```

## What Gets Instrumented

| Method | Span Kind | Span Name | Description |
|--------|-----------|-----------|-------------|
| `Client.publish()` | `PRODUCER` | `{subject} send` | Fire-and-forget publish |
| `Client.subscribe()` | `CONSUMER` | `{subject} receive` | Per-message spans for both callback and `next_msg()` patterns |
| `Client.request()` | `CLIENT` | `{subject} request` | Request-reply (waits for response) |
| `JetStreamContext.publish()` | `PRODUCER` | `{subject} send` | JetStream publish with stream attributes |

### Span Attributes

All spans include standard [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/):

| Attribute | Value |
|-----------|-------|
| `messaging.system` | `nats` |
| `messaging.destination.name` | NATS subject (e.g. `orders.new`) |
| `messaging.operation` | `publish`, `receive`, or `request` |
| `server.address` | NATS server hostname |
| `server.port` | NATS server port |

JetStream publish spans additionally include:

| Attribute | Value |
|-----------|-------|
| `messaging.destination.kind` | `stream` |
| `messaging.destination.stream` | Stream name (when specified) |

## Trace Context Propagation

Trace context is automatically propagated through NATS message headers using the [W3C TraceContext](https://www.w3.org/TR/trace-context/) format. This means:

- **Publishers** inject `traceparent` and `tracestate` into message headers
- **Subscribers** extract trace context from incoming message headers and create child spans

This links producer and consumer spans into a single distributed trace, even across service boundaries.

```
Service A                          NATS                          Service B
─────────                          ────                          ─────────
publish("orders.new")  ──────►  message with  ──────►  subscribe callback
  │                             traceparent header        │
  ▼                                                       ▼
[PRODUCER span]                                    [CONSUMER span]
  trace_id: abc123                                   trace_id: abc123
  span_id:  def456                                   parent_id: def456
```

## Usage Patterns

### Publish/Subscribe with Callback

```python
NatsInstrumentor().instrument()

nc = await nats.connect()

async def on_message(msg):
    # A CONSUMER span is active here, linked to the producer trace
    print(f"Got {msg.data} on {msg.subject}")

await nc.subscribe("events.>", cb=on_message)
await nc.publish("events.click", b"data")  # Creates a PRODUCER span
```

### Subscribe with next_msg() (Pull Pattern)

```python
sub = await nc.subscribe("tasks")
await nc.publish("tasks", b"do-something")

msg = await sub.next_msg(timeout=5.0)  # Creates a CONSUMER span
```

### Request-Reply

```python
async def handler(msg):
    await msg.respond(b"pong")

await nc.subscribe("ping", cb=handler)

# Creates a CLIENT span that encompasses the full request-reply cycle
response = await nc.request("ping", b"data", timeout=2.0)
```

### JetStream

```python
js = nc.jetstream()

await js.add_stream(name="ORDERS", subjects=["orders.>"])

# Creates a PRODUCER span with stream attributes
await js.publish("orders.new", b"order-data", stream="ORDERS")

# Subscribe callback receives CONSUMER spans linked to the producer
async def process_order(msg):
    await msg.ack()

await js.subscribe("orders.new", cb=process_order, manual_ack=True)
```

### Custom Tracer Provider

```python
from opentelemetry.sdk.trace import TracerProvider

provider = TracerProvider()
# ... configure your provider with exporters, processors, etc.

NatsInstrumentor().instrument(tracer_provider=provider)
```

### Uninstrumenting

```python
instrumentor = NatsInstrumentor()
instrumentor.instrument()

# ... later, to restore original behavior:
instrumentor.uninstrument()
```

## Development

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- Docker (for integration tests)

### Setup

```bash
git clone <repo-url>
cd otel-instrumentation-nats
uv sync --extra instruments --extra test
```

### Running Tests

Start a NATS server (or use the included docker-compose):

```bash
docker compose up -d
```

Run the full test suite:

```bash
uv run pytest -v
```

Run only unit tests (no NATS server required):

```bash
uv run pytest tests/test_instrumentor.py tests/test_context_propagation.py -v
```

Run only integration tests:

```bash
uv run pytest -m integration -v
```

### Project Structure

```
src/otel_instrumentation_nats/
├── __init__.py               # Public API
├── instrumentor.py           # NatsInstrumentor (patches/restores methods)
├── publish_wrapper.py        # Client.publish() -> PRODUCER spans
├── subscribe_wrapper.py      # Client.subscribe() -> CONSUMER spans
├── request_wrapper.py        # Client.request() -> CLIENT spans
├── jetstream_wrapper.py      # JetStreamContext.publish() -> PRODUCER spans
├── context_propagation.py    # Trace context inject/extract via NATS headers
├── utils.py                  # Shared span building, nested suppression
├── version.py                # Package version
└── package.py                # Instrumented library version constraints
```

## Compatibility

| Dependency | Version |
|------------|---------|
| `nats-py` | `>= 2.0.0, < 3.0.0` |
| `opentelemetry-api` | `~= 1.5` |
| `opentelemetry-instrumentation` | `>= 0.50b0` |
| Python | `>= 3.13` |

## License

See [LICENSE](LICENSE) for details.
