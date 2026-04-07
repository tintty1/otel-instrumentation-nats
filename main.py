"""Example: Using otel-instrumentation-nats with a console exporter."""

import asyncio

import nats
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

from otel_instrumentation_nats import NatsInstrumentor


async def main():
    # Set up OpenTelemetry with a console exporter
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)

    # Instrument nats-py
    NatsInstrumentor().instrument()

    # Connect to NATS
    nc = await nats.connect("nats://localhost:4222")

    # Publish (creates a PRODUCER span)
    await nc.publish("demo.subject", b"Hello, NATS!")

    # Subscribe with callback (creates CONSUMER spans)
    received = asyncio.Event()

    async def handler(msg):
        print(f"Received: {msg.data.decode()} on {msg.subject}")
        await msg.respond(b"pong")
        received.set()

    await nc.subscribe("demo.request", cb=handler)

    # Request-reply (creates a CLIENT span)
    response = await nc.request("demo.request", b"ping", timeout=2.0)
    print(f"Reply: {response.data.decode()}")

    await received.wait()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
