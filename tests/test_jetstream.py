"""Integration tests for JetStream instrumentation.

These tests require a running NATS server with JetStream enabled (via docker-compose).
"""

from __future__ import annotations

import asyncio

import nats
import pytest
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

from tests.conftest import NATS_URL

pytestmark = pytest.mark.integration


class TestJetStreamPublishInstrumentation:
    async def test_jetstream_publish_creates_producer_span(self, spans):
        """JetStreamContext.publish() should create a PRODUCER span."""
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()

        try:
            # Create a stream
            await js.add_stream(name="TEST_PUB", subjects=["js.test.pub.>"])
            await js.publish("js.test.pub.span", b"hello")
        finally:
            try:
                await js.delete_stream("TEST_PUB")
            except Exception:
                pass
            await nc.close()

        finished_spans = spans()
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]

        assert len(producer_spans) >= 1
        # Find the JetStream publish span (not admin API spans)
        js_span = [s for s in producer_spans if "js.test.pub.span" in s.name]
        assert len(js_span) == 1
        span = js_span[0]
        assert span.name == "js.test.pub.span send"
        assert span.attributes[SpanAttributes.MESSAGING_SYSTEM] == "nats"

    async def test_jetstream_publish_with_stream_name(self, spans):
        """JetStream publish with explicit stream should add stream attribute."""
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()

        try:
            await js.add_stream(
                name="TEST_STREAM", subjects=["js.test.stream.>"]
            )
            await js.publish(
                "js.test.stream.attr", b"hello", stream="TEST_STREAM"
            )
        finally:
            try:
                await js.delete_stream("TEST_STREAM")
            except Exception:
                pass
            await nc.close()

        finished_spans = spans()
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]

        js_span = [s for s in producer_spans if "js.test.stream.attr" in s.name]
        assert len(js_span) == 1
        span = js_span[0]
        assert (
            span.attributes.get("messaging.destination.stream") == "TEST_STREAM"
        )


class TestJetStreamSubscribeInstrumentation:
    async def test_jetstream_subscribe_creates_consumer_span(self, spans):
        """JetStream subscribe should create CONSUMER spans for delivered messages."""
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        received = asyncio.Event()

        async def handler(msg):
            await msg.ack()
            received.set()

        try:
            await js.add_stream(name="TEST_SUB", subjects=["js.test.sub.>"])
            await js.subscribe("js.test.sub.span", cb=handler, manual_ack=True)
            await js.publish("js.test.sub.span", b"hello")
            await asyncio.wait_for(received.wait(), timeout=5.0)
        finally:
            try:
                await js.delete_stream("TEST_SUB")
            except Exception:
                pass
            await nc.close()

        finished_spans = spans()
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]
        # Find the actual message consumer span (not _INBOX responses)
        js_consumers = [
            s for s in consumer_spans if "js.test.sub.span" in s.name
        ]

        assert len(js_consumers) >= 1

    async def test_jetstream_pull_subscribe(self, spans):
        """JetStream pull_subscribe + fetch should produce spans."""
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()

        try:
            await js.add_stream(name="TEST_PULL", subjects=["js.test.pull.>"])
            psub = await js.pull_subscribe(
                "js.test.pull.span", durable="test-durable"
            )

            # Publish a message
            await js.publish("js.test.pull.span", b"hello")
            await asyncio.sleep(0.5)

            # Fetch messages
            msgs = await psub.fetch(1, timeout=5.0)
            assert len(msgs) == 1
            await msgs[0].ack()
        finally:
            try:
                await js.delete_stream("TEST_PULL")
            except Exception:
                pass
            await nc.close()

        finished_spans = spans()
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]
        # JetStream publish should create a producer span
        assert len(producer_spans) >= 1

    async def test_jetstream_trace_propagation(self, spans):
        """JetStream publish -> subscribe should propagate trace context."""
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        received = asyncio.Event()

        async def handler(msg):
            await msg.ack()
            received.set()

        try:
            await js.add_stream(name="TEST_PROP", subjects=["js.test.prop.>"])
            await js.subscribe("js.test.prop.span", cb=handler, manual_ack=True)
            await js.publish("js.test.prop.span", b"hello")
            await asyncio.wait_for(received.wait(), timeout=5.0)
        finally:
            try:
                await js.delete_stream("TEST_PROP")
            except Exception:
                pass
            await nc.close()

        finished_spans = spans()
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]

        # Find the JS publish producer span and the matching consumer span
        js_producers = [
            s for s in producer_spans if "js.test.prop.span" in s.name
        ]
        js_consumers = [
            s for s in consumer_spans if "js.test.prop.span" in s.name
        ]

        assert len(js_producers) >= 1, (
            "Should have at least one JS producer span"
        )
        assert len(js_consumers) >= 1, (
            "Should have at least one JS consumer span"
        )

        producer = js_producers[0]
        consumer = js_consumers[0]
        assert producer.context.trace_id == consumer.context.trace_id, (
            "JetStream producer and consumer should be in the same trace"
        )
        assert consumer.parent is not None
        assert consumer.parent.span_id == producer.context.span_id
