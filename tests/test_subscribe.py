"""Integration tests for subscribe/consume instrumentation.

These tests require a running NATS server (via docker-compose).
"""

from __future__ import annotations

import asyncio

import nats
import pytest
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

from tests.conftest import NATS_URL

pytestmark = pytest.mark.integration


class TestSubscribeCallbackInstrumentation:
    async def test_subscribe_callback_creates_consumer_span(self, spans):
        """Messages delivered via callback should create CONSUMER spans."""
        nc = await nats.connect(NATS_URL)
        received = asyncio.Event()

        async def handler(msg):
            received.set()

        try:
            await nc.subscribe("test.sub.cb", cb=handler)
            await nc.publish("test.sub.cb", b"hello")
            await nc.flush()
            await asyncio.wait_for(received.wait(), timeout=2.0)
        finally:
            await nc.close()

        finished_spans = spans()
        # Should have at least a PRODUCER span (publish) and a CONSUMER span (callback)
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]

        assert len(producer_spans) >= 1
        assert len(consumer_spans) == 1

        consumer = consumer_spans[0]
        assert consumer.name == "test.sub.cb receive"
        assert consumer.attributes[SpanAttributes.MESSAGING_SYSTEM] == "nats"
        assert (
            consumer.attributes[SpanAttributes.MESSAGING_DESTINATION_NAME]
            == "test.sub.cb"
        )
        assert (
            consumer.attributes[SpanAttributes.MESSAGING_OPERATION] == "receive"
        )

    async def test_subscribe_callback_links_producer_consumer_traces(
        self, spans
    ):
        """Consumer span should be a child of the producer span via propagation."""
        nc = await nats.connect(NATS_URL)
        received = asyncio.Event()

        async def handler(msg):
            received.set()

        try:
            await nc.subscribe("test.sub.trace", cb=handler)
            await nc.publish("test.sub.trace", b"hello")
            await nc.flush()
            await asyncio.wait_for(received.wait(), timeout=2.0)
        finally:
            await nc.close()

        finished_spans = spans()
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]

        assert len(producer_spans) >= 1
        assert len(consumer_spans) == 1

        producer = producer_spans[0]
        consumer = consumer_spans[0]

        # They should share the same trace ID (linked via context propagation)
        assert producer.context.trace_id == consumer.context.trace_id, (
            "Producer and consumer should be in the same trace"
        )

        # Consumer's parent should be the producer
        assert consumer.parent is not None
        assert consumer.parent.span_id == producer.context.span_id

    async def test_subscribe_multiple_messages(self, spans):
        """Each message should create its own CONSUMER span."""
        nc = await nats.connect(NATS_URL)
        count = 0
        done = asyncio.Event()

        async def handler(msg):
            nonlocal count
            count += 1
            if count >= 3:
                done.set()

        try:
            await nc.subscribe("test.sub.multi", cb=handler)
            for i in range(3):
                await nc.publish("test.sub.multi", f"msg-{i}".encode())
            await nc.flush()
            await asyncio.wait_for(done.wait(), timeout=2.0)
        finally:
            await nc.close()

        finished_spans = spans()
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]
        assert len(consumer_spans) == 3


class TestSubscribeNextMsgInstrumentation:
    async def test_next_msg_creates_consumer_span(self, spans):
        """Messages retrieved via next_msg() should create CONSUMER spans."""
        nc = await nats.connect(NATS_URL)

        try:
            sub = await nc.subscribe("test.sub.next")
            await nc.publish("test.sub.next", b"hello")
            await nc.flush()
            msg = await sub.next_msg(timeout=2.0)
        finally:
            await nc.close()

        finished_spans = spans()
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]

        assert len(consumer_spans) == 1
        consumer = consumer_spans[0]
        assert consumer.name == "test.sub.next receive"

    async def test_next_msg_links_traces(self, spans):
        """next_msg() consumer span should link to the producer trace."""
        nc = await nats.connect(NATS_URL)

        try:
            sub = await nc.subscribe("test.sub.nextlink")
            await nc.publish("test.sub.nextlink", b"hello")
            await nc.flush()
            msg = await sub.next_msg(timeout=2.0)
        finally:
            await nc.close()

        finished_spans = spans()
        producer_spans = [
            s for s in finished_spans if s.kind == SpanKind.PRODUCER
        ]
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]

        assert len(producer_spans) >= 1
        assert len(consumer_spans) == 1

        producer = producer_spans[0]
        consumer = consumer_spans[0]

        assert producer.context.trace_id == consumer.context.trace_id
