"""Integration tests for publish instrumentation.

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


class TestPublishInstrumentation:
    async def test_publish_creates_producer_span(self, spans):
        """Client.publish() should create a PRODUCER span."""
        nc = await nats.connect(NATS_URL)
        try:
            await nc.publish("test.publish", b"hello")
            await nc.flush()
        finally:
            await nc.close()

        finished_spans = spans()
        assert len(finished_spans) == 1

        span = finished_spans[0]
        assert span.name == "test.publish send"
        assert span.kind == SpanKind.PRODUCER
        assert span.attributes[SpanAttributes.MESSAGING_SYSTEM] == "nats"
        assert (
            span.attributes[SpanAttributes.MESSAGING_DESTINATION_NAME] == "test.publish"
        )
        assert span.attributes[SpanAttributes.MESSAGING_OPERATION] == "publish"

    async def test_publish_injects_trace_context_into_headers(self, spans):
        """Publish should inject traceparent/tracestate into message headers."""
        nc = await nats.connect(NATS_URL)
        received_headers = {}

        async def handler(msg):
            nonlocal received_headers
            received_headers = msg.headers or {}

        try:
            sub = await nc.subscribe("test.headers", cb=handler)
            await nc.publish("test.headers", b"hello")
            await nc.flush()
            await asyncio.sleep(0.2)
        finally:
            await nc.close()

        assert "traceparent" in received_headers

    async def test_publish_preserves_existing_headers(self, spans):
        """Publish should preserve user-provided headers."""
        nc = await nats.connect(NATS_URL)
        received_headers = {}

        async def handler(msg):
            nonlocal received_headers
            received_headers = msg.headers or {}

        try:
            sub = await nc.subscribe("test.preserve", cb=handler)
            await nc.publish(
                "test.preserve",
                b"hello",
                headers={"X-Custom": "my-value"},
            )
            await nc.flush()
            await asyncio.sleep(0.2)
        finally:
            await nc.close()

        assert received_headers.get("X-Custom") == "my-value"
        assert "traceparent" in received_headers

    async def test_publish_includes_server_info(self, spans):
        """PRODUCER span should include server address/port attributes."""
        nc = await nats.connect(NATS_URL)
        try:
            await nc.publish("test.serverinfo", b"hello")
            await nc.flush()
        finally:
            await nc.close()

        finished_spans = spans()
        assert len(finished_spans) == 1
        span = finished_spans[0]
        assert span.attributes.get(SpanAttributes.SERVER_ADDRESS) is not None
        assert span.attributes.get(SpanAttributes.SERVER_PORT) is not None
