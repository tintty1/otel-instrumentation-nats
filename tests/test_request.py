"""Integration tests for request/reply instrumentation.

These tests require a running NATS server (via docker-compose).
"""

from __future__ import annotations

import nats
import pytest
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

from tests.conftest import NATS_URL

pytestmark = pytest.mark.integration


class TestRequestReplyInstrumentation:
    async def test_request_creates_client_span(self, spans):
        """Client.request() should create a CLIENT span."""
        nc = await nats.connect(NATS_URL)

        async def handler(msg):
            await nc.publish(msg.reply, b"response")

        try:
            await nc.subscribe("test.req", cb=handler)
            response = await nc.request("test.req", b"hello", timeout=2.0)
            assert response.data == b"response"
        finally:
            await nc.close()

        finished_spans = spans()
        client_spans = [s for s in finished_spans if s.kind == SpanKind.CLIENT]

        assert len(client_spans) == 1
        span = client_spans[0]
        assert span.name == "test.req request"
        assert span.attributes[SpanAttributes.MESSAGING_SYSTEM] == "nats"
        assert (
            span.attributes[SpanAttributes.MESSAGING_DESTINATION_NAME]
            == "test.req"
        )
        assert span.attributes[SpanAttributes.MESSAGING_OPERATION] == "request"

    async def test_request_injects_trace_context(self, spans):
        """Request should inject trace context for the responder to extract."""
        nc = await nats.connect(NATS_URL)
        received_headers = {}

        async def handler(msg):
            nonlocal received_headers
            received_headers = msg.headers or {}
            await nc.publish(msg.reply, b"response")

        try:
            await nc.subscribe("test.req.headers", cb=handler)
            await nc.request("test.req.headers", b"hello", timeout=2.0)
        finally:
            await nc.close()

        assert "traceparent" in received_headers

    async def test_request_reply_full_trace(self, spans):
        """Full request-reply cycle should produce linked spans."""
        nc = await nats.connect(NATS_URL)

        async def handler(msg):
            await nc.publish(msg.reply, b"pong")

        try:
            await nc.subscribe("test.req.full", cb=handler)
            response = await nc.request("test.req.full", b"ping", timeout=2.0)
            assert response.data == b"pong"
        finally:
            await nc.close()

        finished_spans = spans()
        client_spans = [s for s in finished_spans if s.kind == SpanKind.CLIENT]
        # The handler's subscribe callback also creates consumer spans
        consumer_spans = [
            s for s in finished_spans if s.kind == SpanKind.CONSUMER
        ]

        assert len(client_spans) == 1

        # The consumer span should share the same trace as the client span
        if consumer_spans:
            assert (
                client_spans[0].context.trace_id
                == consumer_spans[0].context.trace_id
            )
