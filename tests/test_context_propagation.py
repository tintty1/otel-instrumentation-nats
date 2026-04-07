"""Unit tests for context propagation utilities."""

from __future__ import annotations

from opentelemetry import context, trace

from otel_instrumentation_nats.context_propagation import (
    extract_trace_context,
    inject_trace_context,
    nats_getter,
    nats_setter,
)


class TestNatsHeadersSetter:
    def test_set_value(self):
        headers = {}
        nats_setter.set(headers, "traceparent", "00-abc-def-01")
        assert headers["traceparent"] == "00-abc-def-01"

    def test_set_overwrites_existing(self):
        headers = {"traceparent": "old-value"}
        nats_setter.set(headers, "traceparent", "new-value")
        assert headers["traceparent"] == "new-value"


class TestNatsHeadersGetter:
    def test_get_existing_key(self):
        headers = {"traceparent": "00-abc-def-01"}
        result = nats_getter.get(headers, "traceparent")
        assert result == ["00-abc-def-01"]

    def test_get_missing_key(self):
        headers = {"other": "value"}
        result = nats_getter.get(headers, "traceparent")
        assert result is None

    def test_keys(self):
        headers = {"traceparent": "val1", "tracestate": "val2"}
        keys = list(nats_getter.keys(headers))
        assert sorted(keys) == ["traceparent", "tracestate"]

    def test_keys_empty(self):
        headers = {}
        keys = list(nats_getter.keys(headers))
        assert keys == []


class TestInjectTraceContext:
    def test_inject_creates_headers_if_none(self):
        result = inject_trace_context(None)
        assert isinstance(result, dict)

    def test_inject_preserves_existing_headers(self):
        headers = {"custom-key": "custom-value"}
        result = inject_trace_context(headers)
        assert result["custom-key"] == "custom-value"
        assert result is headers  # mutated in place

    def test_inject_adds_trace_context(self, tracer_provider):
        """When there's an active span, inject should add traceparent."""
        tracer = tracer_provider.get_tracer("test")

        with tracer.start_as_current_span("test-span"):
            headers = inject_trace_context(None)
            assert "traceparent" in headers


class TestExtractTraceContext:
    def test_extract_empty_headers(self):
        ctx = extract_trace_context(None)
        # Should return a valid context (current context)
        assert ctx is not None

    def test_extract_with_traceparent(self, tracer_provider):
        """Extract should reconstruct context from traceparent header."""
        tracer = tracer_provider.get_tracer("test")

        # Create a span and inject its context into headers
        with tracer.start_as_current_span("producer-span") as span:
            headers = inject_trace_context(None)
            expected_trace_id = span.get_span_context().trace_id

        # Extract the context from headers and verify trace ID is preserved
        extracted_ctx = extract_trace_context(headers)
        token = context.attach(extracted_ctx)
        try:
            extracted_span = trace.get_current_span()
            assert (
                extracted_span.get_span_context().trace_id == expected_trace_id
            )
        finally:
            context.detach(token)
