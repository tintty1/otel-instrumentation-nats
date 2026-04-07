"""Shared test fixtures for NATS instrumentation tests."""

from __future__ import annotations

import os

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from otel_instrumentation_nats import NatsInstrumentor

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


@pytest.fixture
def memory_exporter():
    """Create an InMemorySpanExporter for capturing spans in tests."""
    return InMemorySpanExporter()


@pytest.fixture
def tracer_provider(memory_exporter):
    """Create a TracerProvider with InMemorySpanExporter."""
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    return provider


@pytest.fixture(autouse=True)
def instrument_nats(tracer_provider):
    """Instrument and uninstrument nats-py around each test."""
    instrumentor = NatsInstrumentor()
    if instrumentor.is_instrumented_by_opentelemetry:
        instrumentor.uninstrument()
    instrumentor.instrument(tracer_provider=tracer_provider)
    yield
    instrumentor.uninstrument()


@pytest.fixture
def spans(memory_exporter):
    """Helper to retrieve finished spans from the exporter."""
    return memory_exporter.get_finished_spans
