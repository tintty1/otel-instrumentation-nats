"""Wrapper for NATS Client.request() to create CLIENT spans."""

from __future__ import annotations

from typing import Any, Callable

from opentelemetry import trace
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.trace import SpanKind

from otel_instrumentation_nats.context_propagation import inject_trace_context
from otel_instrumentation_nats.utils import (
    extract_server_info,
    get_span_name,
    is_nats_instrumentation_suppressed,
    set_common_span_attributes,
)

_OPERATION = "request"


def wrap_request(tracer: trace.Tracer) -> Callable:
    """Return a wrapper function for Client.request().

    Creates a CLIENT span for the request-reply pattern, since the caller
    sends a request and waits for a single response.

    Args:
        tracer: The OpenTelemetry tracer to use for creating spans.

    Returns:
        A wrapt-compatible wrapper function.
    """

    async def _traced_request(
        wrapped: Callable,
        instance: Any,
        args: tuple,
        kwargs: dict,
    ) -> Any:
        if not is_instrumentation_enabled() or is_nats_instrumentation_suppressed():
            return await wrapped(*args, **kwargs)

        # Extract arguments: request(subject, payload=b"", timeout=0.5, ...)
        subject = args[0] if args else kwargs.get("subject", "unknown")

        span_name = get_span_name(subject, "request")
        server_address, server_port = extract_server_info(instance)

        with tracer.start_as_current_span(
            span_name,
            kind=SpanKind.CLIENT,
        ) as span:
            set_common_span_attributes(
                span, subject, _OPERATION, server_address, server_port
            )

            # Inject trace context into headers
            headers = kwargs.get("headers")
            headers = inject_trace_context(headers)
            kwargs["headers"] = headers

            return await wrapped(*args, **kwargs)

    return _traced_request
