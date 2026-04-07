"""Wrappers for NATS JetStream operations.

JetStreamContext.publish() delegates to Client.request() internally,
so we wrap it to create PRODUCER spans (rather than CLIENT spans).

JetStreamContext.subscribe() delegates to Client.subscribe() internally,
so the core subscribe wrapper already handles callback wrapping. We wrap
the JetStream subscribe to add JetStream-specific span attributes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from opentelemetry import trace
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.trace import SpanKind

from otel_instrumentation_nats.context_propagation import inject_trace_context
from otel_instrumentation_nats.utils import (
    extract_server_info,
    get_span_name,
    set_common_span_attributes,
    suppress_nats_instrumentation,
)


def wrap_jetstream_publish(tracer: trace.Tracer) -> Callable:
    """Return a wrapper function for JetStreamContext.publish().

    JetStream publish uses Client.request() under the hood, but we want
    to create a PRODUCER span (not CLIENT), and add JetStream-specific
    attributes like the stream name.

    Args:
        tracer: The OpenTelemetry tracer to use for creating spans.

    Returns:
        A wrapt-compatible wrapper function.
    """

    async def _traced_js_publish(
        wrapped: Callable,
        instance: Any,
        args: tuple,
        kwargs: dict,
    ) -> Any:
        if not is_instrumentation_enabled():
            return await wrapped(*args, **kwargs)

        # JetStreamContext.publish(
        #     subject, payload=b"", timeout=None, stream=None, headers=None
        # )
        subject = args[0] if args else kwargs.get("subject", "unknown")
        stream = kwargs.get("stream")

        span_name = get_span_name(subject, "send")

        # Get server info from the underlying NATS client
        nc = getattr(instance, "_nc", None)
        server_address, server_port = (
            extract_server_info(nc) if nc else (None, None)
        )

        with tracer.start_as_current_span(
            span_name,
            kind=SpanKind.PRODUCER,
        ) as span:
            set_common_span_attributes(
                span, subject, "publish", server_address, server_port
            )

            if span.is_recording():
                if stream:
                    span.set_attribute("messaging.destination.stream", stream)
                span.set_attribute("messaging.destination.kind", "stream")

            # Inject trace context into headers
            headers = kwargs.get("headers")
            headers = inject_trace_context(headers)
            kwargs["headers"] = headers

            # Suppress inner instrumentation (JetStream publish calls
            # Client.request() internally, which would create duplicate spans)
            with suppress_nats_instrumentation():
                return await wrapped(*args, **kwargs)

    return _traced_js_publish
