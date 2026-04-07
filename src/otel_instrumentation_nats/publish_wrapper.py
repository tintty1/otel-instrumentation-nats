"""Wrapper for NATS Client.publish() to create PRODUCER spans."""

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
    is_nats_instrumentation_suppressed,
    set_common_span_attributes,
)

_OPERATION = "publish"


def wrap_publish(tracer: trace.Tracer) -> Callable:
    """Return a wrapper function for Client.publish().

    Args:
        tracer: The OpenTelemetry tracer to use for creating spans.

    Returns:
        A wrapt-compatible wrapper function.
    """

    async def _traced_publish(
        wrapped: Callable,
        instance: Any,
        args: tuple,
        kwargs: dict,
    ) -> None:
        if (
            not is_instrumentation_enabled()
            or is_nats_instrumentation_suppressed()
        ):
            return await wrapped(*args, **kwargs)

        # publish(subject, payload=b"", reply="", headers=None)
        subject = args[0] if args else kwargs.get("subject", "unknown")

        span_name = get_span_name(subject, "send")
        server_address, server_port = extract_server_info(instance)

        with tracer.start_as_current_span(
            span_name,
            kind=SpanKind.PRODUCER,
        ) as span:
            set_common_span_attributes(
                span, subject, _OPERATION, server_address, server_port
            )

            # Inject trace context into headers
            headers = kwargs.get("headers")
            if len(args) > 3:
                # headers passed as positional arg (4th arg)
                headers = args[3]
                headers = inject_trace_context(headers)
                args = (*args[:3], headers, *args[4:])
            else:
                headers = inject_trace_context(headers)
                kwargs["headers"] = headers

            return await wrapped(*args, **kwargs)

    return _traced_publish
