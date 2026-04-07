"""Wrapper for NATS Client.subscribe() to create CONSUMER spans
on message delivery."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from opentelemetry import context, trace
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.trace import SpanKind

from otel_instrumentation_nats.context_propagation import extract_trace_context
from otel_instrumentation_nats.utils import (
    extract_server_info,
    get_span_name,
    set_common_span_attributes,
)

_OPERATION = "receive"


def _wrap_callback(
    tracer: trace.Tracer,
    callback: Callable,
    server_address: str | None = None,
    server_port: int | None = None,
) -> Callable:
    """Wrap a subscription callback to create a CONSUMER span for each message.

    The wrapped callback:
    1. Extracts trace context from the message headers
    2. Creates a CONSUMER span linked to the producer via propagated context
    3. Calls the original callback within the span context
    """

    @functools.wraps(callback)
    async def _traced_callback(msg: Any) -> None:
        if not is_instrumentation_enabled():
            return await callback(msg)

        subject = getattr(msg, "subject", "unknown")

        # Extract propagated context from message headers
        headers = getattr(msg, "headers", None)
        extracted_ctx = extract_trace_context(headers)
        token = context.attach(extracted_ctx)

        try:
            span_name = get_span_name(subject, "receive")

            with tracer.start_as_current_span(
                span_name,
                kind=SpanKind.CONSUMER,
            ) as span:
                set_common_span_attributes(
                    span, subject, _OPERATION, server_address, server_port
                )

                return await callback(msg)
        finally:
            context.detach(token)

    return _traced_callback


def wrap_subscribe(tracer: trace.Tracer) -> Callable:
    """Return a wrapper function for Client.subscribe().

    This wraps the subscribe method to intercept the user callback (the `cb`
    parameter) and decorate it with tracing. If no callback is provided
    (async iterator pattern), we wrap the Subscription's next_msg method
    instead.

    Args:
        tracer: The OpenTelemetry tracer to use for creating spans.

    Returns:
        A wrapt-compatible wrapper function.
    """

    async def _traced_subscribe(
        wrapped: Callable,
        instance: Any,
        args: tuple,
        kwargs: dict,
    ) -> Any:
        if not is_instrumentation_enabled():
            return await wrapped(*args, **kwargs)

        server_address, server_port = extract_server_info(instance)

        # Intercept the cb kwarg and wrap it
        cb = kwargs.get("cb")
        if cb is not None:
            kwargs["cb"] = _wrap_callback(
                tracer, cb, server_address, server_port
            )
        else:
            # Check positional args: subscribe(subject, queue="", cb=None, ...)
            # cb is the 3rd positional argument (index 2)
            if len(args) > 2 and args[2] is not None:
                cb = args[2]
                wrapped_cb = _wrap_callback(
                    tracer, cb, server_address, server_port
                )
                args = (*args[:2], wrapped_cb, *args[3:])

        sub = await wrapped(*args, **kwargs)

        # If no callback was provided, wrap the subscription's next_msg
        # to trace the async iterator / next_msg() pull pattern
        if cb is None:
            _wrap_subscription_next_msg(
                sub, tracer, server_address, server_port
            )

        return sub

    return _traced_subscribe


def _wrap_subscription_next_msg(
    sub: Any,
    tracer: trace.Tracer,
    server_address: str | None = None,
    server_port: int | None = None,
) -> None:
    """Wrap a Subscription's next_msg method to create CONSUMER spans.

    This handles the pull/iterator pattern where no callback is provided
    to subscribe(). Each call to next_msg() will create a CONSUMER span.
    """
    original_next_msg = sub.next_msg

    async def _traced_next_msg(timeout: float | None = 1.0) -> Any:
        msg = await original_next_msg(timeout=timeout)

        if msg is None:
            return msg

        subject = getattr(msg, "subject", "unknown")
        headers = getattr(msg, "headers", None)
        extracted_ctx = extract_trace_context(headers)
        token = context.attach(extracted_ctx)

        try:
            span_name = get_span_name(subject, "receive")
            with tracer.start_as_current_span(
                span_name,
                kind=SpanKind.CONSUMER,
            ) as span:
                set_common_span_attributes(
                    span, subject, _OPERATION, server_address, server_port
                )
        finally:
            context.detach(token)

        return msg

    sub.next_msg = _traced_next_msg
