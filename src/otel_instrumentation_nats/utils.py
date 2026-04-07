"""Shared utilities for NATS instrumentation."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from opentelemetry import trace
from opentelemetry.semconv.attributes import server_attributes
from opentelemetry.semconv.trace import SpanAttributes

_MESSAGING_SYSTEM = "nats"

# Context variable to suppress nested instrumentation.
# When a higher-level wrapper (e.g., JetStream publish) is active, it sets
# this to True so that the lower-level wrappers (e.g., Client.request,
# Client.publish) called internally do not create duplicate spans.
_suppress_nats_instrumentation: ContextVar[bool] = ContextVar(
    "_suppress_nats_instrumentation", default=False
)


def is_nats_instrumentation_suppressed() -> bool:
    """Check if NATS instrumentation should be suppressed."""
    return _suppress_nats_instrumentation.get()


class suppress_nats_instrumentation:
    """Context manager to suppress nested NATS instrumentation."""

    def __enter__(self):
        self._token = _suppress_nats_instrumentation.set(True)
        return self

    def __exit__(self, *args):
        _suppress_nats_instrumentation.reset(self._token)


def get_span_name(subject: str, operation: str) -> str:
    """Build a span name following OTel messaging conventions:
    '{destination} {operation}'."""
    return f"{subject} {operation}"


def set_common_span_attributes(
    span: trace.Span,
    subject: str,
    operation: str,
    server_address: str | None = None,
    server_port: int | None = None,
) -> None:
    """Set standard messaging semantic convention attributes on a span."""
    if not span.is_recording():
        return

    span.set_attribute(SpanAttributes.MESSAGING_SYSTEM, _MESSAGING_SYSTEM)
    span.set_attribute(SpanAttributes.MESSAGING_DESTINATION_NAME, subject)
    span.set_attribute(SpanAttributes.MESSAGING_OPERATION, operation)

    if server_address:
        span.set_attribute(server_attributes.SERVER_ADDRESS, server_address)
    if server_port:
        span.set_attribute(server_attributes.SERVER_PORT, server_port)


def extract_server_info(client: Any) -> tuple[str | None, int | None]:
    """Extract server address and port from a NATS client instance.

    The nats-py Client stores the connected server URL in
    client._current_server, which has a .uri attribute.
    """
    try:
        current_server = getattr(client, "_current_server", None)
        if current_server is None:
            return None, None
        uri = getattr(current_server, "uri", None)
        if uri is None:
            return None, None
        # uri is a urllib.parse.ParseResult
        host = uri.hostname
        port = uri.port
        return host, port
    except Exception:
        return None, None
