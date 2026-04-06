"""Context propagation utilities for NATS message headers.

nats-py uses Dict[str, str] for message headers, which is compatible
with the OpenTelemetry TextMap propagation API.
"""

from __future__ import annotations

from typing import Iterable, Optional

from opentelemetry.context.context import Context
from opentelemetry.propagators import textmap


class NatsHeadersSetter(textmap.Setter[dict[str, str]]):
    """Injects trace context into NATS message headers (Dict[str, str])."""

    def set(self, carrier: dict[str, str], key: str, value: str) -> None:
        carrier[key] = value


class NatsHeadersGetter(textmap.Getter[dict[str, str]]):
    """Extracts trace context from NATS message headers (Dict[str, str])."""

    def get(self, carrier: dict[str, str], key: str) -> Optional[list[str]]:
        value = carrier.get(key)
        if value is None:
            return None
        return [value]

    def keys(self, carrier: dict[str, str]) -> list[str]:
        return list(carrier.keys())


nats_setter = NatsHeadersSetter()
nats_getter = NatsHeadersGetter()


def inject_trace_context(
    headers: dict[str, str] | None,
    context: Context | None = None,
) -> dict[str, str]:
    """Inject current trace context into NATS headers.

    If headers is None, creates a new dict. Returns the headers dict
    (mutated in-place or newly created).
    """
    from opentelemetry import propagate

    if headers is None:
        headers = {}
    propagate.inject(headers, setter=nats_setter, context=context)
    return headers


def extract_trace_context(
    headers: dict[str, str] | None,
) -> Context:
    """Extract trace context from NATS message headers.

    Returns the extracted Context, or the current context if headers
    is None or empty.
    """
    from opentelemetry import propagate

    if not headers:
        return propagate.extract({})
    return propagate.extract(headers, getter=nats_getter)
