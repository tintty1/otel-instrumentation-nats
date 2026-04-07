"""NatsInstrumentor: OpenTelemetry auto-instrumentation for nats-py.

Patches the nats-py Client and JetStreamContext to automatically create
OpenTelemetry spans for publish, subscribe, request, and JetStream operations.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import Any

import wrapt
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap

from otel_instrumentation_nats.jetstream_wrapper import wrap_jetstream_publish
from otel_instrumentation_nats.package import _instruments
from otel_instrumentation_nats.publish_wrapper import wrap_publish
from otel_instrumentation_nats.request_wrapper import wrap_request
from otel_instrumentation_nats.subscribe_wrapper import wrap_subscribe
from otel_instrumentation_nats.version import __version__

logger = logging.getLogger(__name__)

_SCHEMA_URL = "https://opentelemetry.io/schemas/1.11.0"


class NatsInstrumentor(BaseInstrumentor):
    """OpenTelemetry instrumentor for nats-py.

    Instruments the following operations:
    - Client.publish() -> PRODUCER spans with trace context injection
    - Client.subscribe() -> CONSUMER spans with trace context extraction
    - Client.request() -> CLIENT spans with trace context injection
    - JetStreamContext.publish() -> PRODUCER spans with JetStream attributes

    Usage::

        from otel_instrumentation_nats import NatsInstrumentor

        NatsInstrumentor().instrument()

        # All nats-py operations will now be traced
        nc = await nats.connect()
        await nc.publish("subject", b"data")
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=_SCHEMA_URL,
        )

        # Import nats modules to patch
        import nats.aio.client

        # Wrap core Client methods
        wrapt.wrap_function_wrapper(
            nats.aio.client, "Client.publish", wrap_publish(tracer)
        )
        wrapt.wrap_function_wrapper(
            nats.aio.client, "Client.subscribe", wrap_subscribe(tracer)
        )
        wrapt.wrap_function_wrapper(
            nats.aio.client, "Client.request", wrap_request(tracer)
        )

        # Wrap JetStream methods
        try:
            import nats.js.client

            wrapt.wrap_function_wrapper(
                nats.js.client,
                "JetStreamContext.publish",
                wrap_jetstream_publish(tracer),
            )
        except (ImportError, AttributeError) as exc:
            logger.debug("Could not instrument JetStream: %s", exc)

    def _uninstrument(self, **kwargs: Any) -> None:
        import nats.aio.client

        unwrap(nats.aio.client.Client, "publish")
        unwrap(nats.aio.client.Client, "subscribe")
        unwrap(nats.aio.client.Client, "request")

        try:
            import nats.js.client

            unwrap(nats.js.client.JetStreamContext, "publish")
        except (ImportError, AttributeError):
            pass
