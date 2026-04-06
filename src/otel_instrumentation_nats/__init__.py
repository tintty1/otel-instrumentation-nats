"""OpenTelemetry auto-instrumentation for nats-py.

Usage::

    from otel_instrumentation_nats import NatsInstrumentor

    NatsInstrumentor().instrument()

    # All nats-py operations will now be traced automatically
    import nats
    nc = await nats.connect()
    await nc.publish("subject", b"hello")
"""

from otel_instrumentation_nats.instrumentor import NatsInstrumentor
from otel_instrumentation_nats.version import __version__

__all__ = ["NatsInstrumentor", "__version__"]
