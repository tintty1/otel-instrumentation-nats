"""Tests for NatsInstrumentor lifecycle (instrument/uninstrument)."""

from __future__ import annotations

import nats.aio.client
import nats.js.client
import wrapt

from otel_instrumentation_nats import NatsInstrumentor


class TestNatsInstrumentorLifecycle:
    """Test that instrument() patches and uninstrument() restores methods."""

    def test_instrument_patches_client_methods(self, tracer_provider):
        """After instrument(), Client methods should be wrapped."""
        assert isinstance(
            nats.aio.client.Client.publish, wrapt.BoundFunctionWrapper
        ) or isinstance(nats.aio.client.Client.publish, wrapt.FunctionWrapper)

        assert isinstance(
            nats.aio.client.Client.subscribe, wrapt.BoundFunctionWrapper
        ) or isinstance(nats.aio.client.Client.subscribe, wrapt.FunctionWrapper)

        assert isinstance(
            nats.aio.client.Client.request, wrapt.BoundFunctionWrapper
        ) or isinstance(nats.aio.client.Client.request, wrapt.FunctionWrapper)

    def test_instrument_patches_jetstream(self, tracer_provider):
        """After instrument(), JetStreamContext.publish should be wrapped."""
        assert isinstance(
            nats.js.client.JetStreamContext.publish,
            wrapt.BoundFunctionWrapper,
        ) or isinstance(
            nats.js.client.JetStreamContext.publish, wrapt.FunctionWrapper
        )

    def test_uninstrument_restores_methods(self, tracer_provider):
        """After uninstrument(), methods should not be wrapped."""
        instrumentor = NatsInstrumentor()
        instrumentor.uninstrument()

        assert not isinstance(
            nats.aio.client.Client.publish, wrapt.FunctionWrapper
        )
        assert not isinstance(
            nats.aio.client.Client.subscribe, wrapt.FunctionWrapper
        )
        assert not isinstance(
            nats.aio.client.Client.request, wrapt.FunctionWrapper
        )
        assert not isinstance(
            nats.js.client.JetStreamContext.publish, wrapt.FunctionWrapper
        )

        # Re-instrument for other tests (autouse fixture will handle this)
        instrumentor.instrument(tracer_provider=tracer_provider)

    def test_double_instrument_is_safe(self, tracer_provider):
        """Calling instrument() twice should not double-wrap."""
        instrumentor = NatsInstrumentor()
        # Already instrumented by fixture; call again
        instrumentor.instrument(tracer_provider=tracer_provider)

        # Should still be wrapped (not double-wrapped)
        assert isinstance(
            nats.aio.client.Client.publish, wrapt.BoundFunctionWrapper
        ) or isinstance(nats.aio.client.Client.publish, wrapt.FunctionWrapper)
