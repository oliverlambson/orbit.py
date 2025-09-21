import logging
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from nats import connect as nats_connect
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.errors import Error as JSError
from testcontainers.nats import NatsContainer

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def nats_container() -> Iterator[NatsContainer]:
    with NatsContainer(command="-js") as nats_container:  # "-js enables jetstream"
        yield nats_container


@pytest_asyncio.fixture
async def nats_client(nats_container: NatsContainer) -> AsyncIterator[NATS]:
    """Fixture providing a connected NATS client"""
    nc = await nats_connect(nats_container.nats_uri(), connect_timeout=10.0)
    yield nc
    await nc.close()


@pytest_asyncio.fixture
async def js_client(nats_client: NATS) -> AsyncIterator[JetStreamContext]:
    """Fixture providing a JetStream context"""
    js = nats_client.jetstream()
    yield js

    # Clean up streams after each test
    try:
        streams = await js.streams_info()
        for stream_info in streams:
            if stream_info.config.name:
                try:
                    _ = await js.delete_stream(stream_info.config.name)
                except JSError as e:
                    logger.warning(
                        "Failed to delete stream %s: %s", stream_info.config.name, e
                    )
    except JSError as e:
        logger.warning("Failed to list streams during cleanup: %s", e)
