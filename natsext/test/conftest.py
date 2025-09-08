from collections.abc import Iterator

import pytest
from nats import connect as nats_connect
from testcontainers.nats import NatsContainer


@pytest.fixture(scope="session")
def nats_container() -> Iterator[NatsContainer]:
    with NatsContainer() as nats_container:
        yield nats_container


@pytest.mark.asyncio
async def test_nats_fixture(nats_container: NatsContainer):
    client = await nats_connect(nats_container.nats_uri())
    sub_tc = await client.subscribe("tc")
    await client.publish("tc", b"Test-Containers")
    next_message = await sub_tc.next_msg(timeout=5.0)
    await client.close()
    assert next_message.data == b"Test-Containers"
