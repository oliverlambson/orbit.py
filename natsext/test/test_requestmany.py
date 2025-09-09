# Copyright 2025 Oliver Lambson
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import time

import nats
import pytest
import pytest_asyncio
from nats import NATS
from nats.aio.msg import Msg
from nats.errors import NoRespondersError
from testcontainers.nats import NatsContainer

from natsext import default_sentinel, request_many, request_many_msg


@pytest_asyncio.fixture
async def nats_client(nats_container: NatsContainer):
    """Fixture providing a connected NATS client"""
    nc = await nats.connect(nats_container.nats_uri(), connect_timeout=0.4)
    yield nc
    await nc.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "default, context timeout",
            "subject": "foo",
            "stall": None,
            "max_messages": None,
            "sentinel": None,
            "timeout": None,
            "min_time": 0.4,
            "expected_msgs": 6,
        },
        {
            "name": "with stall, short circuit",
            "subject": "foo",
            "stall": 0.05,
            "max_messages": None,
            "sentinel": None,
            "timeout": None,
            "min_time": 0.05,
            "expected_msgs": 5,
        },
        {
            "name": "with custom timeout",
            "subject": "foo",
            "stall": None,
            "max_messages": None,
            "sentinel": None,
            "timeout": 0.2,  # Shorter timeout
            "min_time": 0.2,
            "expected_msgs": 6,  # Gets 5 hello messages + 1 sentinel message (empty)
        },
        {
            "name": "with count reached",
            "subject": "foo",
            "stall": None,
            "max_messages": 3,
            "sentinel": None,
            "timeout": None,
            "min_time": 0,
            "expected_msgs": 3,
        },
        {
            "name": "with custom timeout and limit",
            "subject": "foo",
            "stall": None,
            "max_messages": 3,
            "sentinel": None,
            "timeout": 0.5,
            "min_time": 0,
            "expected_msgs": 3,
        },
        {
            "name": "with count timeout",
            "subject": "foo",
            "stall": None,
            "max_messages": 10,
            "sentinel": None,
            "timeout": None,
            "min_time": 0.4,
            "expected_msgs": 6,
        },
        {
            "name": "sentinel",
            "subject": "foo",
            "stall": None,
            "max_messages": None,
            "sentinel": default_sentinel,
            "timeout": None,
            "min_time": 0.1,
            "expected_msgs": 5,
        },
        {
            "name": "all options provided, stall timer short circuit",
            "subject": "foo",
            "stall": 0.05,
            "max_messages": 10,
            "sentinel": default_sentinel,
            "timeout": 0.5,
            "min_time": 0.05,
            "expected_msgs": 5,
        },
        {
            "name": "all options provided, msg count short circuit",
            "subject": "foo",
            "stall": 0.05,
            "max_messages": 3,
            "sentinel": default_sentinel,
            "timeout": 0.5,
            "min_time": 0,
            "expected_msgs": 3,
        },
        {
            "name": "all options provided, timeout short circuit",
            "subject": "foo",
            "stall": 0.1,
            "max_messages": 10,
            "sentinel": default_sentinel,
            "timeout": 0.05,
            "min_time": 0,
            "expected_msgs": 5,  # Actually gets 5 messages before timeout
        },
        {
            "name": "all options provided, sentinel short circuit",
            "subject": "foo",
            "stall": 0.15,
            "max_messages": 10,
            "sentinel": default_sentinel,
            "timeout": 0.5,
            "min_time": 0.1,
            "expected_msgs": 5,
        },
    ],
)
async def test_request_many(nats_client: NATS, test_case):
    """Test request_many with various option combinations"""
    nc = nats_client

    # Set up responders - 5 that send "hello" responses
    subs = []
    for _i in range(5):

        async def responder(msg: Msg):
            await asyncio.sleep(0.01)  # 10ms delay
            await nc.publish(msg.reply, b"hello")

        sub = await nc.subscribe("foo", cb=responder)
        subs.append(sub)

    # Set up sentinel responder that sends empty response after delay
    async def sentinel_responder(msg: Msg):
        await asyncio.sleep(0.1)  # 100ms delay
        await nc.publish(msg.reply, b"")

    sentinel_sub = await nc.subscribe("foo", cb=sentinel_responder)
    subs.append(sentinel_sub)

    try:
        start_time = time.time()

        msg_count = 0
        async for _msg in request_many(
            nc,
            test_case["subject"],
            b"",
            stall=test_case["stall"],
            max_messages=test_case["max_messages"],
            sentinel=test_case["sentinel"],
            timeout=test_case["timeout"],
        ):
            msg_count += 1

        elapsed = time.time() - start_time

        assert msg_count == test_case["expected_msgs"], (
            f"Expected {test_case['expected_msgs']} messages, got {msg_count}"
        )

        # Check timing (with some tolerance)
        min_time = test_case["min_time"]
        max_time = min_time + 0.15  # 150ms tolerance (increased for CI stability)
        if min_time > 0:
            assert elapsed >= min_time, f"Expected at least {min_time}s, got {elapsed}s"
        if min_time < 0.4:  # Don't check max time for default timeout cases
            assert elapsed <= max_time, f"Expected at most {max_time}s, got {elapsed}s"

    finally:
        # Clean up subscriptions
        for sub in subs:
            await sub.unsubscribe()


@pytest.mark.asyncio
async def test_request_many_no_responders(nats_client: NATS):
    """Test request_many with no responders"""
    nc = nats_client

    # Should raise NoRespondersError when there are no responders
    with pytest.raises(NoRespondersError):
        async for _msg in request_many(
            nc, "no.responders.test.subject", b"", timeout=0.1
        ):
            pass  # Should not reach this point


@pytest.mark.asyncio
async def test_request_many_no_responders_with_timeout(nats_client: NATS):
    """Test request_many with no responders and shorter timeout"""
    nc = nats_client

    # Should raise NoRespondersError when there are no responders
    with pytest.raises(NoRespondersError):
        async for _msg in request_many(
            nc, "no.responders.timeout.test", b"", timeout=0.1
        ):
            pass  # Should not reach this point


@pytest.mark.asyncio
async def test_request_many_invalid_options(nats_client: NATS):
    """Test request_many with invalid options"""
    nc = nats_client

    # Test invalid stall time - validation happens immediately
    with pytest.raises(ValueError, match="stall time has to be greater than 0"):
        request_many(nc, "test", b"", stall=-1)

    # Test invalid max messages - validation happens immediately
    with pytest.raises(
        ValueError, match="expected request count has to be greater than 0"
    ):
        request_many(nc, "test", b"", max_messages=-1)


@pytest.mark.asyncio
async def test_request_many_cancel(nats_client: NATS):
    """Test cancellation of request_many during iteration"""
    nc = nats_client

    # Set up a responder
    async def responder(msg: Msg):
        await nc.publish(msg.reply, b"hello")

    sub = await nc.subscribe("foo", cb=responder)

    try:
        msg_count = 0

        async def cancel_after_delay():
            await asyncio.sleep(0.1)
            task.cancel()

        # Start the request_many iteration
        async def iterate():
            nonlocal msg_count
            async for _msg in request_many(nc, "foo", b""):
                msg_count += 1

        task = asyncio.create_task(iterate())
        cancel_task = asyncio.create_task(cancel_after_delay())

        with pytest.raises(asyncio.CancelledError):
            await task

        await cancel_task

        # Should have received at least 1 message before cancellation
        assert msg_count >= 1

    finally:
        await sub.unsubscribe()


@pytest.mark.asyncio
async def test_request_many_cancel_context(nats_client: NATS):
    """Test cancellation of request_many using asyncio timeout (equivalent to Go context cancellation)"""
    nc = nats_client

    # Set up a responder
    async def responder(msg: Msg):
        await nc.publish(msg.reply, b"hello")

    sub = await nc.subscribe("foo", cb=responder)

    try:
        msg_count = 0

        async for _msg in request_many(
            nc, "foo", b"", timeout=0.1
        ):  # Use timeout parameter
            msg_count += 1

        # Should have received at least 1 message before timeout
        assert msg_count >= 1

    finally:
        await sub.unsubscribe()


@pytest.mark.asyncio
async def test_request_many_sentinel_custom(nats_client: NATS):
    """Test request_many with custom sentinel function"""
    nc = nats_client

    # Set up responder that sends multiple messages
    async def responder(msg: Msg):
        await nc.publish(msg.reply, b"hello")
        await asyncio.sleep(0.01)  # 10ms delay
        await nc.publish(msg.reply, b"world")
        await asyncio.sleep(0.01)  # 10ms delay
        await nc.publish(msg.reply, b"goodbye")

    sub = await nc.subscribe("foo", cb=responder)

    try:
        expected_msgs = ["hello", "world"]

        # Custom sentinel that stops at "goodbye"
        def goodbye_sentinel(msg: Msg) -> bool:
            return msg.data == b"goodbye"

        received_msgs = [
            msg.data.decode()
            async for msg in request_many(nc, "foo", b"", sentinel=goodbye_sentinel)
        ]

        # Verify we got exactly the expected messages
        assert len(received_msgs) == len(expected_msgs), (
            f"Expected {len(expected_msgs)} messages, got {len(received_msgs)}"
        )

        for i, msg in enumerate(received_msgs):
            assert msg == expected_msgs[i], (
                f"Expected '{expected_msgs[i]}', got '{msg}'"
            )

    finally:
        await sub.unsubscribe()


@pytest.mark.asyncio
async def test_request_many_msg(nats_client: NATS):
    """Test request_many_msg with a pre-constructed message"""
    nc = nats_client

    # Set up responder
    async def responder(msg: Msg):
        await nc.publish(msg.reply, b"response")

    sub = await nc.subscribe("test", cb=responder)

    try:
        # Create a message with headers
        request_msg = Msg(
            _client=nc,
            subject="test",
            data=b"request_data",
            headers={"Custom-Header": "test-value"},
        )

        msg_count = 0
        async for msg in request_many_msg(nc, request_msg, max_messages=1):
            assert msg.data == b"response"
            msg_count += 1

        assert msg_count == 1

    finally:
        await sub.unsubscribe()
