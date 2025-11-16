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

"""Integration tests for jetstreampcg.

These tests are ported from orbit.go/pcgroups/test/stream_consumer_group_test.go
"""

import asyncio

import pytest
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, StreamConfig, SubjectTransform

from jetstreampcg.elastic import (
    add_members,
    create_elastic,
    delete_elastic,
    delete_members,
)
from jetstreampcg.static import create_static, delete_static, static_consume


@pytest.mark.asyncio
class TestStaticIntegration:
    """Integration tests for static consumer groups.

    Ported from orbit.go/pcgroups/test/stream_consumer_group_test.go:TestStatic
    """

    async def test_static_consumer_group(self, js_client: JetStreamContext):
        """Test static consumer group with two members consuming messages in parallel."""
        stream_name = "test-static"
        cg_name = "group"
        c1_count = 0
        c2_count = 0

        # Create a stream with subject transform for partitioning
        await js_client.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=["bar.*"],
                subject_transform=SubjectTransform(
                    src="bar.*",
                    dest="{{partition(2,1)}}.bar.{{wildcard(1)}}",
                ),
            )
        )

        # Publish 10 messages
        for i in range(10):
            await js_client.publish(f"bar.{i}", b"payload")

        # Consumer config
        consumer_config = ConsumerConfig(
            max_ack_pending=1,
            ack_wait=1.0,
            ack_policy=AckPolicy.EXPLICIT,
        )

        # Create static consumer group with 2 members
        await create_static(
            js_client,
            stream_name,
            cg_name,
            max_members=2,
            filter="bar.*",
            members=["m1", "m2"],
            member_mappings=[],
        )

        # Track when to stop consuming
        stop_event = asyncio.Event()

        # Consumer 1
        async def consume_m1():
            nonlocal c1_count

            async def m1_handler(msg):
                nonlocal c1_count
                c1_count += 1
                await msg.ack()

            ctx = await static_consume(
                js_client,
                stream_name,
                cg_name,
                "m1",
                m1_handler,
                consumer_config,
            )

            # Wait for stop signal
            await stop_event.wait()
            ctx.stop()
            await ctx.done()

        # Consumer 2
        async def consume_m2():
            nonlocal c2_count

            async def m2_handler(msg):
                nonlocal c2_count
                c2_count += 1
                await msg.ack()

            ctx = await static_consume(
                js_client,
                stream_name,
                cg_name,
                "m2",
                m2_handler,
                consumer_config,
            )

            # Wait for stop signal
            await stop_event.wait()
            ctx.stop()
            await ctx.done()

        # Start both consumers
        task1 = asyncio.create_task(consume_m1())
        task2 = asyncio.create_task(consume_m2())

        # Wait for all messages to be consumed (with timeout)
        start_time = asyncio.get_event_loop().time()
        while c1_count + c2_count < 10:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start_time > 5:
                pytest.fail("Timeout waiting for messages to be consumed")

        # Signal consumers to stop
        stop_event.set()

        # Wait for consumers to finish
        await asyncio.gather(task1, task2)

        # Verify all messages were consumed
        assert c1_count + c2_count == 10

        # Clean up
        await delete_static(js_client, stream_name, cg_name)


@pytest.mark.asyncio
class TestElasticIntegration:
    """Integration tests for elastic consumer groups.

    Ported from orbit.go/pcgroups/test/stream_consumer_group_test.go:TestElastic
    """

    async def test_elastic_consumer_group_with_membership_changes(
        self, js_client: JetStreamContext
    ):
        """Test elastic consumer group with dynamic member addition and removal."""
        stream_name = "test-elastic"
        cg_name = "group"
        c1_count = 0
        c2_count = 0

        # Create a stream
        await js_client.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=["bar.*"],
            )
        )

        # Publish 10 messages
        for i in range(10):
            await js_client.publish(f"bar.{i}", b"payload")

        # Consumer config
        consumer_config = ConsumerConfig(
            max_ack_pending=1,
            ack_wait=1.0,
            ack_policy=AckPolicy.EXPLICIT,
        )

        # Create elastic consumer group with max 2 members
        await create_elastic(
            js_client,
            stream_name,
            cg_name,
            max_num_members=2,
            filter="bar.*",
            partitioning_wildcards=[1],
        )

        # Track when to stop consuming
        stop_event_m1 = asyncio.Event()
        stop_event_m2 = asyncio.Event()

        # Consumer 1
        async def consume_m1():
            nonlocal c1_count

            async def m1_handler(msg):
                nonlocal c1_count
                c1_count += 1
                await msg.ack()

            from jetstreampcg.elastic import elastic_consume

            ctx = await elastic_consume(
                js_client,
                stream_name,
                cg_name,
                "m1",
                m1_handler,
                consumer_config,
            )

            # Wait for stop signal
            await stop_event_m1.wait()
            ctx.stop()
            await ctx.done()

        # Consumer 2
        async def consume_m2():
            nonlocal c2_count

            async def m2_handler(msg):
                nonlocal c2_count
                c2_count += 1
                await msg.ack()

            from jetstreampcg.elastic import elastic_consume

            ctx = await elastic_consume(
                js_client,
                stream_name,
                cg_name,
                "m2",
                m2_handler,
                consumer_config,
            )

            # Wait for stop signal
            await stop_event_m2.wait()
            ctx.stop()
            await ctx.done()

        # Start both consumers
        task1 = asyncio.create_task(consume_m1())
        task2 = asyncio.create_task(consume_m2())

        # Add only m1 to membership
        await add_members(js_client, stream_name, cg_name, ["m1"])

        # Wait for m1 to consume all 10 messages (m2 should not consume any)
        start_time = asyncio.get_event_loop().time()
        while c1_count != 10 or c2_count != 0:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start_time > 5:
                pytest.fail(
                    f"Timeout: expected c1=10, c2=0, got c1={c1_count}, c2={c2_count}"
                )

        assert c1_count == 10
        assert c2_count == 0

        # Add m2 to membership
        await add_members(js_client, stream_name, cg_name, ["m2"])

        # Wait a bit for m2 to be effectively added
        await asyncio.sleep(0.05)

        # Publish 10 more messages
        for i in range(10):
            await js_client.publish(f"bar.{i}", b"payload")

        # Wait for messages to be split between m1 and m2
        start_time = asyncio.get_event_loop().time()
        while c1_count + c2_count < 20:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start_time > 10:
                pytest.fail(
                    f"Timeout: expected total=20, got c1={c1_count}, c2={c2_count}"
                )

        # Both should have consumed some messages (split between them)
        assert c1_count == 15
        assert c2_count == 5

        # Remove m1 from membership
        await delete_members(js_client, stream_name, cg_name, ["m1"])

        # Wait a bit for m1 to be effectively deleted
        await asyncio.sleep(0.05)

        # Publish 10 more messages
        for i in range(10):
            await js_client.publish(f"bar.{i}", b"payload")

        # Wait for m2 to consume all new messages (m1 should not consume any more)
        start_time = asyncio.get_event_loop().time()
        while c1_count != 15 or c2_count != 15:
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start_time > 10:
                pytest.fail(
                    f"Timeout: expected c1=15, c2=15, got c1={c1_count}, c2={c2_count}"
                )

        assert c1_count == 15
        assert c2_count == 15

        # Signal consumers to stop
        stop_event_m1.set()
        stop_event_m2.set()

        # Wait for consumers to finish
        await asyncio.gather(task1, task2)

        # Clean up
        await delete_elastic(js_client, stream_name, cg_name)
