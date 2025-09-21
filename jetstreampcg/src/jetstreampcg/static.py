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

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from nats.js.api import ConsumerConfig, KeyValueConfig, StorageType
from nats.js.errors import KeyNotFoundError, KeyValueError, NotFoundError
from typing_extensions import override

from .common import (
    ACK_WAIT,
    PULL_TIMEOUT,
    ConsumerGroupConsumeContext,
    ConsumerGroupMsg,
    MemberMapping,
    compose_key,
    generate_partition_filters,
)

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext
    from nats.js.api import ConsumerInfo, StreamInfo
    from nats.js.client import JetStreamContext as JSContext
    from nats.js.kv import KeyValue

logger = logging.getLogger(__name__)

# Constants
KV_STATIC_BUCKET_NAME = "static-consumer-groups"


class StaticConsumerGroupConsumerInstance(ConsumerGroupConsumeContext):
    """Instance for consuming messages from a static consumer group."""

    stream_name: str
    consumer_group_name: str
    member_name: str
    config: StaticConsumerGroupConfig
    message_handler: Callable[[Msg], Awaitable[None]]
    consumer_user_config: ConsumerConfig
    consumer_name: str | None
    current_pinned_id: str
    # consumer_consume_context
    js: JetStreamContext
    kv: KeyValue
    _cancel_event: asyncio.Event
    key_watcher: KeyValue.KeyWatcher | None
    _done_future: asyncio.Future[Exception | None]

    subscription: JSContext.PullSubscription | None
    _consume_task: asyncio.Task[None] | None
    _instance_task: asyncio.Task[None] | None

    def __init__(
        self,
        stream_name: str,
        consumer_group_name: str,
        member_name: str,
        config: StaticConsumerGroupConfig,
        message_handler: Callable[[Msg], Awaitable[None]],
        consumer_config: ConsumerConfig,
        js: JetStreamContext,
        kv: KeyValue,
    ):
        self.stream_name = stream_name
        self.consumer_group_name = consumer_group_name
        self.member_name = member_name
        self.config = config
        self.message_handler = message_handler
        self.consumer_user_config = consumer_config
        self.js = js
        self.kv = kv
        self._cancel_event = asyncio.Event()
        self.key_watcher = None
        self._done_future = asyncio.Future()

        self.consumer_name = None
        self.current_pinned_id = ""
        self.subscription = None
        self._consume_task = None
        self._instance_task = None

    @override
    def stop(self) -> None:
        """Stop the consumer group instance."""
        self._cancel_event.set()

    @override
    def done(self) -> asyncio.Future[Exception | None]:
        """Return a future that completes when consumption is done."""
        return self._done_future

    async def start(self) -> None:
        """Start the consumer group instance."""
        # Watch for config changes
        self.key_watcher = await self.kv.watch(  # pyright: ignore[reportUnknownMemberType]
            compose_key(self.stream_name, self.consumer_group_name)
        )

        # Join the consumer group if member is in membership
        if self.config.is_in_membership(self.member_name):
            await self._join_member_consumer_static()
        else:
            msg = (
                "the member name is not in the current static consumer group membership"
            )
            raise ValueError(msg)

        # Start the instance routine
        self._instance_task = asyncio.create_task(self._instance_routine())

    async def _instance_routine(self) -> None:
        """Control routine that watches for changes in the consumer group config."""
        try:
            # Create a task to watch for key updates
            async def watch_updates():
                if self.key_watcher:
                    async for update_msg in self.key_watcher:
                        yield update_msg

            update_iterator = watch_updates()

            while not self._cancel_event.is_set():
                try:
                    # Use wait_for with a timeout instead of complex task management
                    try:
                        update_msg = await asyncio.wait_for(
                            anext(update_iterator),
                            timeout=1.0,  # Check cancel event every second
                        )
                    except asyncio.TimeoutError:
                        continue  # Check cancel event and continue

                    # Check if config was deleted
                    if update_msg.operation == "DELETE":
                        await self._stop_and_delete_member_consumer()
                        self._done_future.set_result(None)
                        return

                    # Parse and validate new config
                    try:
                        new_config = StaticConsumerGroupConfig.from_dict(
                            json.loads(update_msg.value or b"")  # pyright: ignore[reportAny]
                        )
                        validate_static_config(new_config)
                    except (json.JSONDecodeError, ValueError) as e:
                        await self._stop_and_delete_member_consumer()
                        self._done_future.set_exception(e)
                        return

                    # Check if config actually changed
                    if (
                        new_config.max_members != self.config.max_members
                        or new_config.filter != self.config.filter
                        or new_config.members != self.config.members
                        or len(new_config.member_mappings)
                        != len(self.config.member_mappings)
                        or any(
                            nm.member != om.member or nm.partitions != om.partitions
                            for nm, om in zip(
                                new_config.member_mappings,
                                self.config.member_mappings,
                                strict=False,
                            )
                        )
                    ):
                        await self._stop_and_delete_member_consumer()
                        self._done_future.set_exception(
                            ValueError(
                                "static consumer group config watcher received a change in the configuration, terminating"
                            )
                        )
                        return

                except asyncio.CancelledError:
                    await self._stop_consuming()
                    self._done_future.set_result(None)
                    return
                except StopAsyncIteration:
                    # Key watcher ended
                    await self._stop_consuming()
                    self._done_future.set_result(None)
                    return

        except Exception as e:  # noqa: BLE001
            # Catch all exceptions to ensure cleanup happens
            await self._stop_and_delete_member_consumer()
            if not self._done_future.done():
                self._done_future.set_exception(e)

    async def _join_member_consumer_static(self) -> None:
        """Create the member's consumer and start consuming."""
        filters = generate_partition_filters(
            self.config.members,
            self.config.max_members,
            self.config.member_mappings,
            self.member_name,
        )

        if not filters:
            return

        # Configure consumer - use ConsumerConfig from nats-py

        # Build consumer config with proper parameters
        consumer_config = self.consumer_user_config
        consumer_config.durable_name = _compose_static_consumer_name(
            self.consumer_group_name, self.member_name
        )
        consumer_config.filter_subjects = filters
        consumer_config.ack_wait = self.consumer_user_config.ack_wait or ACK_WAIT

        # Create consumer
        _ = await self.js.add_consumer(self.stream_name, consumer_config)  # pyright: ignore[reportUnknownMemberType]

        # Store consumer name for later operations
        self.consumer_name = _compose_static_consumer_name(
            self.consumer_group_name, self.member_name
        )

        # Start consuming
        await self._start_consuming()

    async def _start_consuming(self) -> None:
        """Start actively consuming messages from the consumer."""
        if not self.consumer_name:
            return

        # Create pull subscription for the consumer
        # In nats-py, we use pull_subscribe with the consumer name
        self.subscription = await self.js.pull_subscribe(
            "", durable=self.consumer_name, stream=self.stream_name
        )

        # Start processing messages
        async def process_messages():
            while not self._cancel_event.is_set():
                try:
                    if self.subscription is None:
                        continue
                    msgs = await self.subscription.fetch(batch=1, timeout=PULL_TIMEOUT)
                    for msg in msgs:
                        await self._consumer_callback(msg)
                except asyncio.TimeoutError:
                    continue

        self._consume_task = asyncio.create_task(process_messages())

    async def _consumer_callback(self, msg: Msg) -> None:
        """Callback to process messages, checking pinned-id header."""
        # Check pinned-id header
        pid = msg.headers.get("Nats-Pin-Id") if msg.headers else None
        if not pid:
            logger.warning("Received a message without a pinned-id header")
        else:
            if not self.current_pinned_id:
                self.current_pinned_id = pid
            elif self.current_pinned_id != pid:
                # Pinned member changed
                self.current_pinned_id = pid

        # Wrap message and call user handler
        wrapped_msg = ConsumerGroupMsg.from_msg(msg)
        await self.message_handler(wrapped_msg)

    async def _stop_consuming(self) -> None:
        """Stop consuming messages from the consumer."""
        if hasattr(self, "_consume_task") and self._consume_task:
            _ = self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        if hasattr(self, "subscription") and self.subscription:
            await self.subscription.unsubscribe()
        self.consumer_name = None

    async def _stop_and_delete_member_consumer(self) -> None:
        """Stop consuming and delete the durable consumer."""
        await self._stop_consuming()

        try:
            _ = await self.js.delete_consumer(
                self.stream_name,
                _compose_static_consumer_name(
                    self.consumer_group_name, self.member_name
                ),
            )
        except NotFoundError:
            pass


@dataclass
class StaticConsumerGroupConfig:
    """Configuration for a static consumer group."""

    max_members: int
    filter: str = ""
    members: list[str] = field(default_factory=list)
    member_mappings: list[MemberMapping] = field(default_factory=list)

    def is_in_membership(self, name: str) -> bool:
        """Check if the member is in the current membership."""
        # Check member_mappings first
        for mapping in self.member_mappings:
            if mapping.member == name:
                return True
        # Then check members list
        return name in self.members

    def to_dict(self) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]
        """Convert to dictionary for JSON serialization."""
        serializable_dict: dict[str, Any] = {  # pyright: ignore[reportExplicitAny]
            "max_members": self.max_members,
            "filter": self.filter,
        }
        if self.members:
            serializable_dict["members"] = self.members
        if self.member_mappings:
            serializable_dict["member_mappings"] = [
                {"member": m.member, "partitions": m.partitions}
                for m in self.member_mappings
            ]
        return serializable_dict

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StaticConsumerGroupConfig:  # pyright: ignore[reportExplicitAny]
        """Create from dictionary (JSON deserialization)."""
        member_mappings = []
        if "member_mappings" in data:
            member_mappings = [
                MemberMapping(member=m["member"], partitions=m["partitions"])  # pyright: ignore[reportAny]
                for m in data["member_mappings"]  # pyright: ignore[reportAny]
            ]
        return cls(
            max_members=data["max_members"],  # pyright: ignore[reportAny]
            filter=data.get("filter", ""),  # pyright: ignore[reportAny]
            members=data.get("members", []),  # pyright: ignore[reportAny]
            member_mappings=member_mappings,
        )


async def get_static_consumer_group_config(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> StaticConsumerGroupConfig:
    """Get the static consumer group's config from the KV bucket."""
    try:
        kv = await js.key_value(KV_STATIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the static consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    return await _get_static_consumer_group_config(kv, stream_name, consumer_group_name)


async def static_consume(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_name: str,
    message_handler: Callable[[Msg], Awaitable[None]],
    config: ConsumerConfig,
) -> ConsumerGroupConsumeContext:
    """Start consuming messages from a static consumer group."""
    # Ensure minimum ack wait
    if config.ack_wait is None or config.ack_wait < ACK_WAIT:
        config.ack_wait = ACK_WAIT

    # Check stream exists
    try:
        _ = await js.stream_info(stream_name)
    except NotFoundError as e:
        msg = f"the static consumer group's stream does not exist: {e}"
        raise ValueError(msg) from e

    # Get KV bucket
    try:
        kv = await js.key_value(KV_STATIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the static consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    # Get current config
    try:
        consumer_group_config = await _get_static_consumer_group_config(
            kv, stream_name, consumer_group_name
        )
    except Exception as e:
        msg = f"can not get the current static consumer group's config: {e}"
        raise ValueError(msg) from e

    # Create instance
    instance = StaticConsumerGroupConsumerInstance(
        stream_name=stream_name,
        consumer_group_name=consumer_group_name,
        member_name=member_name,
        config=consumer_group_config,
        message_handler=message_handler,
        consumer_config=config,
        js=js,
        kv=kv,
    )

    # Start the instance
    await instance.start()

    return instance


async def create_static(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    max_members: int,
    filter: str = "",  # noqa: A002
    members: list[str] | None = None,
    member_mappings: list[MemberMapping] | None = None,
) -> StaticConsumerGroupConfig:
    """Create a static consumer group."""
    if members is None:
        members = []
    if member_mappings is None:
        member_mappings = []

    config = StaticConsumerGroupConfig(
        max_members=max_members,
        filter=filter,
        members=members,
        member_mappings=member_mappings,
    )

    # Validate config
    validate_static_config(config)

    # Get stream info for replicas
    stream_info: StreamInfo = await js.stream_info(stream_name)
    replicas = stream_info.config.num_replicas or 1

    # Get or create KV bucket
    try:
        kv = await js.key_value(KV_STATIC_BUCKET_NAME)
    except KeyValueError:
        kv = await js.create_key_value(  # pyright: ignore[reportUnknownMemberType]
            KeyValueConfig(
                bucket=KV_STATIC_BUCKET_NAME,
                replicas=replicas,
                storage=StorageType.FILE,
            )
        )

    # Check if config already exists
    key = compose_key(stream_name, consumer_group_name)
    try:
        entry = await kv.get(key)
        # Config exists, verify it matches
        if entry.value is None:
            msg = "static consumer group config has no value"
            raise ValueError(msg)
        existing_config = StaticConsumerGroupConfig.from_dict(json.loads(entry.value))  # pyright: ignore[reportAny]

        if (
            existing_config.max_members != max_members
            or existing_config.filter != filter
            or existing_config.members != members
            or len(existing_config.member_mappings) != len(member_mappings)
            or any(
                em.member != m.member or em.partitions != m.partitions
                for em, m in zip(
                    existing_config.member_mappings, member_mappings, strict=False
                )
            )
        ):
            msg = "the existing static consumer group config doesn't match"
            raise ValueError(msg)

        return existing_config
    except KeyNotFoundError:
        # Create new entry
        payload = json.dumps(config.to_dict()).encode()
        _ = await kv.put(key, payload)
        return config


async def delete_static(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> None:
    """Delete a static consumer group."""

    # Get KV bucket
    try:
        kv = await js.key_value(KV_STATIC_BUCKET_NAME)
    except KeyValueError:
        return

    # Delete config from KV
    try:
        _ = await kv.delete(compose_key(stream_name, consumer_group_name))
    except KeyNotFoundError:
        pass

    # Delete all consumers for this group
    consumers_info = await js.consumers_info(stream_name)

    for consumer_info in consumers_info:
        if consumer_info.name.startswith(f"{consumer_group_name}-"):
            try:
                _ = await js.delete_consumer(stream_name, consumer_info.name)
            except NotFoundError:
                pass


async def list_static_consumer_groups(
    js: JetStreamContext, stream_name: str
) -> list[str]:
    """List the static consumer groups for a given stream."""
    try:
        kv = await js.key_value(KV_STATIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the static consumer group's KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    consumer_group_names: list[str] = []
    try:
        keys = await kv.keys()  # pyright: ignore[reportUnknownMemberType]
        for key in keys:
            parts = key.split(".")
            if len(parts) >= 2 and parts[0] == stream_name:
                consumer_group_names.append(parts[1])
    except Exception as e:
        msg = f"error listing keys in static consumer groups' bucket: {e}"
        raise ValueError(msg) from e

    return consumer_group_names


async def list_static_active_members(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> list[str]:
    """List the active members of a static consumer group."""
    # Get KV and config
    try:
        kv = await js.key_value(KV_STATIC_BUCKET_NAME)
    except KeyValueError as e:
        raise e

    config = await _get_static_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    # Get stream and consumers
    consumers_info = await js.consumers_info(stream_name)

    active_members: list[str] = []

    for consumer_info in consumers_info:
        # Check members list
        if config.members:
            for member in config.members:
                consumer_name = _compose_static_consumer_name(
                    consumer_group_name, member
                )
                if (
                    consumer_info.name == consumer_name
                    and consumer_info.num_waiting
                    and consumer_info.num_waiting > 0
                ):
                    active_members.append(member)
                    break
        # Check member mappings
        elif config.member_mappings:
            for mapping in config.member_mappings:
                consumer_name = _compose_static_consumer_name(
                    consumer_group_name, mapping.member
                )
                if (
                    consumer_info.name == consumer_name
                    and consumer_info.num_waiting
                    and consumer_info.num_waiting > 0
                ):
                    active_members.append(mapping.member)
                    break

    return active_members


async def static_member_step_down(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_name: str,
) -> None:
    """Force the current active (pinned) member to step down."""
    consumer_name = _compose_static_consumer_name(consumer_group_name, member_name)
    # Get consumer info
    info: ConsumerInfo = await js.consumer_info(stream_name, consumer_name)

    # Update config to force re-pin
    # Note: In nats-py, we may need to delete and recreate the consumer
    # since there's no direct update_consumer method
    _ = await js.delete_consumer(stream_name, consumer_name)

    config = info.config
    # BUG: priority group support missing from nats-py
    # For now, just recreate the consumer which will reset the pin
    _ = await js.add_consumer(stream_name, config)  # pyright: ignore[reportUnknownMemberType]


def validate_static_config(config: StaticConsumerGroupConfig) -> None:
    """Validate a static consumer group configuration."""
    # Validate max_members
    if config.max_members < 1:
        msg = "the max number of members must be >= 1"
        raise ValueError(msg)

    # Can't have both members and member_mappings
    if config.members and config.member_mappings:
        msg = "either members or member mappings must be provided, not both"
        raise ValueError(msg)

    # Validate member_mappings if provided
    if config.member_mappings:
        if not (1 <= len(config.member_mappings) <= config.max_members):
            msg = "the number of member mappings must be between 1 and the max number of members"
            raise ValueError(msg)

        seen_members: set[str] = set()
        seen_partitions: set[int] = set()

        for mapping in config.member_mappings:
            # Check unique members
            if mapping.member in seen_members:
                msg = "member names must be unique"
                raise ValueError(msg)
            seen_members.add(mapping.member)

            # Check unique partitions
            for partition in mapping.partitions:
                if partition in seen_partitions:
                    msg = "partition numbers must be used only once"
                    raise ValueError(msg)
                seen_partitions.add(partition)

                # Check partition range
                if not (0 <= partition < config.max_members):
                    msg = "partition numbers must be between 0 and one less than the max number of members"
                    raise ValueError(msg)

        # Check all partitions are covered
        if len(seen_partitions) != config.max_members:
            msg = "the number of unique partition numbers must be equal to the max number of members"
            raise ValueError(msg)


async def _get_static_consumer_group_config(
    kv: KeyValue, stream_name: str, consumer_group_name: str
) -> StaticConsumerGroupConfig:
    """Internal function to get static consumer group config."""
    if not stream_name or not consumer_group_name:
        msg = "invalid stream name or consumer group name"
        raise ValueError(msg)

    try:
        entry = await kv.get(compose_key(stream_name, consumer_group_name))
    except KeyNotFoundError as e:
        msg = "error getting the static consumer group's config: not found"
        raise ValueError(msg) from e

    if entry.value is None:
        msg = "static consumer group config has no value"
        raise ValueError(msg)

    try:
        data = json.loads(entry.value)  # pyright: ignore[reportAny]
        config = StaticConsumerGroupConfig.from_dict(data)  # pyright: ignore[reportAny]
    except (json.JSONDecodeError, KeyError) as e:
        msg = f"invalid JSON value for the static consumer group's config: {e}"
        raise ValueError(msg) from e

    validate_static_config(config)
    return config


def _compose_static_consumer_name(cg_name: str, member: str) -> str:
    """Compose the stream's consumer name for the member in the static consumer group."""
    return f"{cg_name}-{member}"
