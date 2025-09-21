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
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from nats.js.api import (
    ConsumerConfig,
    ConsumerInfo,
    DiscardPolicy,
    KeyValueConfig,
    RetentionPolicy,
    StorageType,
    StreamConfig,
    StreamSource,
    SubjectTransform,
)
from nats.js.errors import (
    APIError,
    KeyNotFoundError,
    KeyValueError,
    NotFoundError,
)
from typing_extensions import override

from .common import (
    ACK_WAIT,
    CONSUMER_IDLE_TIMEOUT,
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
    from nats.js.api import StreamInfo
    from nats.js.kv import KeyValue

logger = logging.getLogger(__name__)

KV_ELASTIC_BUCKET_NAME = "elastic-consumer-groups"


class ElasticConsumerGroupConsumerInstance(ConsumerGroupConsumeContext):
    """Instance for consuming messages from an elastic consumer group."""

    stream_name: str
    consumer_group_name: str
    member_name: str
    config: ElasticConsumerGroupConfig
    message_handler: Callable[[Msg], Awaitable[None]]
    consumer_user_config: ConsumerConfig
    consumer: ConsumerInfo | None
    current_pinned_id: str
    js: JetStreamContext
    kv: KeyValue
    subscription: JetStreamContext.PullSubscription | None
    key_watcher: KeyValue.KeyWatcher | None
    _cancel_event: asyncio.Event
    _done_future: asyncio.Future[Exception | None]
    _consume_task: asyncio.Task[None] | None
    _instance_task: asyncio.Task[None] | None

    def __init__(
        self,
        stream_name: str,
        consumer_group_name: str,
        member_name: str,
        config: ElasticConsumerGroupConfig,
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
        self.consumer = None
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
        """Future that completes when consumption is done."""
        return self._done_future

    async def start(self) -> None:
        """Start the consumer group instance."""
        # Watch for config changes
        self.key_watcher = await self.kv.watch(  # pyright: ignore[reportUnknownMemberType]
            compose_key(self.stream_name, self.consumer_group_name)
        )

        # Join the consumer group if member is in membership
        if self.config.is_in_membership(self.member_name):
            await self._join_member_consumer()

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
                tasks = []
                try:
                    # Create tasks for different events
                    update_task = asyncio.create_task(anext(update_iterator))
                    timeout_task = asyncio.create_task(
                        asyncio.sleep(CONSUMER_IDLE_TIMEOUT + 1.0)
                    )
                    cancel_task = asyncio.create_task(self._cancel_event.wait())

                    tasks = [update_task, timeout_task, cancel_task]

                    # Wait for first to complete
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )

                    # Cancel pending tasks
                    for task in pending:
                        _ = task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    # Process completed task
                    for task in done:
                        if task is cancel_task:
                            # Context was cancelled
                            await self._stop_consuming()
                            self._done_future.set_result(None)
                            return

                        elif task is update_task:
                            try:
                                update_msg = update_task.result()
                            except StopAsyncIteration:
                                # Key watcher ended
                                await self._stop_consuming()
                                self._done_future.set_exception(
                                    ValueError(
                                        "the elastic consumer group config watcher has been closed, stopping"
                                    )
                                )
                                return

                            # Check if config was deleted
                            if update_msg and update_msg.operation == "DELETE":
                                await self._stop_consuming()
                                self._done_future.set_result(None)
                                return

                            # Parse and validate new config
                            try:
                                if not update_msg or update_msg.value is None:
                                    continue
                                new_config = ElasticConsumerGroupConfig.from_dict(
                                    json.loads(update_msg.value)  # pyright: ignore[reportAny]
                                )
                                _validate_config(new_config)
                            except (json.JSONDecodeError, ValueError) as e:
                                await self._stop_consuming()
                                self._done_future.set_exception(e)
                                return

                            # Check if critical config changed
                            if (
                                new_config.max_members != self.config.max_members
                                or new_config.filter != self.config.filter
                                or new_config.max_buffered_msgs
                                != self.config.max_buffered_msgs
                                or new_config.max_buffered_bytes
                                != self.config.max_buffered_bytes
                                or new_config.partitioning_wildcards
                                != self.config.partitioning_wildcards
                            ):
                                await self._stop_consuming()
                                self._done_future.set_exception(
                                    ValueError(
                                        "elastic consumer group config watcher received a bad change in the configuration"
                                    )
                                )
                                return

                            # Check if membership changed
                            if (
                                self.consumer is not None
                                and new_config.members == self.config.members
                                and len(new_config.member_mappings)
                                == len(self.config.member_mappings)
                                and all(
                                    nm.member == om.member
                                    and nm.partitions == om.partitions
                                    for nm, om in zip(
                                        new_config.member_mappings,
                                        self.config.member_mappings,
                                        strict=False,
                                    )
                                )
                            ):
                                # No change needed
                                continue

                            # Update config and process membership change
                            self.config.members = new_config.members
                            self.config.member_mappings = new_config.member_mappings
                            await self._process_membership_change()

                        elif task is timeout_task:
                            # Self-correction: try to join if not currently joined but should be
                            if self.consumer is None and self.config.is_in_membership(
                                self.member_name
                            ):
                                await self._join_member_consumer()

                except Exception as e:
                    # Ensure cleanup on any error
                    for t in tasks:
                        if not t.done():
                            _ = t.cancel()
                    raise e

        except Exception as e:  # noqa: BLE001
            await self._stop_consuming()
            if not self._done_future.done():
                self._done_future.set_exception(e)

    async def _consumer_callback(self, msg: Msg) -> None:
        """Shim callback to strip partition number from subject before passing to user."""
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

    async def _join_member_consumer(self) -> None:
        """Attempt to create the member's consumer and start consuming if successful."""
        filters = generate_partition_filters(
            self.config.members,
            self.config.max_members,
            self.config.member_mappings,
            self.member_name,
        )

        # if we are no longer in the membership list, nothing to do
        if not filters:
            return

        config = self.consumer_user_config
        config.durable_name = None  # Not durable for elastic
        config.name = self.member_name
        config.filter_subjects = filters

        # Priority groups for pinning
        # BUG: priority groups are required for this module, but nats-py hasn't
        # implemented support for them yet
        # config.priority_groups = [self.member_name]
        # config.priority_policy = "pinned"
        # config.pinned_ttl = config.ack_wait

        # Try to create consumer
        consumer_stream_name = _compose_cgs_name(
            self.stream_name, self.consumer_group_name
        )
        try:
            self.consumer = await self.js.add_consumer(consumer_stream_name, config)  # pyright: ignore[reportUnknownMemberType]
        except APIError as e:
            # Check if consumer already exists
            if "consumer already exists" in str(e).lower():
                # Try to delete and recreate
                try:
                    _ = await self.js.delete_consumer(
                        consumer_stream_name, self.member_name
                    )
                    self.consumer = await self.js.add_consumer(  # pyright: ignore[reportUnknownMemberType]
                        consumer_stream_name, config
                    )
                except Exception:
                    # Will try again later
                    # BUG: is this right to pass?
                    return
            else:
                # Some other API error
                return
        except Exception:
            # Will try again later
            # BUG: is this right to pass?
            return

        await self._start_consuming()

    async def _start_consuming(self) -> None:
        """Start actively consuming messages from the consumer."""
        if not self.consumer:
            return

        consumer_stream_name = _compose_cgs_name(
            self.stream_name, self.consumer_group_name
        )

        # Create pull subscription
        self.subscription = await self.js.pull_subscribe(
            "", durable=self.member_name, stream=consumer_stream_name
        )

        # Start processing messages
        async def process_messages():
            while not self._cancel_event.is_set():
                try:
                    if self.subscription is None:
                        break
                    msgs = await self.subscription.fetch(batch=1, timeout=PULL_TIMEOUT)
                    for msg in msgs:
                        await self._consumer_callback(msg)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    # BUG: is this right to break? should it raise or cancel?
                    logger.error("Error processing messages: %s", e)
                    break

        self._consume_task = asyncio.create_task(process_messages())

    async def _stop_consuming(self) -> None:
        """Stop consuming messages from the consumer."""
        if self._consume_task:
            _ = self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        if self.subscription:
            try:
                await self.subscription.unsubscribe()
            except Exception:
                # BUG: is this right to pass?
                pass
            self.subscription = None

        self.consumer = None

    async def _process_membership_change(self) -> None:
        """Process membership changes."""
        # Get current consumer info to check if pinned
        is_pinned = False

        if self.consumer:
            consumer_stream_name = _compose_cgs_name(
                self.stream_name, self.consumer_group_name
            )
            try:
                _ = await self.js.consumer_info(consumer_stream_name, self.member_name)
                # Check if we're the pinned consumer
                # BUG: implementation missing
                is_pinned = True
            except Exception:
                pass

            await self._stop_consuming()

        # Only the pinned member should delete the consumer
        if is_pinned:
            consumer_stream_name = _compose_cgs_name(
                self.stream_name, self.consumer_group_name
            )
            try:
                _ = await self.js.delete_consumer(
                    consumer_stream_name, self.member_name
                )
            except (NotFoundError, asyncio.TimeoutError):
                pass
        else:
            # Backoff to let pinned member handle it first
            await asyncio.sleep(random.randint(400, 500) / 1000.0)  # noqa: S311

        await self._join_member_consumer()


@dataclass
class ElasticConsumerGroupConfig:
    """Configuration for an elastic consumer group."""

    max_members: int
    filter: str
    partitioning_wildcards: list[int]
    max_buffered_msgs: int | None = None
    max_buffered_bytes: int | None = None
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
        result: dict[str, Any] = {  # pyright: ignore[reportExplicitAny]
            "max_members": self.max_members,
            "filter": self.filter,
            "partitioning_wildcards": self.partitioning_wildcards,
        }
        if self.max_buffered_msgs is not None:
            result["max_buffered_msgs"] = self.max_buffered_msgs
        if self.max_buffered_bytes is not None:
            result["max_buffered_bytes"] = self.max_buffered_bytes
        if self.members:
            result["members"] = self.members
        if self.member_mappings:
            result["member_mappings"] = [
                {"member": m.member, "partitions": m.partitions}
                for m in self.member_mappings
            ]
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ElasticConsumerGroupConfig:  # pyright: ignore[reportExplicitAny]
        """Create from dictionary (JSON deserialization)."""
        member_mappings = []
        if "member_mappings" in data:
            member_mappings = [
                MemberMapping(member=m["member"], partitions=m["partitions"])  # pyright: ignore[reportAny]
                for m in data["member_mappings"]  # pyright: ignore[reportAny]
            ]
        return cls(
            max_members=data["max_members"],  # pyright: ignore[reportAny]
            filter=data["filter"],  # pyright: ignore[reportAny]
            partitioning_wildcards=data["partitioning_wildcards"],  # pyright: ignore[reportAny]
            max_buffered_msgs=data.get("max_buffered_msgs"),
            max_buffered_bytes=data.get("max_buffered_bytes"),
            members=data.get("members", []),  # pyright: ignore[reportAny]
            member_mappings=member_mappings,
        )


async def get_elastic_consumer_group_config(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> ElasticConsumerGroupConfig:
    """Get the elastic consumer group's config from the KV bucket."""
    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the elastic consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    return await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )


async def elastic_consume(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_name: str,
    message_handler: Callable[[Msg], Awaitable[None]],
    config: ConsumerConfig,
) -> ConsumerGroupConsumeContext:
    """Start consuming messages from an elastic consumer group."""
    if config.ack_policy != "explicit":
        msg = "the ack policy when consuming from elastic consumer groups must be explicit"
        raise ValueError(msg)

    # Ensure minimum timeouts
    if config.inactive_threshold is None or config.inactive_threshold == 0:
        config.inactive_threshold = CONSUMER_IDLE_TIMEOUT

    if config.ack_wait is None or config.ack_wait < ACK_WAIT:
        config.ack_wait = ACK_WAIT

    # Check stream exists
    try:
        _ = await js.stream_info(_compose_cgs_name(stream_name, consumer_group_name))
    except NotFoundError as e:
        msg = f"the elastic consumer group's stream does not exist: {e}"
        raise ValueError(msg) from e

    # Get KV bucket
    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the elastic consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    # Get current config
    try:
        consumer_group_config = await _get_elastic_consumer_group_config(
            kv, stream_name, consumer_group_name
        )
    except Exception as e:
        msg = f"can not get the current elastic consumer group's config: {e}"
        raise ValueError(msg) from e

    # Create instance
    instance = ElasticConsumerGroupConsumerInstance(
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


async def create_elastic(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    max_num_members: int,
    filter: str,  # noqa: A002
    partitioning_wildcards: list[int],
    max_buffered_messages: int | None = None,
    max_buffered_bytes: int | None = None,
) -> ElasticConsumerGroupConfig:
    """Create an elastic consumer group and its sourcing work queue stream."""
    config = ElasticConsumerGroupConfig(
        max_members=max_num_members,
        filter=filter,
        partitioning_wildcards=partitioning_wildcards,
        max_buffered_msgs=max_buffered_messages,
        max_buffered_bytes=max_buffered_bytes,
    )

    # Validate config
    _validate_config(config)

    # Get partitioning transform destination
    filter_dest = _get_partitioning_transform_dest(config)

    # Get stream info for replicas
    try:
        stream_info: StreamInfo = await js.stream_info(stream_name)
    except NotFoundError as e:
        msg = f"stream {stream_name} not found"
        raise ValueError(msg) from e

    replicas = stream_info.config.num_replicas or 1
    storage = stream_info.config.storage or StorageType.FILE

    # Get or create KV bucket
    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError:
        kv = await js.create_key_value(  # pyright: ignore[reportUnknownMemberType]
            KeyValueConfig(
                bucket=KV_ELASTIC_BUCKET_NAME,
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
            msg = "elastic consumer group config has no value"
            raise ValueError(msg)
        existing_config = ElasticConsumerGroupConfig.from_dict(json.loads(entry.value))  # pyright: ignore[reportAny]

        if (
            existing_config.max_members != max_num_members
            or existing_config.filter != filter
            or existing_config.max_buffered_msgs != max_buffered_messages
            or existing_config.max_buffered_bytes != max_buffered_bytes
            or existing_config.partitioning_wildcards != partitioning_wildcards
        ):
            msg = "the existing elastic consumer group config can not be updated"
            raise ValueError(msg)

        consumer_group_config = existing_config
    except KeyNotFoundError:
        # Create new entry
        consumer_group_config = config
        payload = json.dumps(config.to_dict()).encode()
        _ = await kv.put(key, payload)

    # Create the consumer group's stream
    stream_config = StreamConfig(
        name=_compose_cgs_name(stream_name, consumer_group_name),
        retention=RetentionPolicy.WORK_QUEUE,
        num_replicas=replicas,
        storage=storage,
        max_msgs=max_buffered_messages,
        max_bytes=max_buffered_bytes,
        discard=DiscardPolicy.NEW,
        sources=[
            StreamSource(
                name=stream_name,
                opt_start_seq=0,
                filter_subject="",
                subject_transforms=[SubjectTransform(src=filter, dest=filter_dest)],
            )
        ],
        allow_direct=True,
    )

    try:
        _ = await js.add_stream(stream_config)  # pyright: ignore[reportUnknownMemberType]
    except Exception as e:
        msg = f"can't create the elastic consumer group's stream: {e}"
        raise ValueError(msg) from e

    return consumer_group_config


async def delete_elastic(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> None:
    """Delete an elastic consumer group."""
    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError:
        return

    # Delete config from KV
    try:
        _ = await kv.delete(compose_key(stream_name, consumer_group_name))
    except KeyNotFoundError:
        pass

    # Delete the consumer group's stream
    try:
        _ = await js.delete_stream(_compose_cgs_name(stream_name, consumer_group_name))
    except NotFoundError:
        pass


async def list_elastic_consumer_groups(
    js: JetStreamContext, stream_name: str
) -> list[str]:
    """List the elastic consumer groups for a given stream."""
    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"error getting elastic consumer group KV bucket: {e}"
        raise ValueError(msg) from e

    consumer_group_names: list[str] = []
    try:
        keys = await kv.keys()  # pyright: ignore[reportUnknownMemberType]
        for key in keys:
            parts = key.split(".")
            if len(parts) >= 2 and parts[0] == stream_name:
                consumer_group_names.append(parts[1])
    except Exception as e:
        msg = f"error listing keys in elastic consumer groups' bucket: {e}"
        raise ValueError(msg) from e

    return consumer_group_names


async def add_members(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_names_to_add: list[str],
) -> list[str]:
    """Add members to an elastic consumer group."""
    if not stream_name or not consumer_group_name or not member_names_to_add:
        msg = "invalid stream name or elastic consumer group name or no member names"
        raise ValueError(msg)

    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the elastic consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    # Get current config
    config = await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    if config.member_mappings:
        msg = "can't add members to an elastic consumer group that uses member mappings"
        raise ValueError(msg)

    # Add new members (deduplicated)
    existing_members = set(config.members)
    for member_name in member_names_to_add:
        if member_name:
            existing_members.add(member_name)

    config.members = list(existing_members)

    # Update config
    payload = json.dumps(config.to_dict()).encode()
    _ = await kv.put(compose_key(stream_name, consumer_group_name), payload)

    return config.members


async def delete_members(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_names_to_drop: list[str],
) -> list[str]:
    """Drop members from an elastic consumer group."""
    if not stream_name or not consumer_group_name or not member_names_to_drop:
        msg = "invalid stream name or elastic consumer group name or no member names"
        raise ValueError(msg)

    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the elastic consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    # Get current config
    config = await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    if config.member_mappings:
        msg = "can't drop members from an elastic consumer group that uses member mappings"
        raise ValueError(msg)

    # Remove members
    dropping_members = set(member_names_to_drop)
    config.members = [m for m in config.members if m not in dropping_members]

    # Update config
    payload = json.dumps(config.to_dict()).encode()
    _ = await kv.put(compose_key(stream_name, consumer_group_name), payload)

    return config.members


async def set_member_mappings(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_mappings: list[MemberMapping],
) -> None:
    """Set the custom member mappings for an elastic consumer group."""
    if not stream_name or not consumer_group_name or not member_mappings:
        msg = "invalid stream name or elastic consumer group name or member mappings"
        raise ValueError(msg)

    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the elastic consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    # Get current config
    config = await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    # Clear members and set mappings
    config.members = []
    config.member_mappings = member_mappings

    # Validate updated config
    _validate_config(config)

    # Update config
    payload = json.dumps(config.to_dict()).encode()
    _ = await kv.put(compose_key(stream_name, consumer_group_name), payload)


async def delete_member_mappings(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> None:
    """Delete the custom member mappings for an elastic consumer group."""
    if not stream_name or not consumer_group_name:
        msg = "invalid stream name or elastic consumer group name"
        raise ValueError(msg)

    try:
        kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)
    except KeyValueError as e:
        msg = f"the elastic consumer group KV bucket doesn't exist: {e}"
        raise ValueError(msg) from e

    # Get current config
    config = await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    # Clear mappings
    config.member_mappings = []

    # Update config
    payload = json.dumps(config.to_dict()).encode()
    _ = await kv.put(compose_key(stream_name, consumer_group_name), payload)


async def list_elastic_active_members(
    js: JetStreamContext, stream_name: str, consumer_group_name: str
) -> list[str]:
    """List the active members of an elastic consumer group."""
    kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)

    config = await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    if not config.members and not config.member_mappings:
        return []

    # Get consumers from the consumer group's stream
    consumer_stream_name = _compose_cgs_name(stream_name, consumer_group_name)
    try:
        consumers_info = await js.consumers_info(consumer_stream_name)
    except NotFoundError:
        return []

    active_members: list[str] = []

    for consumer_info in consumers_info:
        # Check members list
        if config.members:
            for member in config.members:
                if consumer_info.name == member:
                    active_members.append(member)
                    break
        # Check member mappings
        elif config.member_mappings:
            for mapping in config.member_mappings:
                if consumer_info.name == mapping.member:
                    active_members.append(mapping.member)
                    break

    return active_members


async def elastic_is_in_membership_and_active(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_name: str,
) -> tuple[bool, bool]:
    """Check if a member is included in the elastic consumer group and is active."""
    kv = await js.key_value(KV_ELASTIC_BUCKET_NAME)

    config = await _get_elastic_consumer_group_config(
        kv, stream_name, consumer_group_name
    )

    in_membership = config.is_in_membership(member_name)

    # Check if active
    consumer_stream_name = _compose_cgs_name(stream_name, consumer_group_name)
    try:
        consumers_info = await js.consumers_info(consumer_stream_name)
    except NotFoundError:
        return in_membership, False

    is_active = False
    for consumer_info in consumers_info:
        if consumer_info.name == member_name:
            is_active = True
            break

    return in_membership, is_active


async def elastic_member_step_down(
    js: JetStreamContext,
    stream_name: str,
    consumer_group_name: str,
    member_name: str,
) -> None:
    """Force the current active (pinned) application instance for a member to step down."""
    consumer_stream_name = _compose_cgs_name(stream_name, consumer_group_name)

    # In Python, we can't directly unpin like in Go
    # The best we can do is delete and recreate the consumer
    try:
        info = await js.consumer_info(consumer_stream_name, member_name)
        _ = await js.delete_consumer(consumer_stream_name, member_name)
        # Recreate with same config
        _ = await js.add_consumer(consumer_stream_name, info.config)  # pyright: ignore[reportUnknownMemberType]
    except NotFoundError:
        pass


def elastic_get_partition_filters(
    config: ElasticConsumerGroupConfig, member_name: str
) -> list[str]:
    """Get the list of partition filters for the given member."""
    return generate_partition_filters(
        config.members, config.max_members, config.member_mappings, member_name
    )


# Helper functions


def _validate_config(config: ElasticConsumerGroupConfig) -> None:
    """Validate an elastic consumer group configuration."""
    # Validate max_members
    if config.max_members < 1:
        msg = "the max number of members must be >= 1"
        raise ValueError(msg)

    # Validate filter and partitioning wildcards
    filter_tokens = config.filter.split(".")
    num_wildcards = sum(1 for token in filter_tokens if token == "*")  # noqa: S105

    if num_wildcards < 1:
        msg = "filter must contain at least one * wildcard"
        raise ValueError(msg)

    if not (1 <= len(config.partitioning_wildcards) <= num_wildcards):
        msg = "the number of partitioning wildcards must be between 1 and the total number of * wildcards in the filter"
        raise ValueError(msg)

    seen_wildcards: set[int] = set()
    for wildcard in config.partitioning_wildcards:
        if wildcard in seen_wildcards:
            msg = "partitioning wildcard indexes must be unique"
            raise ValueError(msg)
        seen_wildcards.add(wildcard)

        if not (1 <= wildcard <= num_wildcards):
            msg = "partitioning wildcard indexes must be between 1 and the number of * wildcards in the filter"
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


def _get_partitioning_transform_dest(config: ElasticConsumerGroupConfig) -> str:
    """Get the partitioning transform destination string."""
    wildcard_list = ",".join(str(wc) for wc in config.partitioning_wildcards)
    cw_index = 1
    filter_tokens = config.filter.split(".")

    for i, token in enumerate(filter_tokens):
        if token == "*":  # noqa: S105
            filter_tokens[i] = "{{Wildcard(%d)}}" % cw_index  # noqa: UP031  # intentional printf style for readability
            cw_index += 1

    dest_from_filter = ".".join(filter_tokens)
    # Use printf-style for the partition part too
    return "{{Partition(%d,%s)}}.%s" % (  # noqa: UP031  # intentional printf style for readability
        config.max_members,
        wildcard_list,
        dest_from_filter,
    )


async def _get_elastic_consumer_group_config(
    kv: KeyValue, stream_name: str, consumer_group_name: str
) -> ElasticConsumerGroupConfig:
    """Internal function to get elastic consumer group config."""
    if not stream_name or not consumer_group_name:
        msg = "invalid stream name or elastic consumer group name"
        raise ValueError(msg)

    try:
        entry = await kv.get(compose_key(stream_name, consumer_group_name))
    except KeyNotFoundError as e:
        msg = "error getting the elastic consumer group's config: not found"
        raise ValueError(msg) from e

    if entry.value is None:
        msg = "elastic consumer group config has no value"
        raise ValueError(msg)

    try:
        data = json.loads(entry.value)  # pyright: ignore[reportAny]
        config = ElasticConsumerGroupConfig.from_dict(data)  # pyright: ignore[reportAny]
    except (json.JSONDecodeError, KeyError) as e:
        msg = f"invalid JSON value for the elastic consumer group's config: {e}"
        raise ValueError(msg) from e

    _validate_config(config)
    return config


def _compose_cgs_name(stream_name: str, consumer_group_name: str) -> str:
    """Compose the Consumer Group Stream Name."""
    return f"{stream_name}-{consumer_group_name}"
