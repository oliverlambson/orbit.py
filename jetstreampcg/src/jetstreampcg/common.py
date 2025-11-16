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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from nats.aio.msg import Msg
from typing_extensions import Self

if TYPE_CHECKING:
    import asyncio

    from nats.aio.client import Client as NATS

# Contains the things that are common to both types of consumer groups
# TODO: the failover times and elasticity reaction times could be made more 'real time' by lowering those values. And could maybe be exposed as tunable by the user by making them all derive from the ack wait value set by the user in the consumer config it passes in when joining the consumer group
# At this point however those values are hard-coded to values that seemed to be reasonable in terms of not incurring too much overhead. This is expected to be revisited later according to community feedback.
PULL_TIMEOUT = 3.0
ACK_WAIT = 2 * PULL_TIMEOUT
CONSUMER_IDLE_TIMEOUT = 2 * PULL_TIMEOUT


@dataclass
class MemberMapping:
    """Mapping of a member to its assigned partitions."""

    member: str
    partitions: list[int]


class ConsumerGroupConsumeContext(ABC):
    @abstractmethod
    def stop(self) -> None: ...
    @abstractmethod
    def done(self) -> asyncio.Future[Exception | None]: ...


def compose_key(stream_name: str, consumer_group_name: str) -> str:
    """Compose the consumer group's config key name."""
    return f"{stream_name}.{consumer_group_name}"


def generate_partition_filters(
    members: list[str],
    max_members: int,
    member_mappings: list[MemberMapping],
    member_name: str,
) -> list[str]:
    """Generate the partition filters for a particular member of a consumer group.

    Args:
        members: List of member names
        max_members: Maximum number of members allowed
        member_mappings: Explicit member to partition mappings
        member_name: Name of the member to generate filters for

    Returns:
        List of partition filter strings (e.g., ["0.>", "1.>"])
    """
    if members:
        members = _deduplicate_string_list(members)
        members.sort()

        if len(members) > max_members:
            members = members[:max_members]

        # Distribute the partitions amongst the members trying to minimize the number of
        # partitions getting re-distributed to another member as the number of members
        # increases/decreases
        num_members = len(members)

        if num_members > 0:
            # Rounded number of partitions per member
            num_per = max_members // num_members
            my_filters: list[str] = []

            for i in range(max_members):
                member_index = i // num_per

                if i < (num_members * num_per):
                    if members[member_index % num_members] == member_name:
                        my_filters.append(f"{i}.>")
                else:
                    # Remainder if the number of partitions is not a multiple of members
                    if (
                        members[(i - (num_members * num_per)) % num_members]
                        == member_name
                    ):
                        my_filters.append(f"{i}.>")

            return my_filters
        return []
    elif member_mappings:
        return [
            f"{partition}.>"
            for mapping in member_mappings
            if mapping.member == member_name
            for partition in mapping.partitions
        ]
    return []


class ConsumerGroupMsg(Msg):
    """JetStream message wrapper to strip the partition number from the subject."""

    def __init__(
        self,
        _client: NATS,
        subject: str = "",
        reply: str = "",
        data: bytes = b"",
        headers: dict[str, str] | None = None,
        _metadata: Msg.Metadata | None = None,
        _ackd: bool = False,  # noqa: FBT001,FBT002
        _sid: int | None = None,
    ) -> None:
        super().__init__(
            _client=_client,
            subject=subject,
            reply=reply,
            data=data,
            headers=headers,
            _metadata=_metadata,
            _ackd=_ackd,
            _sid=_sid,
        )
        self._original_subject: str = self.subject
        dot_index = self._original_subject.find(".")
        if dot_index != -1:
            self.subject: str = self._original_subject[dot_index + 1 :]

    @classmethod
    def from_msg(cls, msg: Msg) -> Self:
        return cls(
            _client=msg._client,
            subject=msg.subject,
            reply=msg.reply,
            data=msg.data,
            headers=msg.headers,
            _metadata=msg._metadata,
            _ackd=msg._ackd,
            _sid=msg._sid,
        )


def _deduplicate_string_list(items: list[str]) -> list[str]:
    """Remove duplicates from a string list while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result
