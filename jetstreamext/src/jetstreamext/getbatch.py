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

import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from nats.aio.msg import Msg
from nats.js import api

if TYPE_CHECKING:
    from nats import NATS
    from nats.js import JetStreamContext

import natsext


class BatchUnsupportedError(Exception):
    """Raised when the server does not support batch get (nats server >=2.11.0)."""


class InvalidResponseError(Exception):
    """Raised when the response from the server is invalid."""


class NoMessagesError(Exception):
    """Raised when there are no messages to fetch given the provided options."""


class InvalidOptionError(Exception):
    """Raised when an invalid option is provided."""


class SubjectRequiredError(Exception):
    """Raised when no subjects are provided in get_last_msgs_for."""


@dataclass
class _GetBatchOpts:
    seq: int | None = None
    next_for: str | None = None
    batch: int = 0
    max_bytes: int | None = None
    start_time: datetime | None = None


@dataclass
class _GetLastBatchOpts:
    multi_last_for: list[str] | None = None
    batch: int | None = None
    up_to_seq: int | None = None
    up_to_time: datetime | None = None


# TODO: this chaining of opt "wrapper" functions is idiomatic of go, but in python
# should probably rather be single a Config class passed in.
def get_batch_seq(seq: int) -> Callable[[_GetBatchOpts], None]:
    """Set the sequence number from which to start fetching messages."""

    def apply_opt(opts: _GetBatchOpts) -> None:
        if seq <= 0:
            msg = "sequence number has to be greater than 0"
            raise InvalidOptionError(msg)
        if opts.start_time is not None:
            msg = "cannot set both start time and sequence number"
            raise InvalidOptionError(msg)
        opts.seq = seq

    # Validate immediately when creating the option
    if seq <= 0:
        msg = "sequence number has to be greater than 0"
        raise InvalidOptionError(msg)

    return apply_opt


def get_batch_subject(subj: str) -> Callable[[_GetBatchOpts], None]:
    """Set the subject from which to start fetching messages. It may include wildcards."""

    def apply_opt(opts: _GetBatchOpts) -> None:
        opts.next_for = subj

    return apply_opt


def get_batch_max_bytes(max_bytes: int) -> Callable[[_GetBatchOpts], None]:
    """Set the maximum number of bytes to fetch.

    The server will try to fetch messages until the maximum number of bytes is
    reached (or the batch size is reached).
    """

    def apply_opt(opts: _GetBatchOpts) -> None:
        if max_bytes <= 0:
            msg = "max bytes has to be greater than 0"
            raise InvalidOptionError(msg)
        opts.max_bytes = max_bytes

    # Validate immediately when creating the option
    if max_bytes <= 0:
        msg = "max bytes has to be greater than 0"
        raise InvalidOptionError(msg)

    return apply_opt


def get_batch_start_time(start_time: datetime) -> Callable[[_GetBatchOpts], None]:
    """Set the start time from which to fetch messages."""

    def apply_opt(opts: _GetBatchOpts) -> None:
        if opts.seq is not None and opts.seq != 0:
            msg = "cannot set both start time and sequence number"
            raise InvalidOptionError(msg)
        opts.start_time = start_time

    return apply_opt


def get_last_msgs_up_to_seq(seq: int) -> Callable[[_GetLastBatchOpts], None]:
    """Set the sequence number up to which to fetch messages (inclusive)."""

    def apply_opt(opts: _GetLastBatchOpts) -> None:
        if opts.up_to_time is not None:
            msg = "cannot set both up to sequence and up to time"
            raise InvalidOptionError(msg)
        opts.up_to_seq = seq

    return apply_opt


def get_last_msgs_up_to_time(tm: datetime) -> Callable[[_GetLastBatchOpts], None]:
    """Set the time up to which to fetch messages."""

    def apply_opt(opts: _GetLastBatchOpts) -> None:
        if opts.up_to_seq is not None and opts.up_to_seq != 0:
            msg = "cannot set both up to sequence and up to time"
            raise InvalidOptionError(msg)
        opts.up_to_time = tm

    return apply_opt


def get_last_msgs_batch_size(batch: int) -> Callable[[_GetLastBatchOpts], None]:
    """Set the optional batch size for fetching messages from multiple subjects."""

    def apply_opt(opts: _GetLastBatchOpts) -> None:
        if batch <= 0:
            msg = "batch size has to be greater than 0"
            raise InvalidOptionError(msg)
        opts.batch = batch

    # Validate immediately when creating the option
    if batch <= 0:
        msg = "batch size has to be greater than 0"
        raise InvalidOptionError(msg)

    return apply_opt


async def get_batch(
    js: JetStreamContext,
    stream: str,
    batch: int,
    *opts: Callable[[_GetBatchOpts], None],
) -> AsyncIterator[api.RawStreamMsg]:
    """Fetch a batch of messages from the specified stream.

    The batch size is determined by the `batch` parameter.
    The function returns an async iterator that can be used to iterate over the
    messages. Any error received during iteration will terminate the loop.
    The iterator will raise an error if there are no messages to fetch.

    Args:
        js: JetStream context
        stream: Stream name
        batch: Batch size
        *opts: Optional configuration functions

    Yields:
        RawStreamMsg: Stream messages

    Raises:
        InvalidOptionError: If invalid options are provided
        NoMessagesError: If no messages are available
        BatchUnsupportedError: If batch get is not supported
        InvalidResponseError: If server response is invalid
    """
    req_opts = _GetBatchOpts(batch=batch)

    for opt in opts:
        opt(req_opts)

    if req_opts.start_time is None and (req_opts.seq is None or req_opts.seq == 0):
        req_opts.seq = 1

    req_json = _serialize_get_batch_opts(req_opts)
    async for msg in _get_direct(js._nc, js, stream, req_json):
        yield msg


async def get_last_msgs_for(
    js: JetStreamContext,
    stream: str,
    subjects: list[str],
    *opts: Callable[[_GetLastBatchOpts], None],
) -> AsyncIterator[api.RawStreamMsg]:
    """Fetch the last messages for the specified subjects from the specified stream.

    The function returns an async iterator that can be used to iterate over the
    messages. Any error received during iteration will terminate the loop.
    It can be configured to fetch messages up to a certain stream sequence number or
    time.

    Args:
        js: JetStream context
        stream: Stream name
        subjects: List of subjects to fetch messages for
        *opts: Optional configuration functions

    Yields:
        RawStreamMsg: Stream messages

    Raises:
        SubjectRequiredError: If no subjects are provided
        InvalidOptionError: If invalid options are provided
        NoMessagesError: If no messages are available
        BatchUnsupportedError: If batch get is not supported
        InvalidResponseError: If server response is invalid
    """
    if not subjects:
        msg = "at least one subject is required"
        raise SubjectRequiredError(msg)

    req_opts = _GetLastBatchOpts(multi_last_for=subjects)

    for opt in opts:
        opt(req_opts)

    req_json = _serialize_get_last_batch_opts(req_opts)
    async for msg in _get_direct(js._nc, js, stream, req_json):
        yield msg


def _serialize_get_batch_opts(opts: _GetBatchOpts) -> bytes:
    """Serialize get batch options to JSON bytes."""
    data = {"batch": opts.batch}

    if opts.seq is not None:
        data["seq"] = opts.seq
    if opts.next_for is not None:
        data["next_by_subj"] = opts.next_for
    if opts.max_bytes is not None:
        data["max_bytes"] = opts.max_bytes
    if opts.start_time is not None:
        # Format in RFC3339Nano format that NATS expects
        # Convert to UTC and use Z suffix (not +00:00)
        utc_time = opts.start_time.astimezone(timezone.utc)
        iso_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
        # Pad microseconds to nanoseconds (6 digits to 9 digits)
        parts = iso_str.split(".")
        if len(parts[1]) < 9:
            parts[1] = parts[1].ljust(9, "0")[:9]
        data["start_time"] = f"{parts[0]}.{parts[1]}Z"

    return json.dumps(data).encode()


def _serialize_get_last_batch_opts(opts: _GetLastBatchOpts) -> bytes:
    """Serialize get last batch options to JSON bytes."""
    data = {}

    if opts.multi_last_for is not None:
        data["multi_last"] = opts.multi_last_for
    if opts.batch is not None:
        data["batch"] = opts.batch
    if opts.up_to_seq is not None:
        data["up_to_seq"] = opts.up_to_seq
    if opts.up_to_time is not None:
        # Format in RFC3339Nano format that NATS expects
        # Convert to UTC and use Z suffix (not +00:00)
        utc_time = opts.up_to_time.astimezone(timezone.utc)
        iso_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
        # Pad microseconds to nanoseconds (6 digits to 9 digits)
        parts = iso_str.split(".")
        if len(parts[1]) < 9:
            parts[1] = parts[1].ljust(9, "0")[:9]
        data["up_to_time"] = f"{parts[0]}.{parts[1]}Z"

    return json.dumps(data).encode()


async def _get_direct(
    nc: NATS, js: JetStreamContext, stream: str, req: bytes
) -> AsyncIterator[api.RawStreamMsg]:
    """Internal function to perform direct get request."""
    subj = _get_prefixed_subject(js, f"DIRECT.GET.{stream}")

    def eob_sentinel(msg: Msg) -> bool:
        """End-of-batch sentinel function."""
        status = msg.header.get("Status") if msg.header else None
        desc = msg.header.get("Description") if msg.header else None
        return len(msg.data) == 0 and status == "204" and desc == "EOB"

    async for msg in natsext.request_many(
        nc, subj, req, sentinel=eob_sentinel, timeout=30.0
    ):
        try:
            raw_msg = _convert_direct_get_msg_response_to_msg(msg)
            yield raw_msg
        except NoMessagesError:
            return


def _convert_direct_get_msg_response_to_msg(msg: Msg) -> api.RawStreamMsg:
    """Convert direct get message response to RawStreamMsg."""
    if len(msg.data) == 0:
        status = msg.header.get("Status") if msg.header else None
        if status == "404":
            error_msg = "no messages"
            raise NoMessagesError(error_msg)
        elif status and status != "204":
            # Handle other error status codes (like 408 Malformed Request)
            desc = msg.header.get("Description") if msg.header else "Unknown error"
            error_msg = f"Server returned status {status}: {desc}"
            raise InvalidResponseError(error_msg)

    if not msg.header:
        msg_text = "response should have headers"
        raise InvalidResponseError(msg_text)

    num_pending = msg.header.get("Nats-Num-Pending")
    if num_pending is None:
        msg_text = "batch get not supported by server"
        raise BatchUnsupportedError(msg_text)

    stream = msg.header.get("Nats-Stream")
    if stream is None:
        msg_text = "missing stream header"
        raise InvalidResponseError(msg_text)

    seq_str = msg.header.get("Nats-Sequence")
    if seq_str is None:
        msg_text = "missing sequence header"
        raise InvalidResponseError(msg_text)

    try:
        seq = int(seq_str)
    except ValueError as e:
        msg_text = f"invalid sequence header '{seq_str}': {e}"
        raise InvalidResponseError(msg_text) from e

    time_str = msg.header.get("Nats-Time-Stamp")
    if time_str is None:
        msg_text = "missing timestamp header"
        raise InvalidResponseError(msg_text)

    try:
        # Parse RFC3339Nano format - handle nanosecond precision by truncating to microseconds
        iso_time = time_str.replace("Z", "+00:00")
        # Handle nanosecond precision by truncating to microseconds (6 digits)
        if "." in iso_time and "+" in iso_time:
            before_dot = iso_time.split(".")[0]
            after_dot_full = iso_time.split(".")[1]
            timezone_part = "+" + after_dot_full.split("+")[1]
            fractional_part = after_dot_full.split("+")[0]
            # Truncate to 6 digits (microseconds)
            if len(fractional_part) > 6:
                fractional_part = fractional_part[:6]
            iso_time = f"{before_dot}.{fractional_part}{timezone_part}"
        datetime.fromisoformat(iso_time)
    except ValueError as e:
        msg_text = f"invalid timestamp header '{time_str}': {e}"
        raise InvalidResponseError(msg_text) from e

    subj = msg.header.get("Nats-Subject")
    if subj is None:
        msg_text = "missing subject header"
        raise InvalidResponseError(msg_text)

    return api.RawStreamMsg(
        subject=subj,
        seq=seq,
        data=msg.data,
        headers=msg.header,
        stream=stream,
        # TODO: Add time field when available in nats-py
    )


def _get_prefixed_subject(js: JetStreamContext, subject: str) -> str:
    """Get prefixed subject based on JetStream context configuration."""
    # Access the prefix from the JetStream context
    # In nats-py, the prefix is stored in the _prefix attribute
    prefix = getattr(js, "_prefix", "$JS.API")

    if not prefix.endswith("."):
        prefix += "."

    return f"{prefix}{subject}"
