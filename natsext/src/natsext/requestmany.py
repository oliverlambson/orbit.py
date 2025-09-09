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

import time
from collections.abc import AsyncIterator, Callable

from nats import NATS
from nats.aio.msg import Msg
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NatsTimeoutError


def default_sentinel(msg: Msg) -> bool:
    """Default sentinel function that stops receiving messages once a message with an empty payload is received."""
    return len(msg.data) == 0


def request_many(
    nc: NATS,
    subject: str,
    data: bytes = b"",
    *,
    stall: float | None = None,
    max_messages: int | None = None,
    sentinel: Callable[[Msg], bool] | None = None,
    timeout: float | None = None,
) -> AsyncIterator[Msg]:
    """Send a request payload and return an async iterator to receive multiple responses.

    If timeout is not set, the number of messages received is constrained by the client's timeout.

    Args:
        nc: NATS client connection
        subject: Subject to send the request to
        data: Request payload data
        stall: Stall timer for scatter-gather scenarios where subsequent responses
               are expected to arrive within a certain time frame
        max_messages: Maximum number of messages to receive
        sentinel: Function that stops returning responses once it returns True for a message
        timeout: Overall timeout for the request operation

    Yields:
        Msg: Response messages from the request

    Raises:
        ValueError: If stall time or max_messages is invalid
        TimeoutError: If no response is received within the timeout period
        Error: For other NATS-related errors
    """
    # Validate options immediately
    if stall is not None and stall <= 0:
        msg = "stall time has to be greater than 0"
        raise ValueError(msg)

    if max_messages is not None and max_messages <= 0:
        msg = "expected request count has to be greater than 0"
        raise ValueError(msg)

    return _request_many(
        nc, subject, data, None, stall, max_messages, sentinel, timeout
    )


def request_many_msg(
    nc: NATS,
    msg: Msg,
    *,
    stall: float | None = None,
    max_messages: int | None = None,
    sentinel: Callable[[Msg], bool] | None = None,
    timeout: float | None = None,
) -> AsyncIterator[Msg]:
    """Send a Msg request and return an async iterator to receive multiple responses.

    If timeout is not set, the number of messages received is constrained by the client's timeout.

    Args:
        nc: NATS client connection
        msg: Message to send as request
        stall: Stall timer for scatter-gather scenarios where subsequent responses
               are expected to arrive within a certain time frame
        max_messages: Maximum number of messages to receive
        sentinel: Function that stops returning responses once it returns True for a message
        timeout: Overall timeout for the request operation

    Yields:
        Msg: Response messages from the request

    Raises:
        ValueError: If stall time/max_messages is invalid
        TimeoutError: If no response is received within the timeout period
        Error: For other NATS-related errors
    """

    # Validate options immediately
    if stall is not None and stall <= 0:
        error_msg = "stall time has to be greater than 0"
        raise ValueError(error_msg)

    if max_messages is not None and max_messages <= 0:
        error_msg = "expected request count has to be greater than 0"
        raise ValueError(error_msg)

    return _request_many(
        nc, msg.subject, msg.data, msg.header, stall, max_messages, sentinel, timeout
    )


async def _request_many(
    nc: NATS,
    subject: str,
    data: bytes,
    headers: dict | None = None,
    stall: float | None = None,
    max_messages: int | None = None,
    sentinel: Callable[[Msg], bool] | None = None,
    timeout: float | None = None,
) -> AsyncIterator[Msg]:
    """Internal implementation of request_many functionality."""

    # Create unique inbox for responses
    inbox = nc.new_inbox()

    # Subscribe to the inbox
    sub = await nc.subscribe(inbox)

    try:
        # Publish the request message
        await nc.publish(subject, data, reply=inbox, headers=headers)

        count = max_messages or -1
        first = True
        end_time = time.time() + timeout if timeout else None

        while True:
            # Check overall timeout
            if end_time and time.time() >= end_time:
                break

            # Determine timeout for this iteration
            msg_timeout = None
            if not first and stall is not None:
                msg_timeout = stall
            elif end_time:
                # Use remaining time until overall timeout
                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                msg_timeout = remaining
            first = False

            try:
                # Get next message with appropriate timeout
                if msg_timeout is not None:
                    msg = await sub.next_msg(timeout=msg_timeout)
                else:
                    msg = await sub.next_msg()

            except NatsTimeoutError:
                # Stall timeout or subscription timeout - stop iteration
                break
            except Exception:  # noqa: BLE001
                # Other errors (like connection closed) - stop iteration
                break

            # Check for "no responders" status message from server
            if msg.headers and msg.headers.get("Status") == "503":
                error_msg = "No responders available for request"
                raise NoRespondersError(error_msg)

            # Check sentinel condition
            if sentinel is not None and sentinel(msg):
                break

            # Yield the message
            yield msg

            # Check max message count
            if count > 0:
                count -= 1
                if count <= 0:
                    break

    finally:
        # Clean up subscription
        try:
            await sub.unsubscribe()
        except Exception:  # noqa: BLE001, S110
            pass  # Ignore cleanup errors
