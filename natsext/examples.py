"""natsext example usage.

1. Ensure a local nats server is running on port 4222.
2. run `uv run examples.py`
"""

import asyncio

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

import natsext


async def main() -> None:
    nc = await nats.connect()

    # Set up responders
    print("Setting up responders...")
    subs = await _setup_responders(nc)

    try:
        # Basic usage
        print("\n=== Basic request_many usage ===")
        async for msg in natsext.request_many(nc, "subject", b"request data"):
            print(f"Received: {msg.data}")

        # Using request_many_msg to send a nats.Msg request
        print("\n=== request_many_msg usage ===")
        msg = Msg(
            nc,
            subject="subject",
            data=b"request data",
            headers={
                "Key": "Value",
            },
        )
        async for response in natsext.request_many_msg(nc, msg):
            print(f"Received: {response.data}")

        # With all configuration options (limiting to 3 messages)
        print("\n=== request_many with all options ===")
        async for msg in natsext.request_many(
            nc,
            "subject",
            b"request data",
            timeout=5.0,
            stall=0.1,
            max_messages=3,
            sentinel=None,  # Don't use sentinel here to show max_messages working
        ):
            print(f"Received: {msg.data}")

        # Using default sentinel
        print("\n=== request_many with default_sentinel ===")
        async for msg in natsext.request_many(
            nc, "subject", b"request", sentinel=natsext.default_sentinel
        ):
            print(f"Received: {msg.data}")

    finally:
        # Clean up subscriptions
        print("\nCleaning up...")
        for sub in subs:
            await sub.unsubscribe()
        await nc.close()


async def _setup_responders(nc: NATS) -> list[Subscription]:
    """Set up background responders for the examples"""

    # Basic responder for "subject"
    async def basic_responder(msg: Msg) -> None:
        await nc.publish(msg.reply, b"Hello from basic responder!")

    # Multiple responders for scatter-gather pattern
    async def responder_1(msg: Msg) -> None:
        await asyncio.sleep(0.01)  # Small delay
        await nc.publish(msg.reply, b"Response from service 1")

    async def responder_2(msg: Msg) -> None:
        await asyncio.sleep(0.02)  # Slightly longer delay
        await nc.publish(msg.reply, b"Response from service 2")

    async def responder_3(msg: Msg) -> None:
        await asyncio.sleep(0.03)  # Even longer delay
        await nc.publish(msg.reply, b"Response from service 3")

    # Sentinel responder (sends empty message after delay)
    async def sentinel_responder(msg: Msg) -> None:
        await asyncio.sleep(0.1)  # Longer delay
        await nc.publish(msg.reply, b"")  # Empty message triggers sentinel

    # Subscribe to subjects
    subs = [
        await nc.subscribe("subject", cb=basic_responder),
        await nc.subscribe("subject", cb=responder_1),
        await nc.subscribe("subject", cb=responder_2),
        await nc.subscribe("subject", cb=responder_3),
        await nc.subscribe("subject", cb=sentinel_responder),
    ]

    return subs


if __name__ == "__main__":
    asyncio.run(main())
