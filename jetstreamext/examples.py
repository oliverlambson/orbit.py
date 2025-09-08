"""jetstreamext example usage.

1. Ensure a local nats server is running on port 4222.
2. run `uv run examples.py`
"""

import asyncio
from datetime import datetime, timedelta, timezone

import nats
from nats.js import JetStreamContext
from nats.js.api import StreamConfig

import jetstreamext


async def main() -> None:
    nc = await nats.connect()
    js = nc.jetstream()

    # Set up stream and test data
    print("Setting up stream and test data...")
    await _setup_stream_and_data(js)

    try:
        # Basic batch fetching
        print("\n=== Basic get_batch usage ===")
        async for msg in jetstreamext.get_batch(js, "mystream", batch=5):
            print(f"Received: {msg.data}")

        # Batch fetching with sequence start
        print("\n=== get_batch with sequence start ===")
        async for msg in jetstreamext.get_batch(
            js, "mystream", batch=5, seq=50, subject="foo"
        ):
            print(f"Received: {msg.data}")

        # Batch fetching with time start
        print("\n=== get_batch with time start ===")
        async for msg in jetstreamext.get_batch(
            js,
            "mystream",
            batch=5,
            start_time=datetime.now(timezone.utc) - timedelta(hours=1),
        ):
            print(f"Received: {msg.data}")

        # Batch fetching with byte limit
        print("\n=== get_batch with byte limit ===")
        async for msg in jetstreamext.get_batch(
            js, "mystream", batch=10, max_bytes=1024
        ):
            print(f"Received: {msg.data}")

        # Getting last messages for subjects
        print("\n=== get_last_msgs_for basic usage ===")
        async for msg in jetstreamext.get_last_msgs_for(js, "mystream", ["foo", "bar"]):
            print(f"Received: {msg.data}")

        # Getting last messages up to sequence
        print("\n=== get_last_msgs_for with sequence limit ===")
        async for msg in jetstreamext.get_last_msgs_for(
            js, "mystream", ["foo", "bar"], up_to_seq=100
        ):
            print(f"Received: {msg.data}")

        # Getting last messages up to time
        print("\n=== get_last_msgs_for with time limit ===")
        async for msg in jetstreamext.get_last_msgs_for(
            js,
            "mystream",
            ["foo", "bar"],
            up_to_time=datetime.now(timezone.utc) - timedelta(hours=1),
        ):
            print(f"Received: {msg.data}")

        # Getting last messages with batch size
        print("\n=== get_last_msgs_for with batch size ===")
        async for msg in jetstreamext.get_last_msgs_for(
            js, "mystream", ["foo.*"], batch=10
        ):
            print(f"Received: {msg.data}")

    finally:
        # Clean up
        print("\nCleaning up...")
        _ = await js.delete_stream("mystream")
        await nc.close()


async def _setup_stream_and_data(js: JetStreamContext) -> None:
    """Set up the test stream and publish sample data"""
    # Clean up any existing stream
    try:
        _ = await js.delete_stream("mystream")
    except Exception:  # noqa: BLE001, S110
        pass  # Stream might not exist

    # Create stream
    _ = await js.add_stream(
        StreamConfig(
            name="mystream",
            subjects=["foo", "bar"],
            allow_direct=True,  # IMPORTANT: can't get direct without this (disabled by default)
        )
    )

    # Publish test messages
    for i in range(100):
        _ = await js.publish("foo", f"foo-{i}".encode(), stream="mystream")
        _ = await js.publish("bar", f"bar-{i}".encode(), stream="mystream")


if __name__ == "__main__":
    asyncio.run(main())
