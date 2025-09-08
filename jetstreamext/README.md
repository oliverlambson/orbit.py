
# NATS JetStream Extensions

JetStream Extensions is a set of utilities providing additional features to `jetstream` package in nats-py client.

## Installation

```bash
uv add jetstreamext
```

## Utilities

### get_batch and get_last_msgs_for

`get_batch` and `get_last_msgs_for` are utilities that allow you to fetch multiple messages from a JetStream stream.
Responses are returned as async iterators, which you can iterate over using `async for` to receive messages.

#### get_batch

`get_batch` fetches a `batch` of messages from a provided stream, starting from
either the lowest matching sequence, from the provided sequence, or from the
given time. It can be configured to fetch messages from matching subject (which
may contain wildcards) and up to a maximum byte limit.

Examples:

- fetching 10 messages from the beginning of the stream:

```py
async for msg in jetstreamext.get_batch(js, "stream", 10):
    print(msg.data)
```

- fetching 10 messages from the stream starting from sequence 100 and matching subject:

```py
async for msg in jetstreamext.get_batch(js, "stream", 10, jetstreamext.get_batch_seq(100), jetstreamext.get_batch_subject("foo")):
    print(msg.data)
```

- fetching 10 messages from the stream starting from time 1 hour ago:

```py
async for msg in jetstreamext.get_batch(js, "stream", 10, jetstreamext.get_batch_start_time(datetime.now() - timedelta(hours=1))):
    print(msg.data)
```

- fetching 10 messages or up to provided byte limit:

```py
async for msg in jetstreamext.get_batch(js, "stream", 10, jetstreamext.get_batch_max_bytes(1024)):
    print(msg.data)
```

#### get_last_msgs_for

`get_last_msgs_for` fetches the last messages for the specified subjects from the specified stream. It can be optionally configured to fetch messages up to the provided sequence (or time), rather than the latest messages available. It can also be configured to fetch messages up to a provided batch size.
The provided subjects may contain wildcards, however it is important to note that the NATS server will match a maximum of 1024 subjects.

Responses are returned as async iterators, which you can iterate over using `async for` to receive messages.

Examples:

- fetching last messages from the stream for the provided subjects:

```py
async for msg in jetstreamext.get_last_msgs_for(js, "stream", ["foo", "bar"]):
    print(msg.data)
```

- fetching last messages from the stream for the provided subjects up to stream sequence 100:

```py
async for msg in jetstreamext.get_last_msgs_for(js, "stream", ["foo", "bar"], jetstreamext.get_last_msgs_up_to_seq(100)):
    print(msg.data)
```

- fetching last messages from the stream for the provided subjects up to time 1 hour ago:

```py
async for msg in jetstreamext.get_last_msgs_for(js, "stream", ["foo", "bar"], jetstreamext.get_last_msgs_up_to_time(datetime.now() - timedelta(hours=1))):
    print(msg.data)
```

- fetching last messages from the stream for the provided subjects up to a batch size of 10:

```py
async for msg in jetstreamext.get_last_msgs_for(js, "stream", ["foo.*"], jetstreamext.get_last_msgs_batch_size(10)):
    print(msg.data)
```
