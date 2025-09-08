# Core NATS Extensions

Core NATS Extensions is a set of utilities providing additional features to Core NATS component of nats-py client.

## Installation

```bash
uv add natsext
```

## Utilities

> see [examples.py](https://www.github.com/oliverlambson/orbit.py/blob/main/natsext/examples.py) for a runnable version of all snippets below.

### request_many

`request_many` is a utility that allows you to send a single request and await multiple responses.
This allows you to implement various patterns like scatter-gather or streaming responses.

Responses are returned in an async iterator, which you can iterate over to receive messages.
When a termination condition is met, the iterator is closed (and no error is returned).

```py
import natsext

# Basic usage
async for msg in natsext.request_many(nc, "subject", b"request data"):
    print(msg.data)
```

Alternatively, use `request_many_msg` to send a `nats.Msg` request:

```py
import nats
import natsext

msg = nats.Msg(
    _client=nc,
    subject="subject",
    data=b"request data",
    headers={
        "Key": "Value",
    },
)
async for response in natsext.request_many_msg(nc, msg):
    print(response.data)
```

#### Configuration

You can configure the following options:

- `timeout`: Overall timeout for the request operation (float, seconds)
- `stall`: Stall timer, useful in scatter-gather scenarios where subsequent responses are expected within a certain timeframe (float, seconds)
- `max_messages`: Maximum number of messages to receive (int)
- `sentinel`: Function that stops returning responses once it returns True for a message (Callable[[Msg], bool])

```py
# With all options
async for msg in natsext.request_many(
    nc,
    "subject",
    b"request data",
    timeout=5.0,
    stall=0.1,
    max_messages=10,
    sentinel=natsext.default_sentinel  # Stops on empty message
):
    print(msg.data)
```

#### Default Sentinel

The package includes a `default_sentinel` function that stops receiving messages once a message with an empty payload is received:

```py
import natsext

async for msg in natsext.request_many(
    nc, "subject", b"request", sentinel=natsext.default_sentinel
):
    print(msg.data)
```
