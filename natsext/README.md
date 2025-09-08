> [!IMPORTANT]
>
> The codeblocks are a naive translation of the go codeblocks from orbit.go.
> The client still has to be implemented so the api here is likely not how
> things will actually look.

# Core NATS Extensions

Core NATS Extensions is a set of utilities providing additional features to Core NATS component of nats-py client.

## Installation

```bash
uv add natsext
```

## Utilities

### request_many

`request_many` is a utility that allows you to send a single request and await multiple responses.
This allows you to implement various patterns like scatter-gather or streaming responses.

Responses are returned in an iterator, which you can range over to receive messages.
When a termination condition is met, the iterator is closed (and no error is returned).

```py
msgs = natsext.request_many(ctx, nc, "subject", b"request data")
for msg in msgs:
    print(msg.data)
```

Alternatively, use `request_many_msg` to send a `nats.Msg` request:

```py
msg = nats.Msg(
    subject="subject",
    data=b"request data"),
    header={
        "Key": "Value",
    },
)
iter = natsext.request_many_msg(ctx, nc, msg)
# gather responses
```

#### Configuration

Timeout and cancellation are handled by the context passed to `request_many` and `request_many_msg`. In addition, you can configure the following options:

- `request_many_stall`: Sets the stall timer, useful in scatter-gather scenarios where subsequent responses are expected within a certain timeframe.
- `request_many_max_messages`: Sets the maximum number of messages to receive.
- `request_many_sentinel`: Stops receiving messages once a sentinel message is received.
