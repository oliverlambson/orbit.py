# Contributing

## Environment

The project is managed using [`uv`](https://docs.astral.sh/uv/).

## Checks that should pass

- `uv run ruff format .`  # formatting
- `uv run ruff check --fix .`  # linting
- `uv run ty check .`  # type checking
- `uv run pytest .`  # test on lastest python version
- `uv run --python=3.10 pytest .`  # test on oldest supported python version
