# Checks that should pass

- `uv run ruff format .`
- `uv run ruff check --fix .`
- `uv run ty check .`
- `uv run basedpyright .`
- `uv run pytest .`
- `uv run --python=3.10 pytest .`
