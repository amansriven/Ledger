FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1

# Dependencies first, so code changes don't reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --group bench --group examples --no-install-project

COPY ledger ledger
COPY examples examples
COPY bench bench
COPY chaos/*.py chaos/
RUN uv sync --frozen --no-dev --group bench --group examples

ENV PATH=/app/.venv/bin:$PATH
