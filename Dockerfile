FROM python:3.13-slim AS builder


COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_PYTHON=python3.13 \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app \
    UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y build-essential

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync \
    --locked \
    --no-dev \
    --no-install-project

COPY . /src
WORKDIR /src

RUN --mount=type=cache,target=/root/.cache \
    uv sync \
        --locked \
        --no-dev \
        --no-editable

FROM python:3.13-slim
COPY --from=builder /app /app

ENV USER=ci_relay
RUN adduser --gecos "" --disabled-password $USER

WORKDIR /app

ENV PATH=/home/$USER/.local/bin:$PATH
ENV PATH="/app/bin:$PATH"

USER $USER
CMD ["uvicorn", "herald.web:create_app", "--factory", "--port", "5000", "--host", "0.0.0.0"]
