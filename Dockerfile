# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.12
ARG DEBIAN_RELEASE=bookworm

FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /src
COPY pyproject.toml README.md LICENSE hatch_build_core.py ./
COPY packages/netaudio-core packages/netaudio-core
COPY packages/netaudio/src packages/netaudio/src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/packages/netaudio-core/target \
    uv build --wheel --out-dir /dist


FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS runtime

RUN --mount=type=bind,from=build,source=/dist,target=/dist \
    pip install --no-cache-dir /dist/netaudio-*.whl

RUN useradd --system --create-home --home-dir /data --uid 10001 netaudio \
    && install -d -o netaudio -g netaudio -m 0700 /data/config

ENV HOME=/data \
    NETAUDIO_CONFIG=/data/config/config.toml \
    PYTHONUNBUFFERED=1

USER netaudio
WORKDIR /data
VOLUME ["/data"]

EXPOSE 9443

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/server-info' % os.environ.get('NETAUDIO_DAEMON_PORT', '9000'), timeout=4)"

ENTRYPOINT ["netaudio"]
CMD ["daemon", "run"]
