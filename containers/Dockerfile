# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.13
ARG DEBIAN_RELEASE=trixie
ARG WHEELS=compiled

FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS compiled

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /usr/local/bin/uv

WORKDIR /src
COPY pyproject.toml README.md LICENSE hatch_build_core.py ./
COPY packages/netaudio-core packages/netaudio-core
COPY packages/netaudio/src packages/netaudio/src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/packages/netaudio-core/target \
    uv build --wheel --out-dir /dist


FROM scratch AS prebuilt
COPY dist/ /dist/


FROM ${WHEELS} AS wheels


FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS runtime

ARG EXTRAS=""
RUN --mount=type=bind,from=wheels,source=/dist,target=/dist \
    pip install --no-cache-dir --no-index --no-deps --find-links /dist netaudio \
    && pip install --no-cache-dir "netaudio${EXTRAS:+[${EXTRAS}]}"

RUN groupadd --system --gid 10001 netaudio \
    && useradd --system --uid 10001 --gid 10001 --home-dir /var/lib/netaudio --no-create-home netaudio \
    && install -d -o netaudio -g netaudio -m 0750 /var/lib/netaudio


FROM scratch

COPY --from=runtime / /

ENV PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin \
    LANG=C.UTF-8 \
    HOME=/var/lib/netaudio \
    NETAUDIO_CONFIG=/var/lib/netaudio/config.toml \
    PYTHONUNBUFFERED=1

USER netaudio
WORKDIR /var/lib/netaudio
VOLUME ["/var/lib/netaudio"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import urllib.request; from netaudio.common.app_config import configured_daemon_port; urllib.request.urlopen(f'http://127.0.0.1:{configured_daemon_port()}/server-info', timeout=4)"]

LABEL org.opencontainers.image.title="netaudio" \
      org.opencontainers.image.description="Dante network audio daemon, web interface and MCP server" \
      org.opencontainers.image.source="https://github.com/chris-ritsen/network-audio-controller" \
      org.opencontainers.image.licenses="Unlicense"

ENTRYPOINT ["netaudio"]
CMD ["daemon", "run"]
