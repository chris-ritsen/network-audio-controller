# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.13
ARG DEBIAN_RELEASE=trixie
ARG WHEELS=compiled

FROM --platform=$BUILDPLATFORM ghcr.io/astral-sh/uv:0.12 AS uv


FROM --platform=$BUILDPLATFORM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS toolchain

ARG BUILDARCH
RUN case "$BUILDARCH" in \
        amd64) cross="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
        arm64) cross="gcc-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
        *) echo "cannot build on $BUILDARCH" >&2; exit 1 ;; \
    esac \
    && apt-get update \
    && apt-get install --yes --no-install-recommends gcc libc6-dev $cross ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
RUN curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path \
        --target x86_64-unknown-linux-gnu --target aarch64-unknown-linux-gnu

COPY --from=uv /uv /usr/local/bin/uv


FROM toolchain AS compiled

WORKDIR /src
COPY pyproject.toml README.md LICENSE hatch_build_core.py ./
COPY packages/netaudio-core packages/netaudio-core
COPY packages/netaudio/src packages/netaudio/src

ARG TARGETARCH
ARG REVISION=""
RUN --mount=type=cache,id=cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=netaudio-core-target-${TARGETARCH},target=/src/packages/netaudio-core/target \
    case "$TARGETARCH" in \
        amd64) target=x86_64-unknown-linux-gnu ;; \
        arm64) target=aarch64-unknown-linux-gnu ;; \
        *) echo "cannot build for $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && CARGO_BUILD_TARGET="$target" NETAUDIO_GIT_REVISION="$REVISION" uv build --wheel --out-dir /dist


FROM scratch AS prebuilt
COPY dist/ /dist/


FROM ${WHEELS} AS wheels


FROM --platform=$BUILDPLATFORM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS installed

COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /lock
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project --no-dev --all-extras --no-hashes --format requirements-txt \
        --output-file /lock/constraints.txt

ARG TARGETARCH
ARG EXTRAS=""
RUN --mount=type=bind,from=wheels,source=/dist,target=/dist \
    --mount=type=cache,target=/root/.cache/uv \
    case "$TARGETARCH" in \
        amd64) machine=x86_64 elf=62 ;; \
        arm64) machine=aarch64 elf=183 ;; \
        *) echo "cannot build for $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && set -- /dist/netaudio-*_"$machine".whl \
    && if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then echo "expected one $machine netaudio wheel in /dist" >&2; exit 1; fi \
    && uv pip install --python /usr/local/bin/python3 --python-platform "$machine-manylinux_2_40" \
        --prefix /installed/usr/local --compile-bytecode --link-mode copy --constraints /lock/constraints.txt \
        "netaudio${EXTRAS:+[${EXTRAS}]} @ file://$1" \
    && python3 -c 'import pathlib, sys; expected = int(sys.argv[1]); headers = {path: path.open("rb").read(20) for path in pathlib.Path("/installed").rglob("*") if path.is_file() and not path.is_symlink()}; foreign = [str(path) for path, header in headers.items() if header[:4] == b"\x7fELF" and int.from_bytes(header[18:20], "little") != expected]; sys.exit("built for another architecture: " + ", ".join(foreign) if foreign else 0)' "$elf"


FROM --platform=$BUILDPLATFORM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS accounts

RUN groupadd --system --gid 10001 netaudio \
    && useradd --system --uid 10001 --gid 10001 --home-dir /var/lib/netaudio --no-create-home netaudio \
    && install -d -o netaudio -g netaudio -m 0750 /accounts/var/lib/netaudio \
    && install -d /accounts/etc \
    && cp -p /etc/passwd /etc/group /etc/shadow /etc/gshadow /accounts/etc/


FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS runtime

COPY --from=accounts /accounts/ /
COPY --from=installed /installed/ /


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

ARG VERSION=""
ARG REVISION=""
LABEL org.opencontainers.image.title="netaudio" \
      org.opencontainers.image.description="Dante network audio daemon, web interface and MCP server" \
      org.opencontainers.image.source="https://github.com/chris-ritsen/network-audio-controller" \
      org.opencontainers.image.licenses="Unlicense" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

ENTRYPOINT ["netaudio"]
CMD ["daemon", "run"]
