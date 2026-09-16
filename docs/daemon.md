# NetAudio daemon and managed configuration

The NetAudio daemon maintains device inventory, monitoring state, event history,
and control services for the browser interface and HTTP API. Clients receive
live updates through server-sent events (SSE).

Start the daemon and open its browser interface:

```bash
netaudio daemon start
netaudio daemon web --open
```

Use `netaudio daemon run` to run in the foreground, or
`netaudio daemon install --help` for boot-service installation options.

The server reports its installed version at `GET /server-info` and during
local-network discovery. A Git revision is included when the installation
metadata identifies a Git commit.

## HTTPS

The daemon serves plain HTTP by default. If you already have a certificate for
the machine, the daemon can additionally serve HTTPS on a second port; nothing
is generated or managed for you:

```toml
[daemon]
tls_certificate = "/etc/netaudio/daemon.crt"
tls_key = "/etc/netaudio/daemon.key"
tls_port = 9443
```

Relative paths resolve against the config file's directory, `tls_port`
defaults to 9443, and the HTTP port keeps working for local tools. `netaudio
daemon tls` shows the active configuration and the certificate's SHA-256
fingerprint; `netaudio daemon web` lists the HTTPS addresses alongside HTTP.
The Bonjour record carries the HTTPS port as `tls_port`. Clients trust the
certificate through their normal trust store, so a certificate signed by a CA
your devices already trust is the least friction.

## Managed operation permissions

Writes to an enrolled device fail closed until its saved DDM context explicitly
permits each operation. Add `operation_permissions` to the context in the
NetAudio configuration file:

```toml
[ddm.contexts.studio]
server = "manager"
domain_id = "0123456789abcdef0123456789abcdef"
operation_permissions = ["identify", "sample_rate", "encoding"]
```

Accepted operation names are `identify`, `sample_rate`, `encoding`,
`sample_rate_pullup`, `aes67`, `static_ipv4`, `redundancy`, `codec_control`, and
`locking`. Omitting the setting means permission has not been configured; an
empty array explicitly permits no operations. Device capability, current
configuration state, lock state, and transport support can still prevent a
listed operation. Managed lock and unlock remain unavailable because NetAudio
has no verified DDM transport for them.
