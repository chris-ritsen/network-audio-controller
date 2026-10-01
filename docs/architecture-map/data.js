// Nodes, edges, journeys and surprises shown on the map. Edit this file to change the content.

const ZONES = [
  { id: "client", name: "Clients", sub: "who asks for things", x: -30, y: -40, w: 300, h: 1300, c: "--z-client" },
  { id: "http", name: "Daemon edge", sub: "daemon/http/*  ·  :9000 / :9443", x: 350, y: -40, w: 300, h: 1070, c: "--z-http" },
  { id: "daemon", name: "Daemon internals", sub: "one asyncio process · daemon/server.py", x: 730, y: -40, w: 620, h: 1540, c: "--z-daemon" },
  { id: "core", name: "Rust core", sub: "packages/netaudio-core · C ABI", x: 1430, y: 110, w: 300, h: 1210, c: "--z-core" },
  { id: "net", name: "On the wire", sub: "UDP / multicast / TCP", x: 1810, y: -40, w: 300, h: 1480, c: "--z-net" },
  { id: "store", name: "On disk", sub: "everything else lives in memory", x: 350, y: 1600, w: 1000, h: 330, c: "--z-store" },
  { id: "research", name: "Research tooling", sub: "protocol evidence, not normal control", x: 1430, y: 1600, w: 680, h: 330, c: "--z-research" },
];

const N = {};
function node(id, zone, x, y, t, s, chips, files, body, note) { N[id] = { id, zone, x, y, t, s, chips: chips || [], files: files || [], body, note }; }

// Clients
node("web", "client", -10, 0, "Web UI", "Preact + htm + signals, no build step", ["EventSource /events", "fetch()"],
  ["daemon/http/webapp/store.js", "webapp/api.js", "webapp/matrix.js", "webapp/route-picker.js", "webapp/inventory-cache.js"],
  "Served by the daemon straight from daemon/http/webapp. It opens one EventSource on /events for live state and calls fetch() for every action. It caches the last inventory in localStorage (netaudio.inventory.v1) so the page paints before the snapshot arrives. Views: devices, routing, subscriptions, presets, settings, events, DDM, Shure.");
node("cli", "client", -10, 210, "CLI · netaudio", "builds its own DanteApplication every run", ["GET /devices", "--json"],
  ["commands/subscription.py", "cli_support/execution.py", "daemon/client.py", "dante/subscription_operations.py"],
  "Each command builds a local DanteApplication. It first asks the daemon for GET /devices on 127.0.0.1:9000. If the daemon answers, the CLI borrows that inventory. If not, it runs its own one-shot mDNS discovery. Either way, writes are sent from the CLI process straight to the device.",
  "cli-direct");
node("ios", "client", -10, 430, "iOS / iPadOS app", "native client, not in this repo", ["direct", "via daemon", "DDM"],
  ["packages/netaudio-core/scripts/build_xcframework.sh", "packages/netaudio-core/include/module.modulemap"],
  "Links the same Rust core as an XCFramework (Swift module netaudio_core). It can control devices directly from the phone, or find a daemon through the _netaudio-relay._tcp Bonjour advert and use its HTTPS API on 9443.");
node("mcpc", "client", -10, 640, "AI agent", "any MCP client", ["bearer token", "OAuth + PKCE"],
  [], "Connects to /mcp with either the static bearer token (preference mcp_token) or an OAuth grant. Write tools refuse to run until called again with confirmed=true.");
node("dbusc", "client", -10, 830, "DBus consumers", "desktop widgets, scripts", ["com.netaudio.Daemon"],
  [], "Anything on the session bus can read device and channel objects under /com/netaudio and listen for DanteDeviceAdded / Removed.");
node("redisc", "client", -10, 990, "Redis subscribers", "optional integration", ["netaudio:daemon:*", "netaudio:shure:*"],
  [], "When REDIS_HOST or REDIS_SOCKET is set, the daemon mirrors devices, DDM state and Shure meters into Redis hashes.");

// Daemon edge
node("http", "http", 370, 0, "HTTP server", "hand-written HTTP/1.1 on asyncio", [":9000 loopback", ":9443 TLS"],
  ["daemon/http/api.py", "daemon/http/web.py"],
  "Not aiohttp: a small parser on asyncio.start_server, Connection: close on every response. Port 9000 binds to 127.0.0.1 while TLS is on (0.0.0.0 with no_ssl). Dispatch order: SSE, /mcp, OAuth, the GET table, the POST table, then static web assets.");
node("handlers", "http", 370, 175, "POST handlers", "devices · configuration · presets · managed", ["/subscribe", "/subscriptions/apply", "/presets/load"],
  ["daemon/http/devices.py", "daemon/http/configuration.py", "daemon/http/presets.py", "daemon/http/managed.py", "daemon/http/connections.py", "daemon/http/settings.py"],
  "One table maps each POST path to a handler. devices.py holds routing, rename, latency, lock, sample rate and metering. configuration.py holds flows, gain, AES67, clock, reboot. presets.py holds save, preview and load. managed.py and connections.py hold DDM.",
  "early-ok");
node("sse", "http", 370, 360, "SSE /events", "snapshot first, then deltas", ["device_updated", "meter_values", "monitoring_event"],
  ["daemon/http/api.py", "daemon/http/sse_view.py"],
  "The first message is a snapshot: devices, external flows, Shure devices, metering, DDM. After that it streams device_discovered / updated / removed, meter_values, subscription_pending, monitoring_event and more. Each client has a 128-message queue. ?patches=1 sends per-device diffs; ?telemetry=0 strips clock and traffic fields.");
node("bonjour", "http", 370, 545, "Bonjour advert", "so the iOS app can find the daemon", ["_netaudio-relay._tcp"],
  ["daemon/http/api.py"],
  "Advertises netaudio-daemon (<host>) on 9443 (9000 without TLS). TXT: version, git_revision, mcp_path=/mcp, tls_port, scheme. Re-sent every 60 s and when the host address changes or wakes from sleep.");
node("mcp", "http", 370, 700, "MCP /mcp", "JSON-RPC, protocol 2025-06-18", ["netaudio:read", "netaudio:write", "confirmed=true"],
  ["daemon/http/mcp.py", "daemon/http/mcp_views.py", "daemon/http/oauth.py", "daemon/mcp_oauth.py", "daemon/mcp_access.py"],
  "Every tool is a thin wrapper over an internal HTTP path. set_subscriptions calls /subscriptions/apply, get_signal_levels calls /metering/cache, and so on. The call goes through the same dispatcher in-process, then mcp_views compacts the result for the model.",
  "mcp-mismatch");
node("tls", "http", 370, 880, "TLS identity", "self-signed EC P-256, 365 days", ["tls/daemon.pem"],
  ["daemon/http/tls.py"],
  "Generated on first start unless you configure your own certificate. Checked every 5 s; re-issued within 30 days of expiry or when host names or IPv4 addresses change, then hot-reloaded without a restart.");

// Daemon internals, column A
node("batching", "daemon", 750, 0, "Route batching", "parse_routes · plan_batches", ["16 legacy ARC", "32 modern / DDM"],
  ["daemon/subscription_batching.py"],
  "Validates route lists and groups them per receiving device and per action (set or clear), chunked by the device's ARC subscription batch limit. Used by /subscribe and /subscriptions/apply.");
node("app", "daemon", 750, 170, "DanteApplication", "the device inventory and event bus", ["devices{}", "DanteEvent"],
  ["dante/application.py", "dante/events.py", "dante/device.py"],
  "Holds devices keyed by server name. Every write goes through here: add_subscriptions takes the device's topology lock, checks for self-connections, then picks DDM or direct transport. Direct writes wait up to 2 s for the device's own change notification. Emits DEVICE_DISCOVERED / UPDATED / REMOVED for everything downstream.",
  "no-persist");
node("state", "daemon", 750, 360, "DanteStateService", "fills and refreshes device state", ["fetch_device_controls"],
  ["dante/state.py"],
  "Runs the initial ARC reads for a new device, then maps each incoming notification ID (rx/tx channel change, routing, clock, sample rate, reboot…) to a targeted re-fetch. Status that arrives before its device exists is parked and applied later.");
node("readback", "daemon", 750, 540, "Subscription readback", "poll every 0.5 s for 10 s", ["settled?"],
  ["daemon/subscription_readback.py", "dante/subscription_operations.py", "dante/readback.py"],
  "After a daemon write, one task per device re-reads the receiver's channels (or DDM inventory) until the Rust readback says the routes have settled, publishing DEVICE_UPDATED on every pass. The CLI does the same check synchronously instead.");
node("journal", "daemon", 750, 720, "Event journal + issues", "in memory, last 1000 events", ["/event-journal", "/issues"],
  ["monitoring/journal.py", "monitoring/issues.py", "monitoring/model.py", "monitoring/operations.py", "monitoring/signals.py"],
  "observe_device diffs each device snapshot into events (clock changes, subscription failed/recovered, late packets, interface errors…). IssueEngine opens and resolves issues such as subscription_failure or clock_synchronization. MutationAuditRecorder logs configuration operations.",
  "journal-mem");
node("presets", "daemon", 750, 900, "Presets", "XML → plan → apply → read back", ["presets/*.xml", "--dry-run"],
  ["presets/loading.py", "presets/parsing.py", "presets/serialization.py", "presets/schema.py", "daemon/http/presets.py"],
  "Save reads each device fresh and writes Dante-Controller-compatible XML with a netaudio JSON extension (schema v3). Load matches devices by name, server name, MAC or inventory ID, plans every item against a fresh read, applies only changes, and reads each one back.");
node("dbus", "daemon", 750, 1080, "DBus service", "one object per device", ["/com/netaudio"],
  ["daemon/dbus_service.py", "daemon/dbus_interfaces.py", "daemon/dbus_state.py"],
  "Session-bus name com.netaudio.Daemon. Exports Dante and Shure devices and channels as objects and emits property changes from device events.");
node("redis", "daemon", 750, 1230, "Redis publisher", "optional, off by default", ["REDIS_HOST"],
  ["daemon/server.py"], "Writes netaudio:daemon:device:<server_name> hashes on each device event.");
node("loops", "daemon", 750, 1370, "Housekeeping loops", "revalidate 30 s · status 300 s", ["recover known devices"],
  ["daemon/server.py", "daemon/network_cache.py"],
  "Expires stale devices, re-probes offline ones on ARC 4440, verifies quiet online ones and trims the log file. NetworkStatusCache (in memory, 256 entries) restores link speed and interfaces when a device comes back.");

// Daemon internals, column B
node("discovery", "daemon", 1080, 0, "mDNS discovery", "AsyncZeroconf browser", ["resolve 3 s"],
  ["daemon/discovery.py", "dante/discovery.py", "dante/browser.py"],
  "Browses the Dante service types, resolves each instance, creates a DanteDevice keyed <instance>.local. and applies TXT data (model, versions, rate, latency, arcp_vers). Removal starts an offline check rather than deleting immediately.");
node("notif", "daemon", 1080, 170, "Notification listener", "device change announcements", ["224.0.0.231:8702"],
  ["dante/services/notification.py", "dante/services/notification_packet_handlers.py"],
  "Joins the control-monitoring multicast group. Each packet goes through core.parse_response('notification_envelope'). ConMon replies become device status; everything else becomes NOTIFICATION_RECEIVED, which wakes pending write waiters and triggers re-fetches.");
node("heartbeat", "daemon", 1080, 340, "Heartbeat listener", "liveness, clock, traffic", ["224.0.0.233:8708", "offline after 15 s"],
  ["dante/services/heartbeat.py", "dante/heartbeat_connection_health.py"],
  "Every heartbeat marks the device as seen and feeds clock observations, interface traffic, receive-flow health and coarse signal presence. A sweep every 5 s marks silent devices offline.");
node("cmc", "daemon", 1080, 510, "CMC service", "registers this controller with each device", ["cmc_register", "every 10 s"],
  ["dante/services/cmc.py"],
  "Registers the host's MAC with each device's CMC service and re-registers every 10 s. Also sends start / stop metering commands.");
node("metering", "daemon", 1080, 680, "Metering manager", "dBFS levels, client leases", [":8752 → :8753", "50 ms"],
  ["daemon/metering.py", "dante/metering.py"],
  "Clients lease a device's meters by client_id. The manager asks the device to stream, parses the UDP packets to dBFS, keeps them alive every 3 s, drops them after 60 s without a lease, and broadcasts METER_VALUES every 50 ms.",
  "port-8752");
node("clock", "daemon", 1080, 850, "Clock monitor", "probe every 2 s, 8 at a time", ["PTP"],
  ["daemon/clock_monitor.py", "dante/clock_observations.py"], "Polls clocking status so leader changes and sync problems show up in the journal and issues.");
node("sap", "daemon", 1080, 1000, "SAP listener", "AES67 streams", ["SDP", "expiry 3600 s"],
  ["dante/services/sap.py", "dante/sap.py", "dante/sdp.py", "dante/flows.py"],
  "Parses SAP announcements into external flows that receivers can subscribe to with /external-flows/subscribe.");
node("managed", "daemon", 1080, 1150, "DDM inventory", "polls the domain manager", ["refresh_interval"],
  ["daemon/managed_inventory.py", "daemon/managed_controls.py", "daemon/managed_signals.py", "ddm/device_transport.py"],
  "Polls each configured DDM server's GraphQL inventory. Devices enrolled in a domain set requires_managed_control, which reroutes all their writes through DDM.");
node("shure", "daemon", 1080, 1310, "Shure manager", "ARP scan for OUI 00:0e:dd", ["TCP 2202"],
  ["shure/manager.py", "shure/discovery.py", "shure/device.py"],
  "Finds Shure wireless receivers in the neighbour table, polls them with Shure's ASCII protocol, and pairs their channels with Dante devices from [shure.correlations] in the config.");

// Rust core
node("ct", "core", 1450, 160, "CoreTransport", "Python side; caches one client per socket", ["(ip, port, iface…)"],
  ["dante/core_transport.py", "dante/device.py"],
  "DanteDevice.execute ends up here. It keeps one core.CoreClient per (ip, port, timeout, attempts, interface, source IP, generation) and hands the JSON command spec to Rust.");
node("binding", "core", 1450, 320, "ctypes binding", "loads libnetaudio_core, ABI v14", ["JSON in, JSON out"],
  ["core/binding.py", "core/_abi.py (generated)", "core/_requests.py", "core/_types.py", "hatch_build_core.py"],
  "Finds the shared library (NETAUDIO_CORE_LIB first), refuses a mismatched ABI version, and wraps about 130 netaudio_* C functions. Most take a JSON spec and return JSON or bytes in a caller buffer.");
node("planners", "core", 1450, 480, "Planners and readback", "the domain logic lives here", ["reconcile", "plan_subscription_commands"],
  ["src/subscription_reconciliation.rs", "src/spec/subscription_plan.rs", "src/subscription_readback.rs", "src/flow_plan.rs"],
  "Splits requested routes into unchanged / set / clear, pages them into packets, and judges whether a readback has settled. Flow planning, clock, latency and performance configuration live here too.");
node("encode", "core", 1450, 640, "Packet encoders", "commands/*.rs, protocol.rs", ["0x3010 add", "0x3014 remove", "0x3410 modern"],
  ["src/commands/", "src/protocol.rs", "src/spec/command_spec.rs"],
  "Builds ARC, settings and CMC packets. The ARC protocol ID comes from the device's arcp_vers TXT record: legacy devices get 0x3010 pages of 16, modern ones 0x3410 pages of 32.");
node("client", "core", 1450, 810, "UDP client", "send, retry, check the ack", ["client.rs"],
  ["src/client.rs", "src/netif.rs"],
  "A plain std::net::UdpSocket request/response with retries. Each reply is checked with command_acknowledgement; a rejected page stops the sequence.");
node("parser", "core", 1450, 970, "Parsers", "parser.rs, responses/*.rs", ["notifications", "heartbeats", "meters"],
  ["src/parser.rs", "src/responses/", "src/heartbeat.rs", "src/metering.rs"],
  "Decodes every response and multicast packet the Python side receives.");
node("dapi", "core", 1450, 1130, "DAPI framing", "tunnels ARC through DDM", ["dapi.rs"],
  ["src/dapi.rs", "src/managed_session.rs", "ddm/controller.py"],
  "For DDM-enrolled devices, reads are wrapped in DAPI frames and sent over a TLS session to the DDM controller instead of straight to the device.");

// Wire
node("mdns", "net", 1830, 0, "mDNS", "_netaudio-arc / -chan / -cmc / -dbc, _dantevideo", ["224.0.0.251:5353"],
  ["core/_protocols.py"], "Every Dante device advertises its ARC, channel, CMC and DBC services here. ARC's address is treated as the device's IP.");
node("device", "net", 1830, 190, "Dante device", "e.g. stagebox, avio-usb-1", ["ARC 4440", "settings 8700", "CMC 8800"],
  [], "The hardware. ARC on 4440 (secondary 4455) carries channels, names, subscriptions and most settings. 8700 is the settings port; CMC handles controller registration and metering.");
node("mcnotif", "net", 1830, 420, "Change notifications", "multicast from every device", ["224.0.0.231:8702"],
  [], "Devices announce their own changes here (notification 258 = RX channel change, routing changes, clock, sample rate, reboot…).");
node("mchb", "net", 1830, 570, "Heartbeats", "multicast, roughly every second", ["224.0.0.233:8708"],
  [], "Liveness plus clock, interface traffic and receive-flow health.");
node("mcmeter", "net", 1830, 720, "Meter stream", "UDP levels to the controller", [":8752", "8751 = Dante Controller"],
  [], "Once asked over CMC, the device streams levels to the port the daemon registered.");
node("mcsap", "net", 1830, 870, "SAP announcements", "AES67 sessions", ["239.255.255.255:9875"],
  [], "Third-party AES67 senders announce their RTP streams here with SDP bodies.");
node("ddmsrv", "net", 1830, 1060, "Dante Domain Manager", "Audinate's central server", ["/graphql", "DAPI :8443"],
  ["ddm/client.py", "ddm/queries.py", "ddm/schema.py"],
  "Enrolled devices refuse direct control. Writes become GraphQL mutations (for example DeviceRxChannelsSubscriptionSet); reads can tunnel ARC through the DAPI controller on 8443.");
node("shuredev", "net", 1830, 1260, "Shure receivers", "AD4D, P10T…", ["TCP 2202"],
  [], "Polled with < GET … > and answered with < REP … > / < REPORT … >.");

// On disk
node("cfg", "store", 370, 1620, "config.toml", "[daemon] [ddm] [profiles] [shure]", ["~/.config/netaudio/"],
  ["common/config_loader.py", "common/ddm_config_store.py", "common/app_config.py"],
  "Search order: $NETAUDIO_CONFIG, ~/.netaudio, $XDG_CONFIG_HOME/netaudio, ~/.config/netaudio, then the macOS and Windows locations. DDM servers and credentials are written here atomically.");
node("prefs", "store", 370, 1780, "preferences.json", "beside the config", ["mcp_token", "monitoring_port"],
  ["common/preferences.py"], "Small runtime preferences: the MCP bearer token, the OAuth password hash, the metering port, the public MCP URL.");
node("pem", "store", 630, 1620, "tls/daemon.pem", "generated certificate", ["EC P-256"],
  ["daemon/http/tls.py"], "Written on first start; renewed automatically.");
node("presetxml", "store", 630, 1780, "presets/*.xml", "beside the config file", ["schema v3"],
  ["presets/serialization.py"], "Dante-Controller-compatible XML with a <netaudio_configuration> JSON block. Unknown elements survive a load and save.");
node("sqlite", "store", 890, 1620, "packet_capture.sqlite", "only with daemon --capture", ["~/.local/share/netaudio"],
  ["dante/packet_store/store.py"], "Captured packets and artifacts for protocol research.");
node("nothing", "store", 890, 1780, "Not on disk", "device inventory, journal, issues", ["rebuilt every start"],
  [], "The daemon keeps no database of devices. Restarting it rediscovers everything from mDNS and clears the event journal and issues.", "no-persist");

// Research
node("capture", "research", 1450, 1620, "Capture", "tshark + multicast workers", ["capture live"],
  ["capture/daemon.py", "dante/tshark_capture.py", "commands/capture/"], "Records Dante traffic into sessions with markers so behaviour can be tied to actions.");
node("dissect", "research", 1450, 1780, "Dissection", "pairs requests with replies", ["opcode", "txn id"],
  ["dante/dissection/", "_capture.py"], "Decodes headers and renders packets for comparison.");
node("facts", "research", 1710, 1620, "Fact registry", "verified / observed / inferred", ["facts.json"],
  ["dante/fact_store.py", "commands/fact/"], "Each protocol fact records its field layout, the captured packets that support it, and a confidence level.");
node("verifier", "research", 1710, 1780, "Protocol verifier", "scripted experiments", ["evidence bundles"],
  ["dante/protocol_verifier.py"], "Runs experiments against a real device through CoreTransport and stores the packets as evidence.");

// Structural edges shown in the overview
const BASE = [
  "web>http", "cli>http", "ios>http", "mcpc>mcp", "dbus>dbusc", "redis>redisc", "bonjour>ios", "sse>web",
  "http>handlers", "handlers>batching", "batching>app", "mcp>handlers", "app>sse", "app>dbus", "app>redis", "app>journal",
  "discovery>app", "state>app", "readback>app", "notif>state", "heartbeat>app", "cmc>ct", "metering>sse", "presets>app",
  "app>ct", "ct>binding", "binding>planners", "planners>encode", "encode>client", "client>device", "device>mdns", "mdns>discovery",
  "device>mcnotif", "mcnotif>notif", "device>mchb", "mchb>heartbeat", "device>mcmeter", "mcmeter>metering", "mcsap>sap",
  "managed>ddmsrv", "ddmsrv>device", "shure>shuredev", "cli>device", "dapi>ddmsrv", "parser>binding", "clock>ct", "loops>ct",
];

const SURPRISES = [
  { id: "cli-direct", node: "cli", t: "The CLI never asks the daemon to write.",
    d: "Even with the daemon running, `netaudio subscription add` sends ARC packets from the CLI process itself. The daemon only lends its inventory (GET /devices) and receives journal entries. daemon/client.py has no subscribe call." },
  { id: "no-persist", node: "nothing", t: "There is no device database.",
    d: "The inventory lives in DanteApplication.devices and is rebuilt from mDNS every start. The only device state that survives a refresh is the browser's localStorage cache." },
  { id: "early-ok", node: "handlers", t: "The web UI's /subscribe answers before the route is verified.",
    d: "devices.py replies {success: true} after the device acknowledges the packet. Verification happens afterwards in SubscriptionReadback, which polls for up to 10 s and pushes device_updated over SSE." },
  { id: "three-batchers", node: "batching", t: "Three paths batch routes three ways.",
    d: "/subscribe sends one request per receiver; /subscriptions/apply (MCP) uses plan_batches with devices in parallel; the CLI uses Rust reconciliation and skips routes that are already set. Only the CLI path skips no-op writes." },
  { id: "adds-unaudited", node: "app", t: "Adding a subscription isn't written to the audit log; removing one is.",
    d: "remove_subscriptions runs inside _run_configuration_operation and becomes a configuration_operation event. add_subscriptions (dante/application.py:855) does not. The journal still notices failures later through observe_device." },
  { id: "journal-mem", node: "journal", t: "Events and issues vanish on restart.",
    d: "MonitoringEventJournal is a bounded deque (1000 by default). Run `netaudio events export` before restarting if you need the history." },
  { id: "port-8752", node: "metering", t: "Meters move to 8753 when Dante Controller is open.",
    d: "8751 is Dante Controller's metering port and 8752 is NetAudio's. If 8752 is busy the daemon falls back to 8753." },
  { id: "mcp-mismatch", node: "mcp", t: "The MCP tools you use day to day aren't in this checkout.",
    d: "This session's netaudio MCP server offers get_network_overview, get_routing, find_channels, discover_tools and invoke_tool. This branch's mcp.py lists flat tools (list_devices, set_subscriptions…). The running daemon is probably a newer build than this branch." },
];

const FLOWS = [
  { id: "join", name: "A device joins the network", sum: "From plugging in a Dante box to seeing it in the web UI. Nothing is stored; the daemon pieces the device together from mDNS, ARC reads and multicast.",
    steps: [
      { n: "device", e: "device>mdns", l: "advertise", t: "The device advertises itself over mDNS", d: "Services _netaudio-arc, -chan, -cmc, -dbc (and _dantevideo) with TXT records for model, versions, rate, latency and arcp_vers." },
      { n: "discovery", e: "mdns>discovery", l: "browse + resolve", t: "The daemon's zeroconf browser resolves it", d: "<code>_resolve_service</code> waits up to 3 s per instance. The device key becomes <code>&lt;instance&gt;.local.</code>" },
      { n: "app", e: "discovery>app", l: "register_device", t: "A DanteDevice is registered", d: "<code>DanteApplication.register_device</code> stores it in <code>devices{}</code> and emits <code>DEVICE_DISCOVERED</code>." },
      { n: "cmc", e: "discovery>cmc", l: "CMC id → MAC", t: "Identity and CMC registration", d: "The CMC TXT <code>id</code> becomes the MAC. ConMon make/model queries go out and the daemon registers itself with the device's CMC service, then repeats every 10 s." },
      { n: "state", e: "app>state", l: "fetch_device_controls", t: "The initial ARC reads begin", d: "Channel count, RX inventory pages, TX channels, name, settings, property directory, latency, then probes for AES67, sample rate, encoding, clock and lock status." },
      { n: "ct", e: "state>ct", l: "command spec", t: "Each read becomes a JSON command spec", d: "<code>DanteDevice.execute</code> → <code>CoreTransport</code>, which reuses one Rust client per socket." },
      { n: "binding", e: "ct>binding", l: "ctypes", t: "Python crosses into Rust", d: "<code>netaudio_client_execute</code> through the ctypes binding." },
      { n: "client", e: "binding>client", l: "UDP", t: "Rust sends the request", d: "<code>client.rs</code> sends to ARC 4440 and retries on silence." },
      { n: "device", e: "client>device", l: "ARC 4440", t: "The device answers", d: "Each reply is decoded by <code>parser.rs</code> / <code>responses/*.rs</code> and applied with <code>apply_controls</code>." },
      { n: "sse", e: "app>sse", l: "device_updated", t: "DEVICE_UPDATED is broadcast", d: "<code>_on_device_event</code> pushes the serialized device to every SSE client. The same event reaches DBus, Redis and the journal." },
      { n: "web", e: "sse>web", l: "EventSource", t: "The web UI renders it", d: "<code>store.js</code> merges the record and the routing matrix picks up its channels." },
      { n: "heartbeat", e: "mchb>heartbeat", l: "every second", t: "Heartbeats keep it alive", d: "From now on each heartbeat on 224.0.0.233:8708 updates last-seen, clock and traffic." },
    ] },
  { id: "route-web", name: "Route a channel from the web UI", sum: "Click a cell in the matrix: tx:1@stagebox → rx:1@avio. The daemon writes the packet, answers early, then confirms in the background.",
    steps: [
      { n: "web", e: "web>http", l: "POST /subscribe", t: "The browser posts the route", d: "<code>matrix.js</code> → <code>api.subscribe({rx_channel, rx_device, tx_channel, tx_device})</code>." },
      { n: "handlers", e: "http>handlers", l: "_handle_subscribe", t: "devices.py takes it", d: "Looked up in the POST table in <code>api.py</code>." },
      { n: "batching", e: "handlers>batching", l: "parse_routes", t: "The route is validated", d: "<code>subscription_batching.parse_routes</code> normalises channel names and numbers." },
      { n: "app", e: "batching>app", l: "add_subscriptions", t: "DanteApplication takes the device lock", d: "<code>topology_mutation_lock</code>, then a self-connection preflight. If the device is DDM-enrolled the write leaves here for DDM (see that journey)." },
      { n: "planners", e: "app>planners", l: "plan_subscription_commands", t: "Rust plans the packets", d: "The ARC protocol from <code>arcp_vers</code> decides the shape: legacy <code>0x3010</code> pages of 16 or modern <code>0x3410</code> pages of 32. Each page is test-encoded before anything is sent." },
      { n: "encode", e: "planners>encode", l: "encode", t: "Pages are encoded", d: "<code>commands/subscriptions.rs</code>." },
      { n: "client", e: "encode>client", l: "send", t: "Sent over UDP", d: "Via CoreTransport and the ctypes binding." },
      { n: "device", e: "client>device", l: "ARC 4440 + ack", t: "The receiver acknowledges", d: "<code>command_acknowledgement</code> checks each reply. A rejected page stops the rest." },
      { n: "mcnotif", e: "device>mcnotif", l: "notification 258", t: "The receiver announces the change", d: "The application waits up to 2 s for RX-channel or routing notifications. A timeout is only logged." },
      { n: "notif", e: "mcnotif>notif", l: "8702", t: "The notification listener wakes the waiter", d: "…and <code>DanteStateService</code> schedules a channel re-fetch." },
      { n: "sse", e: "handlers>sse", l: "subscription_pending", t: "The UI shows the route as pending", d: "The handler broadcasts <code>subscription_pending</code>, starts readback and replies <code>{success: true}</code> without waiting." },
      { n: "readback", e: "handlers>readback", l: "request()", t: "Readback confirms it", d: "One task per device re-reads RX channels every 0.5 s for up to 10 s until the Rust readback reports <code>settled</code>." },
      { n: "app", e: "readback>app", l: "DEVICE_UPDATED", t: "Each pass publishes the device", d: "The real subscription status from the device replaces the pending flag." },
      { n: "journal", e: "app>journal", l: "observe_device", t: "Failures become events and issues", d: "If the route doesn't come up, the journal records <code>subscription_failed</code> and IssueEngine opens <code>subscription_failure</code>." },
      { n: "web", e: "sse>web", l: "device_updated", t: "The matrix cell turns solid", d: "<code>store.js</code> clears the pending mark." },
    ] },
  { id: "route-cli", name: "Route a channel from the CLI", sum: "netaudio subscription add --tx tx:1@stagebox --rx rx:1@avio. The daemon only provides the device list; the CLI writes to the device itself and verifies before it exits.",
    steps: [
      { n: "cli", e: "cli>http", l: "GET /devices", t: "Borrow the daemon's inventory", d: "<code>get_devices_from_daemon()</code> on 127.0.0.1:9000. If that fails, the CLI runs its own mDNS discovery instead." },
      { n: "cli", t: "Resolve channels locally", d: "<code>parse_qualified_channel</code> and <code>resolve_channel</code> turn <code>rx:1@avio</code> and <code>tx:1@stagebox</code> into a channel number and a TX channel name." },
      { n: "planners", e: "cli>planners", l: "plan_subscription_reconciliation", t: "Reconcile against fresh state", d: "<code>reconcile_receiver_subscriptions</code> re-reads RX channels, then Rust splits the request into unchanged / set / clear. Routes that are already correct are not re-sent." },
      { n: "device", e: "cli>device", l: "ARC 4440, from the CLI", t: "The CLI sends the packets itself", d: "Same encoders and UDP client as the daemon, but in the CLI's own process." },
      { n: "cli", e: "device>cli", l: "readback", t: "Verified before exit", d: "A synchronous readback prints <code>UNCHANGED</code>, <code>(verified)</code> or the failure." },
      { n: "mcnotif", e: "device>mcnotif", l: "notification", t: "The daemon finds out on its own", d: "It sees the device's change notification like any other controller would, re-fetches, and updates the web UI." },
      { n: "journal", e: "cli>journal", l: "POST /event-journal/operations", t: "Audited writes land in the daemon's journal", d: "Operations that go through the audit recorder are posted to the daemon (loopback only). Adding a subscription is not one of them." },
    ] },
  { id: "route-mcp", name: "An AI agent changes routing", sum: "An MCP client calls set_subscriptions. It reuses the daemon's own HTTP handlers in-process, with a confirmation gate in front.",
    steps: [
      { n: "mcpc", e: "mcpc>mcp", l: "tools/call", t: "The agent calls set_subscriptions", d: "Authenticated by bearer token or OAuth; writes need the <code>netaudio:write</code> scope." },
      { n: "mcp", t: "Confirmation gate", d: "Without <code>confirmed=true</code> the tool returns an error asking the agent to check with the user first." },
      { n: "handlers", e: "mcp>handlers", l: "/subscriptions/apply", t: "Dispatched in-process", d: "The MCP layer calls the POST handler directly with a captured response object." },
      { n: "batching", e: "handlers>batching", l: "plan_batches", t: "Routes are batched per device", d: "Grouped by receiver and action, chunked at 16 (legacy ARC) or 32 (modern or DDM). Duplicate receivers are rejected. Non-DDM batches are pre-encoded to catch errors early." },
      { n: "app", e: "batching>app", l: "all devices in parallel", t: "Each device is written in parallel", d: "<code>asyncio.gather</code> across devices, then the same packet path as the web UI." },
      { n: "readback", e: "handlers>readback", l: "per batch", t: "Readback per batch", d: "The response lists per-route ok / error: HTTP 200 if anything applied, 409 if nothing did." },
      { n: "mcp", e: "handlers>mcp", l: "compact", t: "The result is compacted for the model", d: "<code>mcp_views</code> trims the payload before it goes back." },
    ] },
  { id: "meters", name: "Live meters", sum: "Opening a device's meters in the browser starts a UDP level stream from the device, kept alive by a lease.",
    steps: [
      { n: "web", e: "web>http", l: "POST /metering/start", t: "The browser takes a lease", d: "Body <code>{device, client_id}</code>. Leases stop the stream when nobody is watching." },
      { n: "metering", e: "handlers>metering", l: "add lease", t: "Metering manager starts a stream", d: "It needs a free local port: 8752, or 8753 if Dante Controller already holds it." },
      { n: "cmc", e: "metering>cmc", l: "start_metering", t: "CMC asks the device to stream", d: "The command carries the host IP, MAC and listening port." },
      { n: "device", e: "cmc>ct", l: "CMC command", t: "Sent through the Rust core", d: "Via CoreTransport like every other packet." },
      { n: "mcmeter", e: "device>mcmeter", l: "UDP levels", t: "The device streams levels", d: "Keepalive every 3 s; abandoned after 60 s." },
      { n: "metering", e: "mcmeter>metering", l: "parse dBFS", t: "Levels are normalised", d: "<code>parse_metering_levels</code> → dBFS, cached for 2 s." },
      { n: "sse", e: "metering>sse", l: "meter_values / 50 ms", t: "Broadcast every 50 ms", d: "Detailed updates are coalesced per device so a slow client doesn't fall behind." },
      { n: "web", e: "sse>web", l: "meter_values", t: "Meters move", d: "Coarse signal presence also arrives from heartbeats, so the routing view can show activity without a full stream." },
    ] },
  { id: "preset", name: "Load a preset", sum: "Restore a saved setup. Every item is planned against a fresh read, only changes are written, and every write is read back.",
    steps: [
      { n: "web", e: "web>http", l: "POST /presets/load", t: "Pick a preset", d: "Or <code>netaudio preset load stage</code> from the CLI. <code>/presets/preview</code> and <code>--dry-run</code> stop after planning." },
      { n: "presets", e: "handlers>presets", l: "preset lock", t: "Parse the XML", d: "Duplicate device names are rejected. The <code>netaudio_configuration</code> JSON block carries what the Dante fields can't." },
      { n: "presets", e: "presets>app", l: "match devices", t: "Match preset devices to live ones", d: "By name, then server name, MAC or inventory ID." },
      { n: "device", e: "app>ct", l: "fresh reads", t: "Plan against fresh state", d: "Each item becomes change, unchanged, unsupported, unavailable or ambiguous: names, sample rate, encoding, latency, clock, channel names, flows, subscriptions, gain, interfaces, panel controls." },
      { n: "app", t: "Apply only the changes", d: "Each action handler writes, then reads back. Restoring a sample rate that would drop flow membership needs <code>--confirm-destructive</code>." },
      { n: "journal", e: "app>journal", l: "preset_run", t: "Recorded as one preset run", d: "Each configuration operation is linked to the run ID, and the report summarises every item's outcome." },
    ] },
  { id: "ddm", name: "Write to a DDM-enrolled device", sum: "Devices in a Dante Domain Manager domain ignore direct writes. The same add_subscriptions call goes out as GraphQL instead of ARC.",
    steps: [
      { n: "managed", e: "managed>ddmsrv", l: "GraphQL poll", t: "The daemon knows the device is managed", d: "DDM inventory polling sets <code>ddm_enrolment_state</code>; <code>requires_managed_control</code> becomes true." },
      { n: "app", e: "batching>app", l: "add_subscriptions", t: "Same entry point", d: "Web, MCP and presets all reach <code>DanteApplication.add_subscriptions</code>." },
      { n: "ddmsrv", e: "app>ddmsrv", l: "DeviceRxChannelsSubscriptionSet", t: "Sent as a GraphQL mutation", d: "To the DDM server's <code>/graphql</code>. No ARC packet, no notification wait, batches of 32. A clear is the same mutation with empty strings." },
      { n: "device", e: "ddmsrv>device", l: "DDM applies it", t: "DDM changes the device", d: "The mutation must return <code>ok: true</code> or the write fails." },
      { n: "dapi", e: "dapi>ddmsrv", l: "DAPI :8443", t: "Reads can tunnel through DDM", d: "ARC and settings reads are framed by Rust <code>dapi.rs</code> and sent over a TLS session to the DDM controller." },
      { n: "readback", e: "readback>managed", l: "refresh_managed_subscriptions", t: "Readback uses DDM inventory", d: "Status comes from DDM's own subscription status rather than ARC status codes." },
    ] },
  { id: "leave", name: "A device disappears", sum: "Unplugging doesn't remove a device straight away. Two independent signals have to agree, and a final probe gets a last chance.",
    steps: [
      { n: "mdns", e: "mdns>discovery", l: "service removed", t: "mDNS says the service went away", d: "<code>_handle_removed_service</code> marks the device as an offline candidate." },
      { n: "discovery", t: "Wait for it to be real", d: "At least 2 failures and at least 5 s before the daemon believes it." },
      { n: "device", e: "loops>ct", l: "probe ARC", t: "Final ARC probe", d: "<code>_finalize_offline_device</code> pings ARC. A reply cancels the removal." },
      { n: "heartbeat", e: "mchb>heartbeat", l: "silence", t: "Heartbeats stop", d: "Separately, the 5 s sweep marks any device silent for more than 15 s as offline." },
      { n: "app", e: "heartbeat>app", l: "mark offline", t: "The device is marked offline", d: "Capability fields are cleared and <code>DEVICE_REMOVED</code> is emitted." },
      { n: "journal", e: "app>journal", l: "device_disappeared", t: "The journal records it", d: "And the issue engine re-evaluates subscriptions that depended on it." },
      { n: "web", e: "sse>web", l: "device_removed", t: "The UI greys it out", d: "If the device returns, the next mDNS record or heartbeat registers it again and metering reactivates." },
    ] },
  { id: "boot", name: "The daemon starts", sum: "What `netaudio daemon start` brings up, in the order server.py starts it.",
    steps: [
      { n: "http", t: "HTTP and TLS listeners first", d: "If port 9000 is taken the start fails with DaemonAlreadyRunningError. The Bonjour advert goes out with it." },
      { n: "tls", e: "tls>pem", l: "load or create", t: "Load or generate the certificate", d: "<code>tls/daemon.pem</code> beside the config." },
      { n: "redis", t: "Connect Redis (optional)", d: "Only if REDIS_HOST or REDIS_SOCKET is set." },
      { n: "notif", t: "Application startup", d: "Event dispatcher, notification listener on 224.0.0.231 and the SAP listener." },
      { n: "managed", t: "DDM inventory polling", d: "One task per configured DDM server." },
      { n: "metering", t: "Metering listener", d: "Binds 8752 (or 8753)." },
      { n: "shure", t: "Shure neighbour scan", d: "Looks for OUI 00:0e:dd in the ARP table." },
      { n: "heartbeat", t: "Heartbeat listener", d: "224.0.0.233:8708." },
      { n: "dbus", t: "DBus service", d: "If enabled." },
      { n: "discovery", t: "mDNS discovery last", d: "Starting discovery after all listeners means no early notification is missed." },
      { n: "loops", t: "Background loops", d: "Clock monitor every 2 s, revalidation every 30 s, recovery of known devices after 3 s. Then READY=1 to systemd." },
    ] },
];
