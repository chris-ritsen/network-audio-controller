import asyncio
import logging
import shutil

logger = logging.getLogger("netaudio")

DANTE_MULTICAST_PORTS = [8702]
DANTE_METERING_PORT = 8751

DANTE_UNICAST_PORTS = [8800, 8700]


def _build_bpf_filter(device_ips=None):
    multicast_clauses = " or ".join(f"port {p}" for p in DANTE_MULTICAST_PORTS)

    if device_ips:
        host_clauses = " or ".join(f"host {ip}" for ip in device_ips)
        bpf = (
            f"udp and (({host_clauses} and not dst net 224.0.0.0/4) or "
            f"({multicast_clauses}))"
        )
    else:
        unicast_clauses = " or ".join(f"port {p}" for p in DANTE_UNICAST_PORTS)
        bpf = (
            f"udp and (({multicast_clauses}) or "
            f"(({unicast_clauses}) and not dst net 224.0.0.0/4))"
        )

    return bpf


class TsharkCapture:
    TSHARK_FIELDS = [
        "frame.time_epoch",
        "ip.src",
        "udp.srcport",
        "ip.dst",
        "udp.dstport",
        "data.data",
    ]

    def __init__(self, packet_store, interface="en0", device_ips=None, include_metering=False):
        self._store = packet_store
        self._interface = interface
        self._device_ips = set(device_ips) if device_ips else set()
        self._include_metering = include_metering
        self._process = None

    @staticmethod
    def is_available():
        return shutil.which("tshark") is not None

    def _build_command(self):
        bpf = _build_bpf_filter(self._device_ips or None)
        field_args = []
        for f in self.TSHARK_FIELDS:
            field_args.extend(["-e", f])

        return [
            "tshark",
            "-i", self._interface,
            "-T", "fields",
            *field_args,
            "-l",
            "-f", bpf,
        ]

    def _parse_line(self, line: str):
        parts = line.strip().split("\t")
        if len(parts) < 6:
            return None

        epoch_str, src_ip, src_port_str, dst_ip, dst_port_str, hex_data = parts[:6]

        try:
            timestamp_ns = int(float(epoch_str) * 1e9)
        except (ValueError, OverflowError):
            return None

        if not self._include_metering:
            if src_port_str == str(DANTE_METERING_PORT) or dst_port_str == str(DANTE_METERING_PORT):
                return None

        try:
            src_port = int(src_port_str)
            dst_port = int(dst_port_str)
        except ValueError:
            return None

        hex_clean = hex_data.replace(":", "").replace(" ", "")
        if not hex_clean:
            return None

        try:
            payload = bytes.fromhex(hex_clean)
        except ValueError:
            return None

        is_multicast_dst = dst_ip.startswith("224.")
        well_known_ports = {8800, 8700}

        if is_multicast_dst:
            direction = None
            device_ip = src_ip
        elif self._device_ips:
            dst_is_device = dst_ip in self._device_ips
            src_is_device = src_ip in self._device_ips
            if dst_is_device and not src_is_device:
                direction = "request"
                device_ip = dst_ip
            elif src_is_device and not dst_is_device:
                direction = "response"
                device_ip = src_ip
            elif dst_port in well_known_ports:
                direction = "request"
                device_ip = dst_ip
            elif src_port in well_known_ports:
                direction = "response"
                device_ip = src_ip
            else:
                direction = None
                device_ip = src_ip
        elif dst_port in well_known_ports:
            direction = "request"
            device_ip = dst_ip
        elif src_port in well_known_ports:
            direction = "response"
            device_ip = src_ip
        else:
            direction = None
            device_ip = src_ip

        return {
            "payload": payload,
            "timestamp_ns": timestamp_ns,
            "src_ip": src_ip,
            "src_port": src_port,
            "dst_ip": dst_ip,
            "dst_port": dst_port,
            "direction": direction,
            "device_ip": device_ip,
        }

    async def start(self, on_packet=None):
        if not self.is_available():
            logger.error(
                "tshark not found. Install Wireshark or tshark:\n"
                "  macOS: brew install --cask wireshark\n"
                "  Linux: sudo apt install tshark"
            )
            return

        cmd = self._build_command()
        logger.info(f"Starting tshark: {' '.join(cmd)}")

        try:
            self._process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except PermissionError:
            logger.error(
                "Permission denied running tshark. You may need to:\n"
                "  macOS: Add your user to the 'access_bpf' group, or run with sudo\n"
                "  Linux: sudo setcap cap_net_raw+eip $(which tshark)"
            )
            return
        except FileNotFoundError:
            logger.error("tshark binary not found")
            return

        try:
            async for raw_line in self._process.stdout:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue

                fields = self._parse_line(line)
                if not fields:
                    continue

                if fields["direction"] is None:
                    source_type = "multicast"
                else:
                    source_type = "tshark"

                packet_id = self._store.store_packet(
                    payload=fields["payload"],
                    source_type=source_type,
                    src_ip=fields["src_ip"],
                    src_port=fields["src_port"],
                    dst_ip=fields["dst_ip"],
                    dst_port=fields["dst_port"],
                    device_ip=fields["device_ip"],
                    direction=fields["direction"],
                    timestamp_ns=fields["timestamp_ns"],
                )

                if packet_id and on_packet:
                    await on_packet(packet_id, fields)

        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self):
        if self._process and self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self._process.kill()
                await self._process.wait()
            self._process = None
