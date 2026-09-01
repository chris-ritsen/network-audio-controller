from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
from pathlib import Path

from netaudio.dante.const import (
    DEVICE_ARC_PORT,
    DEVICE_CONTROL_PORT,
    DEVICE_INFO_PORT,
    DEVICE_SETTINGS_PORT,
)

logger = logging.getLogger("netaudio")


def _build_bpf_filter(include_tcp=False):
    if include_tcp:
        return "udp or tcp"
    return "udp"


class TsharkCapture:
    UDP_FIELDS = [
        "frame.time_epoch",
        "ip.src",
        "udp.srcport",
        "ip.dst",
        "udp.dstport",
        "data.data",
    ]

    TCP_FIELDS = [
        "frame.time_epoch",
        "ip.src",
        "tcp.srcport",
        "ip.dst",
        "tcp.dstport",
        "tcp.payload",
    ]

    def __init__(
        self,
        packet_store,
        interface="en0",
        device_ips=None,
        known_device_ips=None,
        include_metering=False,
        include_tcp=False,
        packet_filter=None,
        session_id=None,
    ):
        self._store = packet_store
        self._interface = interface
        self._device_ips = set(device_ips) if device_ips else set()
        self._known_device_ips = known_device_ips or self._device_ips
        self._include_metering = include_metering
        self._include_tcp = include_tcp
        self._packet_filter = packet_filter
        self._session_id = session_id
        self._process = None
        self._cached_active_session_ids = []
        self._session_check_counter = 0

    TSHARK_SEARCH_PATHS = [
        "/opt/homebrew/bin/tshark",
        "/usr/local/bin/tshark",
        "/usr/bin/tshark",
    ]

    @classmethod
    def _find_tshark(cls):
        found = shutil.which("tshark")
        if found:
            return found

        for path in cls.TSHARK_SEARCH_PATHS:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path

        return None

    @classmethod
    def is_available(cls):
        return cls._find_tshark() is not None

    @classmethod
    def read_pcap_frames(
        cls,
        packet_store,
        pcap_path: str | Path,
        frame_numbers: list[int],
        device_ip: str,
        session_id: int,
        source_host: str | None = None,
    ) -> list[dict]:
        resolved_path = Path(pcap_path).expanduser().resolve()
        if not resolved_path.is_file():
            raise ValueError(f"capture file not found: {resolved_path}")

        selected_frames = sorted(set(frame_numbers))
        if not selected_frames or any(frame_number <= 0 for frame_number in selected_frames):
            raise ValueError("at least one positive frame number is required")

        tshark_path = cls._find_tshark()
        if tshark_path is None:
            raise RuntimeError("tshark is required to ingest a capture file")

        display_filter = " || ".join(f"frame.number == {frame_number}" for frame_number in selected_frames)
        fields = ["frame.number", *cls.UDP_FIELDS]
        field_arguments = []
        for field in fields:
            field_arguments.extend(["-e", field])

        completed = subprocess.run(
            [
                tshark_path,
                "-r",
                str(resolved_path),
                "-Y",
                display_filter,
                "-T",
                "fields",
                *field_arguments,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or f"exit status {completed.returncode}"
            raise RuntimeError(f"tshark could not read {resolved_path}: {detail}")

        parser = cls(packet_store=packet_store, known_device_ips=[device_ip])
        parsed_frames = []
        observed_frames = set()
        for line in completed.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            try:
                frame_number = int(parts[0])
            except ValueError:
                continue
            parsed = parser._parse_line("\t".join(parts[1:]))
            if parsed is None:
                continue
            observed_frames.add(frame_number)
            parsed_frames.append((frame_number, parsed))

        missing_frames = [frame_number for frame_number in selected_frames if frame_number not in observed_frames]
        if missing_frames:
            missing = ", ".join(str(frame_number) for frame_number in missing_frames)
            raise ValueError(f"selected frames did not contain importable UDP payloads: {missing}")

        imported = []
        for frame_number, parsed in parsed_frames:
            packet_id = packet_store.store_packet(
                payload=parsed["payload"],
                source_type="pcap_import",
                src_ip=parsed["src_ip"],
                src_port=parsed["src_port"],
                dst_ip=parsed["dst_ip"],
                dst_port=parsed["dst_port"],
                device_ip=parsed["device_ip"],
                direction=parsed["direction"],
                session_id=session_id,
                timestamp_ns=parsed["timestamp_ns"],
                source_host=source_host,
                interface=f"pcap:{resolved_path.name}",
            )
            if packet_id is None:
                raise RuntimeError(f"failed to store frame {frame_number}")
            imported.append(
                {
                    "frame_number": frame_number,
                    "packet_id": packet_id,
                    "direction": parsed["direction"],
                    "payload_size": len(parsed["payload"]),
                }
            )
        return imported

    def _build_command(self):
        tshark_path = self._find_tshark() or "tshark"
        bpf = _build_bpf_filter(include_tcp=self._include_tcp)

        fields = list(self.UDP_FIELDS)
        if self._include_tcp:
            for field in self.TCP_FIELDS:
                if field not in fields:
                    fields.append(field)

        field_args = []
        for field in fields:
            field_args.extend(["-e", field])

        return [
            tshark_path,
            "-i",
            self._interface,
            "-T",
            "fields",
            *field_args,
            "-l",
            "-f",
            bpf,
        ]

    def _parse_line(self, line: str):
        parts = line.strip().split("\t")
        if len(parts) < 6:
            return None

        epoch_str = parts[0]
        udp_src_ip = parts[1]
        udp_src_port_str = parts[2]
        udp_dst_ip = parts[3]
        udp_dst_port_str = parts[4]
        udp_hex_data = parts[5]

        tcp_src_port_str = parts[6] if len(parts) > 6 else ""
        tcp_dst_ip = parts[7] if len(parts) > 7 else ""
        tcp_dst_port_str = parts[8] if len(parts) > 8 else ""
        tcp_hex_data = parts[9] if len(parts) > 9 else ""

        is_tcp = False
        if udp_src_port_str and udp_hex_data:
            src_ip = udp_src_ip
            src_port_str = udp_src_port_str
            dst_ip = udp_dst_ip
            dst_port_str = udp_dst_port_str
            hex_data = udp_hex_data
        elif tcp_src_port_str and tcp_hex_data:
            src_ip = udp_src_ip
            src_port_str = tcp_src_port_str
            dst_ip = tcp_dst_ip
            dst_port_str = tcp_dst_port_str
            hex_data = tcp_hex_data
            is_tcp = True
        else:
            return None

        try:
            timestamp_ns = int(float(epoch_str) * 1e9)
        except (ValueError, OverflowError):
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
        dante_ports = {DEVICE_ARC_PORT, DEVICE_CONTROL_PORT, DEVICE_INFO_PORT, DEVICE_SETTINGS_PORT}
        well_known_ports = {DEVICE_ARC_PORT, DEVICE_CONTROL_PORT, DEVICE_SETTINGS_PORT}

        if not is_multicast_dst:
            is_device_traffic = (
                (
                    src_ip in self._known_device_ips
                    or dst_ip in self._known_device_ips
                    or src_port in dante_ports
                    or dst_port in dante_ports
                )
                if self._known_device_ips
                else (src_port in dante_ports or dst_port in dante_ports)
            )
            if not is_device_traffic:
                return None

        if is_multicast_dst:
            direction = None
            device_ip = src_ip
        elif self._known_device_ips:
            dst_is_device = dst_ip in self._known_device_ips
            src_is_device = src_ip in self._known_device_ips
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
            "transport": "tcp" if is_tcp else "udp",
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

        was_cancelled = False
        try:
            async for raw_line in self._process.stdout:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue

                fields = self._parse_line(line)
                if not fields:
                    continue
                if fields["transport"] == "tcp":
                    source_type = "tshark_tcp"
                elif fields["direction"] is None:
                    source_type = "multicast"
                else:
                    source_type = "tshark"

                if self._session_id is not None:
                    active_session_ids = [self._session_id]
                else:
                    self._session_check_counter += 1
                    if self._session_check_counter % 50 == 1:
                        active_sessions = self._store.get_active_sessions()
                        self._cached_active_session_ids = [int(session["id"]) for session in active_sessions]
                    active_session_ids = self._cached_active_session_ids

                primary_session_id = active_session_ids[0] if active_session_ids else None

                packet_id = self._store.store_packet(
                    payload=fields["payload"],
                    source_type=source_type,
                    src_ip=fields["src_ip"],
                    src_port=fields["src_port"],
                    dst_ip=fields["dst_ip"],
                    dst_port=fields["dst_port"],
                    device_ip=fields["device_ip"],
                    direction=fields["direction"],
                    session_id=primary_session_id,
                    timestamp_ns=fields["timestamp_ns"],
                    interface=self._interface,
                )

                if packet_id:
                    for session_id in active_session_ids:
                        self._store.link_packet_to_session(packet_id, session_id)

                if packet_id and on_packet:
                    await on_packet(packet_id, fields)

        except asyncio.CancelledError:
            was_cancelled = True
        finally:
            failure_message = None
            if self._process and not was_cancelled:
                try:
                    await self._process.wait()
                except Exception:
                    logger.exception("Failed to wait for tshark process shutdown")

            if self._process and not was_cancelled and self._process.returncode not in (None, 0):
                stderr_text = ""
                if self._process.stderr is not None:
                    try:
                        stderr_text = (await self._process.stderr.read()).decode("utf-8", errors="replace").strip()
                    except Exception:
                        stderr_text = ""

                if stderr_text:
                    stderr_lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
                    summary = " | ".join(stderr_lines[-3:]) if stderr_lines else stderr_text
                    logger.error(f"tshark exited with code {self._process.returncode}: {summary}")
                    failure_message = summary
                else:
                    logger.error(f"tshark exited with code {self._process.returncode}")
                    failure_message = f"exit code {self._process.returncode}"

            await self.stop()

            if failure_message:
                raise RuntimeError(failure_message)

    async def stop(self):
        if self._process and self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self._process.kill()
                await self._process.wait()
            self._process = None
