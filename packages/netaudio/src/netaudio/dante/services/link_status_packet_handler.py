from __future__ import annotations

import logging


logger = logging.getLogger("netaudio")


class LinkStatusPacketHandler:
    def _handle_link_status(self, data: bytes, source_ip: str) -> None:
        from netaudio import core
        from netaudio.dante.link_status import LinkStatusObservation

        try:
            parsed_response = core.parse_response("unmapped_0040_status", data)
            observation = LinkStatusObservation.from_core(
                parsed_response,
                device=self._lookup_device(source_ip),
            )
        except (core.NetaudioCoreError, KeyError, TypeError, ValueError) as exception:
            logger.warning(f"Invalid link status from {source_ip}: {exception}")
            return

        self._notify_link_status_waiter(source_ip, observation)
