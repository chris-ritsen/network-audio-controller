from __future__ import annotations


class DaemonSettingsHandlers:
    async def _handle_get_settings(self, writer):
        if self.metering is None:
            await self._send_json(writer, {"error": "Monitoring is unavailable"}, 503)
            return
        await self._send_json(writer, self.metering.port_settings())

    async def _handle_monitoring_settings(self, writer, params):
        if self.metering is None:
            await self._send_json(writer, {"error": "Monitoring is unavailable"}, 503)
            return
        try:
            settings = await self.metering.configure_port(params.get("port"))
        except ValueError as error:
            await self._send_json(writer, {"error": str(error)}, 400)
            return
        except OSError:
            await self._send_json(
                writer,
                {"error": "That port could not be opened or saved. Check whether another application is using it."},
                409,
            )
            return
        await self._broadcast_sse({"event": "settings_updated", "settings": settings})
        await self._send_json(writer, settings)
