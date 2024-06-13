import asyncio
import json
import uvicorn

from cleo.commands.command import Command

from fastapi import FastAPI, HTTPException, Path, Body
from netaudio.dante.browser import DanteBrowser

app = FastAPI()
dante_browser = DanteBrowser(mdns_timeout=1.5)


async def device_list():
    devices = await dante_browser.get_devices()

    for _, device in devices.items():
        await device.get_controls()

    devices = dict(sorted(devices.items(), key=lambda x: x[1].name))
    return devices


@app.get("/devices")
async def list_devices():
    try:
        devices = await device_list()
        return json.loads(json.dumps(devices, indent=2))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/devices/{device_name}/configure")
async def configure_device(device_name: str, payload: dict = Body(...)):
    devices = await device_list()
    device = next((d for d in devices.values() if d.name == device_name), None)

    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    if "reset_device_name" in payload:
        await device.reset_name()

    if "device_name" in payload:
        await device.set_name(payload["device_name"])

    if "identify" in payload and payload["identify"]:
        await device.identify()

    if "sample_rate" in payload:
        await device.set_sample_rate(payload["sample_rate"])

    if "encoding" in payload:
        await device.set_encoding(payload["encoding"])

    if all(k in payload for k in ["gain_level", "channel_number", "channel_type"]):
        await device.set_gain_level(
            payload["channel_number"], payload["gain_level"], payload["channel_type"]
        )

    if "aes67" in payload:
        await device.enable_aes67(payload["aes67"])

    return json.loads(json.dumps(device, indent=2))


class ServerHttpCommand(Command):
    name = "http"
    description = "Run an HTTP server"

    def handle(self):
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
