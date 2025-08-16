import json
import uvicorn

from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from netaudio.utils.cli import FireTyped
from netaudio.dante.browser import DanteBrowser
import logging

logger = logging.getLogger(__name__)

app = FastAPI()
dante_browser = DanteBrowser(mdns_timeout=1.5)

origins = [
    "http://192.168.1.107:3002",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


@app.post(
    "/subscribe/{rx_device_name}/{rx_channel_name}/{tx_device_name}/{tx_channel_name}"
)
async def subscribe_device(
    rx_device_name: str,
    rx_channel_name: str,
    tx_device_name: str,
    tx_channel_name: str,
    payload: dict = Body(...),
):
    logger.info(
        f"rx_d: {rx_device_name} {rx_channel_name} {tx_device_name} {tx_channel_name}",
        rx_device_name,
        rx_channel_name,
        tx_device_name,
        tx_channel_name,
    )
    dante_devices = await dante_browser.get_devices()

    for _, device in dante_devices.items():
        await device.get_controls()

    rx_channel = None
    rx_device = None
    tx_channel = None
    tx_device = None

    tx_device = next(
        filter(
            lambda d: d[1].name == tx_device_name,
            dante_devices.items(),
        )
    )[1]
    tx_channel = next(
        filter(
            lambda c: tx_channel_name == c[1].friendly_name
            or tx_channel_name == c[1].name
            and not c[1].friendly_name,
            tx_device.tx_channels.items(),
        )
    )[1]
    rx_device = next(
        filter(
            lambda d: d[1].name == rx_device_name,
            dante_devices.items(),
        )
    )[1]
    rx_channel = next(
        filter(
            lambda c: c[1].name == rx_channel_name,
            rx_device.rx_channels.items(),
        )
    )[1]

    if rx_channel and rx_device and tx_channel and tx_channel:
        await rx_device.add_subscription(rx_channel, tx_channel, tx_device)
    else:
        raise HTTPException(status_code=404, detail="Device or Channel not found")
    return {}


@app.post("/devices/{device_name}/rx_name/{rx_number}")
async def name_rx_device(device_name: str, rx_number: int, payload: dict = Body(...)):
    name = payload["name"]
    devices = await device_list()
    device = next((d for d in devices.values() if d.name == device_name), None)
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    try:
        await device.set_channel_name("rx", rx_number, name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {}


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

@FireTyped
def run_server():
    """
    Run a control HTTP Server
    """
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
