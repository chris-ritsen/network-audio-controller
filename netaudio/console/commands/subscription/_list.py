import asyncio
import json
import os
from json import JSONEncoder

import typer
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from typing_extensions import Annotated

from netaudio.common.app_config import settings as app_settings
from netaudio.common.mdns_cache import MdnsCache
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.subscription import DanteSubscription


def _dante_subscription_serializer(obj):
    if isinstance(obj, DanteSubscription):
        return obj.to_json()

    raise TypeError(
        f"Type {type(obj).__name__} not serializable and not a DanteSubscription"
    )


async def subscription_list(
    json_output: Annotated[bool, typer.Option("--json", help="Output as JSON")] = False,
):
    subscriptions = []
    redis_enabled = False
    redis_socket_path = os.environ.get("REDIS_SOCKET")
    redis_host = os.environ.get("REDIS_HOST") or "localhost"
    redis_port = os.environ.get("REDIS_PORT") or 6379
    redis_db = os.environ.get("REDIS_DB") or 0

    try:
        redis_client = None

        if redis_socket_path:
            redis_client = Redis(
                db=redis_db,
                decode_responses=False,
                socket_timeout=0.1,
                unix_socket_path=redis_socket_path,
            )
        elif os.environ.get("REDIS_PORT") or os.environ.get("REDIS_HOST"):
            redis_client = Redis(
                db=redis_db,
                decode_responses=False,
                host=redis_host,
                socket_timeout=0.1,
                port=redis_port,
            )

        if redis_client:
            redis_client.ping()
            redis_enabled = True
    except RedisConnectionError:
        print("Notice: Redis connection failed. Continuing with live discovery.")
    except Exception as e:
        print(
            f"Notice: Redis initialization error ({e}). Continuing with live discovery."
        )

    devices_dict = {}
    if redis_enabled:
        print(
            "Notice: Redis is enabled, but integrated caching logic for 'subscription list' is not fully active. Using live discovery."
        )
        redis_enabled = False

    if not redis_enabled:
        dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)

        if app_settings.refresh:
            mdns_cache = MdnsCache()
            mdns_cache.clear()
            mdns_cache.close()

        raw_devices = await dante_browser.get_devices()

        if not raw_devices:
            print("No Dante devices found on the network.")
            raise typer.Exit()

        devices_dict = dict(sorted(raw_devices.items(), key=lambda x: x[1].name))

        for _, device in devices_dict.items():
            try:
                await device.get_controls()
            except Exception as e:
                print(
                    f"Warning: Could not get controls for device {getattr(device, 'name', 'Unknown')}: {e}"
                )

    if not devices_dict:
        print("No devices found or processed. Cannot list subscriptions.")
        raise typer.Exit()

    for _, device in devices_dict.items():
        if hasattr(device, "subscriptions") and device.subscriptions:
            for sub in device.subscriptions:
                subscriptions.append(sub)

    if not subscriptions:
        print("No active subscriptions found on any device.")
        raise typer.Exit()

    if json_output:
        try:
            json_object = json.dumps(
                subscriptions, indent=2, default=_dante_subscription_serializer
            )
            print(json_object)
        except TypeError as e:
            print(
                f"Error serializing subscriptions to JSON: {e}. Outputting as strings instead."
            )
            for sub in subscriptions:
                print(str(sub))
    else:
        for sub in subscriptions:
            print(str(sub))
