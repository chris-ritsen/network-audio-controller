import asyncio
import ipaddress
import json
import socket
from json import JSONEncoder

import typer
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from typing_extensions import Annotated

from netaudio.dante.browser import DanteBrowser
from netaudio.dante.channel import DanteChannel
from netaudio.dante.const import SERVICE_CMC
from netaudio.dante.device import DanteDevice
from netaudio.dante.subscription import DanteSubscription

from ....common.app_config import settings as app_settings
from ....common.mdns_cache import MdnsCache
from .._utils import CustomJSONEncoder, filter_devices_util, get_host_by_name_util

app = typer.Typer(
    name="device", help="Control and list network audio devices.", no_args_is_help=True
)


def get_devices_from_redis_util() -> dict[str, DanteDevice] | None:
    redis_client = None

    try:
        redis_client = Redis(
            db=0, decode_responses=True, host="localhost", port=6379, socket_timeout=0.1
        )

        redis_client.ping()
    except RedisConnectionError:
        typer.echo(
            "Warning: Could not connect to Redis. Device list from cache will be unavailable.",
            err=True,
        )

        return None
    except Exception as e:
        typer.echo(
            f"Warning: Redis client initialization error: {e}. Device list from cache will be unavailable.",
            err=True,
        )

        return None

    host_keys = redis_client.smembers("netaudio:dante:hosts")

    devices_dict: dict[str, DanteDevice] = {}
    for host_key in host_keys:
        host_data = redis_client.hgetall(f"netaudio:dante:host:{host_key}")

        if not host_data or "server_name" not in host_data:
            continue

        server_name = host_data["server_name"]
        device = DanteDevice(server_name=server_name)
        device.ipv4 = host_data.get("ipv4")
        device_data = redis_client.hgetall(f"netaudio:dante:device:{server_name}")

        if device_data:
            try:
                device.name = device_data.get("device_name")
                device.model_id = device_data.get("model_id")
                rx_channels_json = device_data.get("rx_channels", "{}")

                device.rx_channels = {
                    chnum: DanteChannel(name=chdata.get("name"), number=int(chnum))
                    for chnum, chdata in json.loads(rx_channels_json).items()
                }

                tx_channels_json = device_data.get("tx_channels", "{}")

                device.tx_channels = {
                    chnum: DanteChannel(name=chdata.get("name"), number=int(chnum))
                    for chnum, chdata in json.loads(tx_channels_json).items()
                }
            except json.JSONDecodeError:
                typer.echo(
                    f"Warning: Corrupt JSON channel data for {server_name} in Redis.",
                    err=True,
                )
            except Exception as e:
                typer.echo(
                    f"Warning: Error processing device data for {server_name} from Redis: {e}",
                    err=True,
                )
        devices_dict[server_name] = device

    return devices_dict if devices_dict else None


async def fetch_and_prepare_devices(name_filter: str | None, host_filter: str | None):
    devices: dict[str, DanteDevice] | None = None
    mdns_cache: MdnsCache | None = None
    loaded_from_mdns_cache = False

    try:
        mdns_cache = MdnsCache()

        if app_settings.refresh:
            mdns_cache.clear()
        else:
            cached_devices_dict: dict[str, DanteDevice] = {}

            try:
                for server_name_key in list(mdns_cache._db.keys()):
                    cached_data = mdns_cache.get(server_name_key)

                    if cached_data:
                        device = DanteDevice(
                            server_name=server_name_key,
                            dump_payloads=app_settings.dump_payloads,
                        )

                        device.ipv4 = cached_data.get("ipv4")

                        device.name = (
                            cached_data.get("name") if cached_data.get("name") else None
                        )

                        device.model_id = cached_data.get("model_id")
                        device.model_name = cached_data.get("model_name")
                        device.manufacturer = cached_data.get("manufacturer")
                        device.services = cached_data.get("discovered_services_map", {})
                        cached_devices_dict[server_name_key] = device

                if cached_devices_dict:
                    devices = cached_devices_dict
                    loaded_from_mdns_cache = True

            except Exception as e:
                typer.echo(
                    f"Warning: Error reading from mDNS disk cache: {e}", err=True
                )

        if devices is None:
            if not app_settings.refresh:
                redis_devices = get_devices_from_redis_util()
                if redis_devices:
                    devices = redis_devices

            if devices is None:
                dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)

                fetched_devices_map = await dante_browser.get_devices()
                devices = (
                    fetched_devices_map if isinstance(fetched_devices_map, dict) else {}
                )

                if devices:
                    try:
                        for server_name, device_obj in devices.items():
                            device_name_val = getattr(device_obj, "name", None)
                            cache_value = {
                                "ipv4": getattr(device_obj, "ipv4", None),
                                "name": device_name_val if device_name_val else None,
                                "model_id": getattr(device_obj, "model_id", None),
                                "model_name": getattr(device_obj, "model_name", None),
                                "manufacturer": getattr(
                                    device_obj, "manufacturer", None
                                ),
                                "discovered_services_map": getattr(
                                    device_obj, "services", {}
                                ),
                            }

                            mdns_cache.set(server_name, cache_value)
                    except Exception as e:
                        typer.echo(
                            f"Warning: Error writing to mDNS disk cache: {e}", err=True
                        )

    finally:
        if mdns_cache:
            mdns_cache.close()

    if not devices:
        devices = {}

    devices = filter_devices_util(
        devices, name_filter=name_filter, host_filter=host_filter
    )

    if devices:
        for device_obj in devices.values():
            if not hasattr(device_obj, "get_controls"):
                continue
            try:
                if asyncio.iscoroutinefunction(device_obj.get_controls):
                    await device_obj.get_controls()
                else:
                    device_obj.get_controls()
            except Exception as e:
                typer.echo(
                    f"Warning: Could not fully fetch details for {getattr(device_obj, 'name', device_obj.server_name)} (IP: {getattr(device_obj, 'ipv4', 'N/A')}). It might be offline or unresponsive. Error: {type(e).__name__}",
                    err=True,
                )

    return dict(
        sorted(
            devices.items(),
            key=lambda item: (
                item[1].name if hasattr(item[1], "name") and item[1].name else item[0]
            ),
        )
    )


@app.command("list")
def list_devices_command(
    json_output: Annotated[
        bool, typer.Option("--json", help="Output as JSON.")
    ] = False,
    host: Annotated[
        str | None, typer.Option(help="Filter devices by host name or IP address.")
    ] = None,
    name: Annotated[
        str | None, typer.Option(help="Filter devices by device name.")
    ] = None,
):
    """List available Dante devices."""
    if name and host:
        typer.echo(
            "Error: Cannot use --name and --host filters simultaneously.", err=True
        )
        raise typer.Exit(code=1)

    devices_data = asyncio.run(
        fetch_and_prepare_devices(name_filter=name, host_filter=host)
    )

    if not devices_data:
        typer.echo("No devices found.")
        return

    if json_output:
        typer.echo(json.dumps(devices_data, indent=2, cls=CustomJSONEncoder))
    else:
        for server_name, device in devices_data.items():
            display_name = (
                device.name if hasattr(device, "name") and device.name else None
            ) or server_name

            typer.secho(f"Device: {display_name}", fg=typer.colors.GREEN, bold=True)

            if server_name != display_name:
                typer.echo(f"  Server Name: {server_name}")

            if hasattr(device, "ipv4") and device.ipv4:
                typer.echo(f"  IP Address: {device.ipv4}")

            if hasattr(device, "model_id") and device.model_id:
                typer.echo(f"  Model ID: {device.model_id}")

            typer.echo("---")


async def get_filtered_devices(
    device_name_filter: str | None,
    device_host_filter: str | None,
    mdns_timeout: float = 1.5,
) -> dict[str, DanteDevice]:
    dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)
    devices = await dante_browser.get_devices()

    if not isinstance(devices, dict):
        devices = {}

    for device_obj_for_controls in devices.values():
        if hasattr(device_obj_for_controls, "get_controls"):
            try:
                if asyncio.iscoroutinefunction(device_obj_for_controls.get_controls):
                    await device_obj_for_controls.get_controls()
                else:
                    device_obj_for_controls.get_controls()
            except Exception:
                pass

    return filter_devices_util(
        devices, name_filter=device_name_filter, host_filter=device_host_filter
    )


async def get_target_device(
    device_name: str | None, device_host: str | None
) -> DanteDevice | None:
    if not device_name and not device_host:
        typer.echo("Error: Must specify --device-name or --device-host.", err=True)
        raise typer.Exit(code=1)
    if device_name and device_host:
        typer.echo(
            "Error: Cannot use --device-name and --device-host simultaneously.",
            err=True,
        )
        raise typer.Exit(code=1)

    filtered_devices = await get_filtered_devices(
        device_name_filter=device_name, device_host_filter=device_host
    )

    if not filtered_devices:
        identifier = device_name or device_host

        if identifier:
            is_potential_ip = False

            try:
                ipaddress.ip_address(identifier)
                is_potential_ip = True
            except ValueError:
                pass

            resolved_ip_by_identifier = get_host_by_name_util(identifier)

            dante_browser = DanteBrowser(mdns_timeout=app_settings.mdns_timeout)
            all_devices = await dante_browser.get_devices()

            if isinstance(all_devices, dict):
                candidate_device = None
                target_ip_to_check = None

                if is_potential_ip:
                    target_ip_to_check = ipaddress.ip_address(identifier)
                elif resolved_ip_by_identifier:
                    target_ip_to_check = resolved_ip_by_identifier

                if target_ip_to_check:
                    for _, dev in all_devices.items():
                        if (
                            hasattr(dev, "ipv4")
                            and dev.ipv4
                            and ipaddress.ip_address(dev.ipv4) == target_ip_to_check
                        ):
                            candidate_device = dev
                            break

                if not candidate_device and not is_potential_ip:
                    for _, dev in all_devices.items():
                        if (
                            hasattr(dev, "name")
                            and dev.name
                            and dev.name.lower() == identifier.lower()
                        ):
                            candidate_device = dev
                            break

                        if (
                            not candidate_device
                            and hasattr(dev, "server_name")
                            and dev.server_name
                        ):
                            possible_server_names = {
                                identifier.lower(),
                                identifier.lower() + ".local.",
                                identifier.lower() + ".local",
                            }

                            if dev.server_name.lower() in possible_server_names:
                                candidate_device = dev
                                break

                if candidate_device and hasattr(candidate_device, "server_name"):
                    filtered_devices = {
                        getattr(candidate_device, "server_name"): candidate_device
                    }

    if not filtered_devices:
        typer.echo(
            f"Error: Device not found ('{device_name or device_host}').", err=True
        )

        return None

    if len(filtered_devices) > 1:
        typer.echo(
            f"Error: Multiple devices found for '{device_name or device_host}'. Please be more specific.",
            err=True,
        )

        return None

    device = list(filtered_devices.values())[0]

    if hasattr(device, "get_controls") and asyncio.iscoroutinefunction(
        device.get_controls
    ):
        await device.get_controls()
    elif hasattr(device, "get_controls"):
        device.get_controls()
    return device


async def _async_identify_logic(
    device_name: str | None,
    device_host: str | None,
):
    target_device = await get_target_device(device_name, device_host)

    if not target_device:
        raise typer.Exit(code=1)

    if hasattr(target_device, "identify") and asyncio.iscoroutinefunction(
        target_device.identify
    ):
        await target_device.identify()
    else:
        typer.echo(
            "Error: Device object does not support 'identify' or it's not async.",
            err=True,
        )

        raise typer.Exit(code=1)
    typer.secho(
        f"Identify command sent to '{target_device.name}'. Check the device.",
        fg=typer.colors.GREEN,
    )


@app.command("identify", help="Identify the device by flashing an LED.")
def identify(
    device_name: Annotated[
        str | None, typer.Option(help="Target device by its current name.")
    ] = None,
    device_host: Annotated[
        str | None, typer.Option(help="Target device by its host name or IP address.")
    ] = None,
):
    try:
        asyncio.run(_async_identify_logic(device_name, device_host))
    except typer.Exit:
        raise
    except RuntimeError as e:
        if "cannot be called when another asyncio event loop is running" in str(e):
            typer.echo(
                "Error: asyncio.run() cannot be called when another event loop is running.",
                err=True,
            )
            raise typer.Exit(code=120)
        raise
