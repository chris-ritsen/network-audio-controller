import os

from typing import Any
from json import JSONEncoder

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from netaudio.dante.browser import DanteBrowser

# from netaudio.dante.cache import DanteCache


def _default(self: Any, obj: Any) -> Any:
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default

async def subscription_list(
        json: bool = False
) -> None:
    """
    List all subscriptions.
    """
    subscriptions = []

    redis_enabled = None

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
        redis_enabled = False

    if redis_enabled:
        # dante_cache = DanteCache()
        devices = await dante_cache.get_devices()
        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))
    else:
        dante_browser = DanteBrowser(mdns_timeout=1.5)
        devices = await dante_browser.get_devices()
        devices = dict(sorted(devices.items(), key=lambda x: x[1].name))

        for _, device in devices.items():
            await device.get_controls()

    #  rx_channel = None
    #  rx_device = None
    #  tx_channel = None
    #  tx_device = None

    #  if self.option('tx-device-name'):
    #      tx_device = next(filter(lambda d: d[1].name == self.option('tx-device-name'), devices.items()))[1]
    #  elif self.option('tx-device-host'):
    #      tx_device = next(filter(lambda d: d[1].ipv4 == self.option('tx-device-host'), devices.items()))[1]

    #  if self.option('tx-channel-name'):
    #      tx_channel = next(filter(lambda c: c[1].name == self.option('tx-channel-name'), tx_device.tx_channels.items()))[1]
    #  elif self.option('tx-channel-number'):
    #      tx_channel = next(filter(lambda c: c[1].number == self.option('tx-channel-number'), tx_device.tx_channels.items()))[1]

    #  if self.option('rx-device-name'):
    #      rx_device = next(filter(lambda d: d[1].name == self.option('rx-device-name'), devices.items()))[1]
    #  elif self.option('rx-device-host'):
    #      rx_device = next(filter(lambda d: d[1].ipv4 == self.option('rx-device-host'), devices.items()))[1]

    #  if self.option('rx-channel-name'):
    #      rx_channel = next(filter(lambda c: c[1].name == self.option('rx-channel-name'), rx_device.rx_channels.items()))[1]
    #  elif self.option('rx-channel-number'):
    #      rx_channel = next(filter(lambda c: c[1].number == self.option('rx-channel-number'), rx_device.rx_channels.items()))[1]

    for _, device in devices.items():
        for subscription in device.subscriptions:
            subscriptions.append(subscription)

    if json:
        json_object = json.dumps(subscriptions, indent=2)
        print(f"{json_object}")
    else:
        for subscription in subscriptions:
            print(f"{subscription}")
