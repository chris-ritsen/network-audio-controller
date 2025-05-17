import asyncio

import typer
from typing_extensions import Annotated

from ._add import subscription_add
from ._list import subscription_list
from ._remove import subscription_remove

app = typer.Typer(
    name="subscription", help="Control subscriptions", no_args_is_help=True
)


@app.command(name="add", help="Add a subscription between two channels.")
async def add_subscription_command(
    rx_channel_name: Annotated[
        str, typer.Option(help="Specify Rx channel by name")
    ] = None,
    rx_channel_number: Annotated[
        str, typer.Option(help="Specify Rx channel by number")
    ] = None,
    rx_device_host: Annotated[
        str, typer.Option(help="Specify Rx device by host")
    ] = None,
    rx_device_name: Annotated[
        str, typer.Option(help="Specify Rx device by name")
    ] = None,
    tx_channel_name: Annotated[
        str, typer.Option(help="Specify Tx channel by name")
    ] = None,
    tx_channel_number: Annotated[
        str, typer.Option(help="Specify Tx channel by number")
    ] = None,
    tx_device_host: Annotated[
        str, typer.Option(help="Specify Tx device by host")
    ] = None,
    tx_device_name: Annotated[
        str, typer.Option(help="Specify Tx device by name")
    ] = None,
):
    await subscription_add(
        rx_channel_name=rx_channel_name,
        rx_channel_number=rx_channel_number,
        rx_device_host=rx_device_host,
        rx_device_name=rx_device_name,
        tx_channel_name=tx_channel_name,
        tx_channel_number=tx_channel_number,
        tx_device_host=tx_device_host,
        tx_device_name=tx_device_name,
    )


@app.command(name="list", help="List active subscriptions.")
async def list_subscriptions_command(
    json_output: Annotated[bool, typer.Option("--json", help="Output as JSON")] = False,
):
    await subscription_list(json_output=json_output)


@app.command(name="remove", help="Remove a subscription from a receiver channel.")
async def remove_subscription_command(
    rx_channel_name: Annotated[str, typer.Option(help="Receiver channel name")] = None,
    rx_channel_number: Annotated[
        str, typer.Option(help="Receiver channel number")
    ] = None,
    rx_device_host: Annotated[
        str, typer.Option(help="Receiver device hostname or IP")
    ] = None,
    rx_device_name: Annotated[str, typer.Option(help="Receiver device name")] = None,
):
    await subscription_remove(
        rx_channel_name=rx_channel_name,
        rx_channel_number=rx_channel_number,
        rx_device_host=rx_device_host,
        rx_device_name=rx_device_name,
    )
