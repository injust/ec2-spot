#! /usr/bin/env -S uv run --script
#
# /// script
# dependencies = ["aiobotocore", "anyio", "attrs", "cattrs", "rich", "uvloop"]
# ///

import datetime as dt
from datetime import UTC
from itertools import groupby
from typing import TYPE_CHECKING

import anyio
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from anyio import create_memory_object_stream, create_task_group
from botocore.exceptions import ConnectionError  # noqa: A004
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from utils import Pricing

try:
    from rich import get_console
except ModuleNotFoundError:
    pass
else:
    print = get_console().print  # noqa: A001
    del get_console

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream
    from types_aiobotocore_ec2.literals import InstanceTypeType

INSTANCE_TYPES: list[InstanceTypeType] = ["c8g.48xlarge", "c8g.metal-48xl"]
MAX_PRICE = 1.0


async def query_region(region: str, progress: Progress, output: MemoryObjectSendStream[Pricing]) -> None:
    task = progress.add_task(region)

    async with aws.create_client("ec2", region, config=config) as ec2, output:  # pyright: ignore[reportUnknownMemberType]
        try:
            paginator = ec2.get_paginator("describe_spot_price_history")
            async for page in paginator.paginate(
                StartTime=dt.datetime.now(UTC), InstanceTypes=INSTANCE_TYPES, ProductDescriptions=["Linux/UNIX"]
            ):
                for pricing in map(Pricing.from_dict, page["SpotPriceHistory"]):
                    await output.send(pricing)
        except ConnectionError:
            progress.update(task, completed=100, refresh=True)
        else:
            progress.update(task, visible=False, refresh=True)


async def main() -> None:
    REGIONS = await aws.get_available_regions("ec2")

    send_stream, receive_stream = create_memory_object_stream[Pricing]()

    with Progress(
        SpinnerColumn(finished_text="[red]"),
        TextColumn("[progress.description]{task.description}"),
        console=Console(stderr=True),
    ) as progress:
        async with create_task_group() as tg, receive_stream:
            for region in REGIONS:
                tg.start_soon(query_region, region, progress, send_stream.clone())
            await send_stream.aclose()

            results = [pricing async for pricing in receive_stream if pricing.spot_price <= MAX_PRICE]

    results.sort()  # pyright: ignore[reportPossiblyUnboundVariable]
    table = Table("Availability zone", "Instance type", "¢/hr", "¢/CPU-hr", "CPUs", highlight=True)
    for _, group in groupby(results, lambda pricing: pricing.region_id):  # pyright: ignore[reportPossiblyUnboundVariable]
        for pricing in group:
            table.add_row(
                pricing.zone_id,
                pricing.instance_type,
                f"¢{pricing.spot_price * 100:.2f}",
                f"¢{pricing.spot_price * 100 / pricing.cpu_count:.2f}",
                str(pricing.cpu_count),
            )
        table.add_section()
    print(table)


if __name__ == "__main__":
    aws = get_session()
    config = AioConfig(connect_timeout=5, retries={"total_max_attempts": 1})
    anyio.run(main, backend_options={"use_uvloop": True})
