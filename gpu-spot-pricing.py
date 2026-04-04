#! /usr/bin/env -S uv run --script
#
# /// script
# dependencies = ["aiobotocore", "anyio", "attrs", "cattrs", "rich", "uvloop"]
# ///

import datetime as dt
from datetime import UTC
from enum import StrEnum, auto
from itertools import groupby
from typing import TYPE_CHECKING

import anyio
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from anyio import create_memory_object_stream, create_task_group
from attrs import frozen
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

G6E_G5_PERF_RATIO = 2.35
MAX_GPU_HOUR_PRICE = 0.48  # G6e


class InstanceFamily(StrEnum):
    G5 = auto()
    G6E = auto()


@frozen(order=True)
class GpuPricing(Pricing):
    @property
    def instance_family(self) -> InstanceFamily:
        return InstanceFamily(self.instance_type.split(".")[0])

    @property
    def gpu_count(self) -> int:
        match self.instance_size:
            case "xlarge" | "2xlarge" | "4xlarge" | "8xlarge" | "16xlarge":
                return 1
            case "12xlarge" | "24xlarge":
                return 4
            case "48xlarge":
                return 8
            case _:
                raise ValueError(self.instance_type)


async def query_region(region: str, progress: Progress, output: MemoryObjectSendStream[GpuPricing]) -> None:
    task = progress.add_task(region)

    async with aws.create_client("ec2", region, config=config) as ec2, output:  # pyright: ignore[reportUnknownMemberType]
        try:
            paginator = ec2.get_paginator("describe_spot_price_history")
            async for page in paginator.paginate(
                StartTime=dt.datetime.now(UTC),
                ProductDescriptions=["Linux/UNIX"],
                Filters=[{"Name": "instance-type", "Values": [f"{family}.*" for family in InstanceFamily]}],
            ):
                for pricing in map(GpuPricing.from_dict, page["SpotPriceHistory"]):
                    await output.send(pricing)
        except ConnectionError:
            progress.update(task, completed=100, refresh=True)
        else:
            progress.update(task, visible=False, refresh=True)


async def main() -> None:
    REGIONS = await aws.get_available_regions("ec2")

    results: list[GpuPricing] = []
    send_stream, receive_stream = create_memory_object_stream[GpuPricing]()

    with Progress(
        SpinnerColumn(finished_text="[red]"),
        TextColumn("[progress.description]{task.description}"),
        console=Console(stderr=True),
    ) as progress:
        async with create_task_group() as tg, receive_stream:
            for region in REGIONS:
                tg.start_soon(query_region, region, progress, send_stream.clone())
            await send_stream.aclose()

            async for pricing in receive_stream:
                max_price = pricing.gpu_count * MAX_GPU_HOUR_PRICE
                match pricing.instance_family:
                    case InstanceFamily.G5:
                        max_price /= G6E_G5_PERF_RATIO
                    case InstanceFamily.G6E:
                        pass

                if pricing.spot_price <= max_price:
                    results.append(pricing)

    results.sort()
    table = Table("Availability zone", "Instance type", "¢/hr", "¢/GPU-hr", "CPUs", "GPUs", highlight=True)
    for _, group in groupby(results, lambda pricing: pricing.region_id):
        for pricing in group:
            table.add_row(
                pricing.zone_id,
                pricing.instance_type,
                f"¢{pricing.spot_price * 100:.2f}",
                f"¢{pricing.spot_price * 100 / pricing.gpu_count:.2f}",
                str(pricing.cpu_count),
                str(pricing.gpu_count),
            )
        table.add_section()
    print(table)


if __name__ == "__main__":
    aws = get_session()
    config = AioConfig(connect_timeout=5, retries={"total_max_attempts": 1})
    anyio.run(main, backend_options={"use_uvloop": True})
