#! /usr/bin/env -S uv run --script
#
# /// script
# dependencies = ["aiobotocore", "anyio", "attrs", "rich", "uvloop"]
# ///

import datetime as dt
from datetime import UTC
from typing import TYPE_CHECKING, Self

import anyio
from aiobotocore.session import get_session
from anyio import create_memory_object_stream, create_task_group
from attrs import field, frozen
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

try:
    from rich import get_console
except ModuleNotFoundError:
    pass
else:
    print = get_console().out  # noqa: A001
    del get_console

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream
    from types_aiobotocore_ec2.literals import InstanceTypeType
    from types_aiobotocore_ec2.type_defs import SpotPriceTypeDef

INSTANCE_TYPES: list[InstanceTypeType] = ["c8g.48xlarge", "c8g.metal-48xl"]
MAX_PRICE = 1.0


@frozen(order=True)
class Pricing:
    zone_id: str
    instance_type: InstanceTypeType
    spot_price: float = field(converter=float)

    @classmethod
    def from_dict(cls, data: SpotPriceTypeDef) -> Self:
        return cls(zone_id=data["AvailabilityZoneId"], instance_type=data["InstanceType"], spot_price=data["SpotPrice"])  # pyright: ignore[reportTypedDictNotRequiredAccess]


async def query_region(region: str, progress: Progress, output: MemoryObjectSendStream[Pricing]) -> None:
    task = progress.add_task(region, total=None)

    async with aws.create_client("ec2", region) as ec2, output:  # pyright: ignore[reportUnknownMemberType]
        paginator = ec2.get_paginator("describe_spot_price_history")
        async for page in paginator.paginate(
            StartTime=dt.datetime.now(UTC), InstanceTypes=INSTANCE_TYPES, ProductDescriptions=["Linux/UNIX"]
        ):
            for pricing in map(Pricing.from_dict, page["SpotPriceHistory"]):
                await output.send(pricing)

        progress.update(task, visible=False, refresh=True)


async def main() -> None:
    REGIONS = await aws.get_available_regions("ec2")

    send_stream, receive_stream = create_memory_object_stream[Pricing]()

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=Console(stderr=True)
    ) as progress:
        async with create_task_group() as tg, receive_stream:
            for region in REGIONS:
                tg.start_soon(query_region, region, progress, send_stream.clone())
            await send_stream.aclose()

            results = [pricing async for pricing in receive_stream if pricing.spot_price <= MAX_PRICE]

    results.sort()  # pyright: ignore[reportPossiblyUnboundVariable]
    for pricing in results:  # pyright: ignore[reportPossiblyUnboundVariable]
        print(pricing)


if __name__ == "__main__":
    aws = get_session()
    anyio.run(main, backend_options={"use_uvloop": True})
