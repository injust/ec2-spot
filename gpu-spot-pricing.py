#! /usr/bin/env -S uv run --script
#
# /// script
# dependencies = ["aiobotocore", "anyio", "attrs", "rich", "uvloop"]
# ///

import datetime as dt
from datetime import UTC
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, Self

import anyio
from aiobotocore.session import get_session
from anyio import create_memory_object_stream, create_task_group
from attrs import field, frozen

try:
    from rich.console import Console
except ModuleNotFoundError:
    pass
else:
    print = Console().out  # noqa: A001

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream
    from types_aiobotocore_ec2.literals import InstanceTypeType
    from types_aiobotocore_ec2.type_defs import SpotPriceTypeDef

    type JSON = dict[str, Any]

G6E_G5_PERF_RATIO = 2.35
MAX_PRICE_PER_GPU_HOUR = 0.49


class InstanceFamily(StrEnum):
    G5 = auto()
    G6E = auto()


@frozen
class Pricing:
    instance_type: InstanceTypeType
    zone_id: str
    spot_price: float = field(converter=float)

    @classmethod
    def from_dict(cls, data: SpotPriceTypeDef) -> Self:
        return cls(instance_type=data["InstanceType"], zone_id=data["AvailabilityZoneId"], spot_price=data["SpotPrice"])  # pyright: ignore[reportTypedDictNotRequiredAccess]

    @property
    def instance_family(self) -> InstanceFamily:
        return InstanceFamily(self.instance_type.split(".")[0])

    @property
    def instance_size(self) -> str:
        return self.instance_type.split(".")[1]

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


async def query_region(region: str, output: MemoryObjectSendStream[Pricing]) -> None:
    async with aws.create_client("ec2", region) as ec2, output:  # pyright: ignore[reportUnknownMemberType]
        paginator = ec2.get_paginator("describe_spot_price_history")
        async for page in paginator.paginate(
            StartTime=dt.datetime.now(UTC),
            ProductDescriptions=["Linux/UNIX"],
            Filters=[{"Name": "instance-type", "Values": [f"{family}.*" for family in InstanceFamily]}],
        ):
            for pricing in map(Pricing.from_dict, page["SpotPriceHistory"]):
                await output.send(pricing)


async def main() -> None:
    REGIONS = await aws.get_available_regions("ec2")

    send_stream, receive_stream = create_memory_object_stream[Pricing]()
    async with create_task_group() as tg, receive_stream:
        for region in REGIONS:
            tg.start_soon(query_region, region, send_stream.clone())
        await send_stream.aclose()

        async for pricing in receive_stream:
            max_price = pricing.gpu_count * MAX_PRICE_PER_GPU_HOUR
            match pricing.instance_family:
                case InstanceFamily.G5:
                    max_price /= G6E_G5_PERF_RATIO
                case InstanceFamily.G6E:
                    pass

            if pricing.spot_price <= max_price:
                print(pricing)


if __name__ == "__main__":
    aws = get_session()
    anyio.run(main, backend_options={"use_uvloop": True})
