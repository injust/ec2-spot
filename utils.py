from typing import TYPE_CHECKING, Self

from attrs import field, frozen
from cattrs import Converter

if TYPE_CHECKING:
    from types_aiobotocore_ec2.literals import InstanceTypeType
    from types_aiobotocore_ec2.type_defs import SpotPriceTypeDef
else:
    type InstanceTypeType = str


@frozen(order=True)
class Pricing:
    zone_id: str = field(alias="AvailabilityZoneId")
    instance_type: InstanceTypeType = field(alias="InstanceType")
    spot_price: float = field(alias="SpotPrice")

    @classmethod
    def from_dict(cls, data: SpotPriceTypeDef) -> Self:
        converter = Converter(use_alias=True)
        return converter.structure(data, cls)

    @property
    def region_id(self) -> str:
        return self.zone_id.split("-")[0]

    @property
    def instance_size(self) -> str:
        return self.instance_type.split(".")[1]

    @property
    def cpu_count(self) -> int:
        match self.instance_size:
            case "medium":
                return 1
            case "large":
                return 2
            case "xlarge":
                return 4
            case "2xlarge":
                return 8
            case "4xlarge":
                return 16
            case "8xlarge":
                return 32
            case "12xlarge":
                return 48
            case "16xlarge":
                return 64
            case "24xlarge" | "metal-24xl":
                return 96
            case "48xlarge" | "metal-48xl":
                return 192
            case _:
                raise ValueError(self.instance_type)
