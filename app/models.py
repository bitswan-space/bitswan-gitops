from datetime import datetime
from pydantic import BaseModel
import json


class ContainerProperties(BaseModel):
    container_id: str | None
    endpoint_name: str | None
    created: datetime | None
    name: str
    state: str | None
    status: str | None
    deployment_id: str

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class Pipeline(BaseModel):
    wires: list
    properties: ContainerProperties
    metrics: list


class Topology(BaseModel):
    topology: dict[str, Pipeline]
    display_style: str


def encode_pydantic_model(data: BaseModel) -> bytearray:
    json_str = data.model_dump_json()
    return bytearray(json_str.encode("utf-8"))
