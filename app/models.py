from datetime import datetime
from pydantic import BaseModel, Field


class ContainerProperties(BaseModel):
    container_id: str | None = Field(alias="container-id", default=None)
    endpoint_name: str | None = Field(alias="endpoint-name", default=None)
    created_at: datetime | None = Field(alias="created-at", default=None)
    name: str
    state: str | None
    status: str | None
    deployment_id: str | None = Field(alias="deployment-id", default=None)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
        populate_by_name = True


class Pipeline(BaseModel):
    wires: list
    properties: ContainerProperties
    metrics: list


class Topology(BaseModel):
    topology: dict[str, Pipeline]
    display_style: str


class DeployedPRE(BaseModel):
    container_id: str | None
    endpoint_name: str | None
    created_at: datetime | None
    name: str
    state: str | None
    status: str | None
    deployment_id: str | None
    active: bool


def encode_pydantic_model(data: BaseModel) -> bytearray:
    json_str = data.model_dump_json(by_alias=True)
    return bytearray(json_str.encode("utf-8"))
