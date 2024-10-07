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


def encode_pydantic_models(data_list: list[BaseModel]) -> bytearray:
    dict_list = [item.model_dump_json() for item in data_list]
    json_str = json.dumps(dict_list)
    return bytearray(json_str.encode("utf-8"))
