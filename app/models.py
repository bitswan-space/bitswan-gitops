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
    automation_url: str | None = Field(alias="automation-url", default=None)
    relative_path: str | None = Field(alias="relative-path", default=None)

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


class DeployedAutomation(BaseModel):
    container_id: str | None
    endpoint_name: str | None
    created_at: datetime | None
    name: str
    state: str | None
    status: str | None
    deployment_id: str | None
    active: bool
    automation_url: str | None
    relative_path: str | None
    stage: str | None
    version_hash: str | None = None


class JupyterServer(BaseModel):
    automation_name: str
    session_id: str
    pre_image: str
    token: str


class JupyterServerHeartbeatRequest(BaseModel):
    servers: list[JupyterServer]


class ProcessInfo(BaseModel):
    id: str
    name: str
    attachments: list[str]
    automation_sources: list[str]


class ProcessList(BaseModel):
    processes: dict[str, ProcessInfo]


class ProcessMarkdown(BaseModel):
    content: str


def encode_pydantic_model(data: BaseModel) -> bytearray:
    json_str = data.model_dump_json(by_alias=True)
    return bytearray(json_str.encode("utf-8"))


# =============================================================================
# Infrastructure Service Models
# =============================================================================


class ServiceEnableRequest(BaseModel):
    stage: str = ""
    image: str = ""
    kafka_image: str = ""
    ui_image: str = ""


class ServiceDisableRequest(BaseModel):
    stage: str = ""


class ServiceActionRequest(BaseModel):
    """Request for start/stop/update actions."""
    stage: str = ""
    image: str | None = None


class ServiceStatusResponse(BaseModel):
    service: str
    stage: str = ""
    enabled: bool
    running: bool
    connection_info: dict | None = None


class ServiceBackupRequest(BaseModel):
    stage: str = ""
    backup_path: str


class ServiceRestoreRequest(BaseModel):
    stage: str = ""
    backup_path: str
    force: bool = False
