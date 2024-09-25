from datetime import datetime
from pydantic import BaseModel

class ContainerProperties(BaseModel):
    container_id: str
    endpoint_name: str
    created: datetime
    name: str
    state: str
    status: str
    deployment_id: str
