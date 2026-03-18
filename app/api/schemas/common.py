from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    database: str
    code_version: str
