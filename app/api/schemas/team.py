from datetime import datetime

from pydantic import BaseModel, Field


class TeamCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=4096)
    is_active: bool = True


class TeamUpdate(BaseModel):
    slug: str | None = Field(default=None, min_length=1, max_length=128)
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=4096)
    is_active: bool | None = None


class TeamRead(BaseModel):
    id: str
    slug: str
    name: str
    description: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class TeamListResponse(BaseModel):
    items: list[TeamRead]
