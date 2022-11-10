# third party libraries
from pydantic import BaseModel, Field


class Job(BaseModel):
    """Dataflow job information"""

    name: str = Field(..., description="Dataflow job name")


class DataPipeline(BaseModel):
    """Data Pipeline information"""

    name: str = Field(..., description="Data pipeline name")

    _api_results: dict

    class Config:
        underscore_attrs_are_private = True
