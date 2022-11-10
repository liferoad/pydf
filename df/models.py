# third party libraries
from pydantic import BaseModel, Field


class Job(BaseModel):
    """Dataflow job information"""

    name: str = Field(..., description="Dataflow job name")


class DataPipeline(BaseModel):
    """Data Pipeline information"""

    _api_results: str

    class Config:
        underscore_attrs_are_private = True
