# standard libraries
from typing import Any, Optional

# third party libraries
from pydantic import BaseModel, Field

# pydf libraries
from df.data_pipeline_mixin import _DataPipelineMixin


class Job(BaseModel):
    """Dataflow job information"""

    name: str = Field(..., description="Dataflow job name")


class DataPipeline(BaseModel, _DataPipelineMixin):
    """Data Pipeline information"""

    short_name: str = Field(..., description="Data pipeline short name without project and location ids")
    name: Optional[str] = Field(None, description="Data pipeline full name")
    display_name: Optional[str] = Field(None, description="Display name for a data pipeline")
    type: Optional[str] = Field(None, description="Data pipeline type")
    state: Optional[str] = Field(None, description="Data pipeline state")

    _api_results: dict = Field(None, description="a private field to store the api results")
    _df: Any = Field(None, description="a private field to store the Dataflow object")

    class Config:
        underscore_attrs_are_private = True
