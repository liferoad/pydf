# third party libraries
from pydantic import BaseModel, Field


class Job(BaseModel):
    """Dataflow job information"""

    name: str = Field(..., description="Dataflow job name")
