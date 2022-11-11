# third party libraries
from pydantic import BaseModel, Field

GCS_PATH_FOR_TEMPLATES = "gs://dataflow-templates/latest"


def _get_template_path(template_name: str) -> str:
    return GCS_PATH_FOR_TEMPLATES + "/" + template_name


class Environment(BaseModel):
    # https://cloud.google.com/compute/docs/regions-zones#available
    zone: str = Field("us-central1-f", description="GCE zone")


class WordCountParameters(BaseModel):
    inputFile: str = Field(..., description="input file")
    output: str = Field(..., description="output file")


class WordCountTemplate(BaseModel):
    # Details: https://cloud.google.com/dataflow/docs/guides/templates/provided-templates#running-the-wordcount-template
    gcsPath: str = Field(_get_template_path("Word_Count"), description="Word_Count template location")
    jobName: str = Field(..., description="a Dataflow job name")
    parameters: WordCountParameters = Field(..., description="parameters for Word_Count")
    environment: Environment = Field(Environment(), description="computing environment")
