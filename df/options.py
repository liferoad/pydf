# standard libraries
from typing import Dict

# third party libraries
from pydantic import BaseModel, Field

GCS_PATH_FOR_TEMPLATES = "gs://dataflow-templates/latest"


def _get_template_path(template_name: str) -> str:
    return GCS_PATH_FOR_TEMPLATES + "/" + template_name


class Environment(BaseModel):
    # https://cloud.google.com/compute/docs/regions-zones#available
    zone: str = Field("us-central1-f", description="GCE zone")


class OptionBuilder(BaseModel):
    """Abstract class to build options for Dataflow APIs"""

    environment: Environment = Field(Environment(), description="computing environment")

    @property
    def body(self) -> Dict:
        """Translate options to Dataflow API options"""
        raise NotImplementedError


class WordCountTemplate(OptionBuilder):
    # Details: https://cloud.google.com/dataflow/docs/guides/templates/provided-templates#running-the-wordcount-template
    gcs_path: str = Field(_get_template_path("Word_Count"), description="Word_Count template location")
    job_name: str = Field(..., description="a Dataflow job name")
    input_file: str = Field(..., description="input file")
    output_file: str = Field(..., description="output file")

    @property
    def body(self) -> Dict:
        return {
            "gcsPath": self.gcs_path,
            "jobName": self.job_name,
            "parameters": {
                "inputFile": self.input_file,
                "output": self.output_file,
            },
            "environment": self.environment.dict(),
        }
