# standard libraries
from typing import Dict, Optional

# third party libraries
from pydantic import BaseModel, Field, root_validator

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


class DataPipelineBuilder(BaseModel):
    """Abstract class to build API options for Data Pipelines"""

    project_id: str = Field(..., description="Project id")
    location_id: str = Field(..., description="a Dataflow service region")
    short_name: str = Field(..., description="Data pipeline short name without project and location ids")

    # https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules#cron_job_format
    scheduler: Optional[str] = Field(None, description="Cron job format")
    # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    time_zone: Optional[str] = Field("America/New_York", description="TZ database time zone name")

    name: Optional[str] = Field(None, description="Data pipeline full name")
    display_name: Optional[str] = Field(None, description="Display name for a data pipeline")
    type: Optional[str] = Field(None, description="Data pipeline type")
    state: Optional[str] = Field(None, description="Data pipeline state")

    @property
    def body(self) -> Dict:
        """Translate options to the API options for Data Pipelines"""
        raise NotImplementedError

    @root_validator(pre=False)
    def _set_fields(cls, values: dict) -> dict:
        """Set name based on the short name when name is None"""
        if not values["name"]:
            values["name"] = "projects/{}/locations/{}/pipelines/{}".format(
                values["project_id"], values["location_id"], values["short_name"]
            )
        if not values["display_name"]:
            values["display_name"] = values["short_name"]
        return values


class WordCountDataPipeline(DataPipelineBuilder):

    gcs_path: str = Field(_get_template_path("Word_Count"), description="Word_Count template location")
    input_file: str = Field(..., description="input file")
    output_file: str = Field(..., description="output file")
    temp_location: str = Field(..., description="temp file location")

    @property
    def body(self) -> Dict:
        """Translate options to the API options for Data Pipelines"""
        payload_body = {
            "name": self.name,
            "displayName": self.display_name,
            "type": "PIPELINE_TYPE_BATCH",
            "workload": {
                "dataflowLaunchTemplateRequest": {
                    "projectId": self.project_id,
                    "gcsPath": self.gcs_path,
                    "launchParameters": {
                        "jobName": self.short_name,
                        "parameters": {"output": self.output_file, "inputFile": self.input_file},
                        "environment": {"tempLocation": self.temp_location},
                    },
                    "location": self.location_id,
                }
            },
        }
        if self.scheduler:
            payload_body["scheduleInfo"] = {
                "schedule": self.scheduler,
                "timeZone": self.time_zone,
            }
        return payload_body
