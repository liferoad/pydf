# standard libraries
from typing import Dict, List, Union

# third party libraries
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import FilePath

# pydf libraries
from df import models as dm
from df import options as op


class Dataflow:
    """Dataflow project level class"""

    def __init__(self, project_id: str, location_id: str, service_account_file: FilePath) -> None:
        """Initialize a Dataflow class with the service information

        Args:
            project_id (str): a GCP project id
            location_id (str): a Dataflow service region
            service_account_file (FilePath): a service account file location
        """
        self._credentials = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._dp_service = build("datapipelines", "v1", credentials=self._credentials, cache_discovery=False)
        self._df_service = build("dataflow", "v1b3", credentials=self._credentials, cache_discovery=False)
        self.project_id = project_id
        self.location_id = location_id
        self._parent = f"projects/{project_id}/locations/{location_id}"

    def create_job(self, body: Dict) -> dm.Job:
        """Create a Dataflow job

        Args:
            body (Dict): information for a Dataflow job to be created

        Returns:
            dm.Job: a updated Dataflow Job object
        """
        response = (
            self._df_service.projects().locations().jobs().create(project_id=self.project_id, body=body).execute()
        )
        one_job = dm.Job(name=response["name"], id=response["id"])
        one_job._api_results = response
        one_job._df = self
        return one_job

    def create_job_from_template(self, template: Union[op.OptionBuilder, Dict]) -> dm.Job:

        """Create a job from a Classic template

        Args:
            job_name(str): a Dataflow job name
            template (Dict or one of predefined templates): template information

        Returns:
            dm.Job: a created Dataflow job
        """

        if isinstance(template, Dict):
            body = template.copy()
            gcs_path = body.pop("gcsPath", None)
        elif isinstance(template, op.OptionBuilder):
            body = template.body
            gcs_path = body.pop("gcsPath", None)

        response = (
            self._df_service.projects()
            .templates()
            .launch(projectId=self.project_id, gcsPath=gcs_path, body=body)
            .execute()
        )

        one_job = dm.Job(name=response["job"]["name"], id=response["job"]["id"])
        one_job._api_results = response
        one_job._df = self

        return one_job

    def list_jobs(self) -> List[dm.Job]:
        response = (
            self._df_service.projects()
            .locations()
            .jobs()
            .list(projectId=self.project_id, location=self.location_id)
            .execute()
        )
        jobs = []
        for one_res in response["jobs"]:
            one_job = dm.Job(name=one_res["name"], id=one_res["id"])
            one_job._api_results = one_res
            one_job._df = self
            jobs.append(one_job)
        return jobs

    def list_data_pipelines(self) -> List[dm.DataPipeline]:
        res = self._dp_service.projects().locations().listPipelines(parent=self._parent).execute()
        pipelines = []
        for one_res in res["pipelines"]:
            one_p = dm.DataPipeline(
                short_name=one_res["name"].split("/")[-1],
                name=one_res["name"],
                display_name=one_res.get("displayName", None),
                type=one_res.get("type", None),
                state=one_res.get("state", None),
            )
            one_p._api_results = one_res
            one_p._df = self
            pipelines.append(one_p)
        return pipelines

    def create_data_pipeline(self, data_pipeline: dm.DataPipeline) -> dm.DataPipeline:
        """Create a data pipeline

        Args:
            data_pipeline (dm.DataPipeline): information for a data pipeline to be created

        Returns:
            dm.DataPipeline: a updated DataPipeline object
        """
        if not data_pipeline.name:
            data_pipeline.name = f"{self._parent}/pipelines/{data_pipeline.short_name}"

        res = (
            self._dp_service.projects()
            .locations()
            .pipelines()
            .create(parent=self._parent, body=dict(name=data_pipeline.name))
            .execute()
        )
        one_dp = dm.DataPipeline(
            short_name=data_pipeline.short_name,
            name=res["name"],
            display_name=res.get("displayName", None),
            type=res.get("type", None),
            state=res.get("state", None),
        )
        one_dp._api_results = res
        one_dp._df = self
        return one_dp
