# standard libraries
from typing import List

# third party libraries
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import FilePath

# pydf libraries
from df import models as dm


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
        self.project_id = project_id
        self.location_id = location_id
        self._parent = f"projects/{project_id}/locations/{location_id}"

    def create_job(self) -> dm.Job:
        pass

    def list_data_pipelines(self) -> List[dm.DataPipeline]:
        res = self._dp_service.projects().locations().listPipelines(parent=self._parent).execute()
        pipelines = []
        for one_res in res["pipelines"]:
            one_p = dm.DataPipeline(
                name=one_res["name"],
                display_name=one_res.get("displayName", None),
                type=one_res.get("type", None),
                state=one_res.get("state", None),
            )
            one_p._api_results = one_res
            one_p._df = self
            pipelines.append(one_p)
        return pipelines
