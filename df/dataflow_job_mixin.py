class _DataflowJobMixin:
    """Mixin for Dataflow Job"""

    @property
    def raw_results(self):
        return self._api_results

    def cancel(self) -> "Job":  # noqa F821
        """Cancel a Dataflow Job"""
        response = (
            self._df._df_service.projects()
            .locations()
            .jobs()
            .update(
                projectId=self._df.project_id,
                location=self._df.location_id,
                jobId=self.id,
                body={"requestedState": "JOB_STATE_CANCELLED"},
            )
            .execute()
        )

        one_job = self.copy()
        one_job._api_results = response
        return one_job
