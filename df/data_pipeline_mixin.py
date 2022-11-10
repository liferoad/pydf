class _DataPipelineMixin:
    """Mixin for DataPipeline"""

    @property
    def raw_results(self):
        return self._api_results

    def delete(self) -> None:
        """Delete a data pipeline"""
        res = self._df._dp_service.projects().locations().pipelines().delete(name=self.name).execute()
        if res:
            raise RuntimeError(f"Cannot delete the data pipeline {self.name}. Error: {res}.")
