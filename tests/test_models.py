# pydf libraries
from df import models as dm


def test_models():
    job = dm.Job(name="test")
    assert job.name == "test"
