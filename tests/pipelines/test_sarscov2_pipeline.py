import pytest

from cherami.pipelines.implementations import SARSCoV2Pipeline


@pytest.fixture
def sarscov2_pipeline():
    return SARSCoV2Pipeline()


def test_sarscov2_pipeline_generate_samplesheet(sarscov2_pipeline):
    ...
    ## TODO: test would go here for method
