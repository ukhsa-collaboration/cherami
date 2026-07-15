import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from cherami.pipelines.strep_pneumo import StrepPneumoPipeline


@pytest.fixture
def mock_config(mocker):
    return mocker.Mock()


@pytest.fixture
def strep_pipeline(mock_config, global_config):
    pipeline = StrepPneumoPipeline(mock_config, global_config)
    return pipeline


@pytest.fixture
def claspar_analysis_table_with_strep():
    """Analysis table that has high strep present."""
    return {
        "AID-12345678": {
            "name": "claspar-kraken-bacteria",
            "methods": {
                "versions": [
                    {
                        "name": "classifier_version",
                        "version": "1.0.0",
                    },  # onyx version
                    {
                        "name": "classifier_db_date",
                        "version": "1970-01-01",
                    },  # onyx version
                    {
                        "name": "ncbi_taxonomy_date",
                        "version": "1970-01-01",
                    },  # onyx version
                    {
                        "name": "scylla_version",
                        "version": "1.0.0",
                    },  # onyx version
                    {
                        "name": "sylph_db_version",
                        "version": "1.0.0",
                    },  # onyx version
                    {
                        "name": "alignment_db_version",
                        "version": "1.0.0",
                    },  # onyx version
                    {"name": "module_dependency_db", "version": "2000-01-01"},
                    {"name": "orange_box_version", "version": "1.2.3"},
                ],
                "onyx_versions_hash": "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614",
                "thresholds": {"limit": 10},
            },
            "result_metrics": {
                "0": {
                    "profile": "test_profile_1",
                    "profile_taxon_id": 573,
                    "kraken_confidence": "low",
                    "profile_taxon_match": "Klebsiella pneumoniae",
                },
                "1": {
                    "profile": "Strep",
                    "profile_taxon_id": 1313,
                    "kraken_confidence": "high",
                    "profile_taxon_match": "Streptococcus pneumoniae",
                },
            },
        }
    }


@patch(
    "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
)
# @patch("cherami.pipelines.Pipeline.should_run", return_value=True)
def test_should_run_high_strep(
    mock_onyx,
    strep_pipeline,
    test_context,
    claspar_analysis_table_with_strep,
    caplog,
):
    """Should run - strep found and 'high' confidence"""
    caplog.set_level(logging.DEBUG)
    test_context.onyx_versions_hash = (
        "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614"
    )

    mock_onyx.side_effect = [({}, 0), (claspar_analysis_table_with_strep, 0)]

    assert strep_pipeline.should_run(test_context)
    assert "Decision: run" in caplog.text


@pytest.fixture
def claspar_analysis_table_with_low_strep(claspar_analysis_table_with_strep):
    """Analysis table that has high strep present."""
    low_strep = claspar_analysis_table_with_strep
    low_strep["AID-12345678"]["result_metrics"]["1"]["kraken_confidence"] = (
        "low"
    )
    return low_strep


@patch(
    "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
)
# @patch("cherami.pipelines.Pipeline.should_run", return_value=True)
def test_should_run_low_strep(
    mock_onyx,
    strep_pipeline,
    test_context,
    claspar_analysis_table_with_low_strep,
    caplog,
):
    """Should not run - strep found but 'low' confidence."""
    caplog.set_level(logging.DEBUG)
    test_context.onyx_versions_hash = (
        "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614"
    )

    mock_onyx.side_effect = [
        ({}, 0),
        (claspar_analysis_table_with_low_strep, 0),
    ]

    assert not strep_pipeline.should_run(test_context)
    assert "Decision: not run" in caplog.text


@pytest.fixture
def claspar_analysis_table_no_strep(claspar_analysis_table_with_strep):
    """Analysis table that has high strep present."""
    no_strep = claspar_analysis_table_with_strep
    no_strep["AID-12345678"]["result_metrics"]["1"] = {
        "profile": "test_profile_1",
        "profile_taxon_id": 1496,
        "kraken_confidence": "high",
        "profile_taxon_match": "Clostridioides difficile",
    }
    return no_strep


@patch(
    "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
)
# @patch("cherami.pipelines.Pipeline.should_run", return_value=True)
def test_should_run_no_strep(
    mock_onyx,
    strep_pipeline,
    test_context,
    claspar_analysis_table_no_strep,
    caplog,
):
    """Should not run - no strep found in claspar results."""
    caplog.set_level(logging.DEBUG)
    test_context.onyx_versions_hash = (
        "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614"
    )

    mock_onyx.side_effect = [
        ({}, 0),
        (claspar_analysis_table_no_strep, 0),
    ]

    assert not strep_pipeline.should_run(test_context)
    assert "Decision: not run" in caplog.text


@patch(
    "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
)
def test_how_long_does_it_take(
    mock_onyx,
    test_context,
    strep_pipeline,
    claspar_analysis_table_with_strep,
    caplog,
):
    """How long does it take if the record has loads of bacteria?."""
    caplog.set_level(logging.DEBUG)
    test_context.onyx_versions_hash = (
        "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614"
    )

    big_sample = claspar_analysis_table_with_strep

    taxa = {
        "profile": "test_profile_1",
        "profile_taxon_id": 123456789,
        "kraken_confidence": "low",
        "profile_taxon_match": "some bacteria",
    }
    large_results_metrics = {str(i): taxa for i in range(0, 10000)}
    large_results_metrics["10000"] = {
        "profile": "Strep",
        "profile_taxon_id": 1313,
        "kraken_confidence": "high",
        "profile_taxon_match": "Streptococcus pneumoniae",
    }
    big_sample["AID-12345678"]["result_metrics"] = large_results_metrics

    mock_onyx.side_effect = [
        ({}, 0),
        (big_sample, 0),
    ]

    assert strep_pipeline.should_run(test_context)
    assert "Decision: run" in caplog.text


@patch(
    "onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get",
)
def test_samplesheet(
    mock_onyx, strep_pipeline, test_context, tmp_path, mock_analysis_1
):
    import pandas as pd

    test_context.onyx_versions_hash = "ABC123DEF456"
    test_samplesheet_path = Path(tmp_path / "test_samplesheet.csv")
    mock_onyx.return_value = mock_analysis_1.onyx_record

    strep_pipeline.generate_samplesheet(
        samples=["ID-12345678"],
        job_id="id1234",
        output_filepath=test_samplesheet_path,
        context=test_context,
    )

    # Read in samplesheet:
    samplesheet = pd.read_csv(test_samplesheet_path)
    assert all(
        x
        for x in ["orange_box_version", "climb_id", "fastq_1", "kraken_out"]
        if x in samplesheet.columns.values
    )
