import logging
import os
from pathlib import Path

import pandas as pd
from onyx import OnyxClient, OnyxConfig, OnyxEnv

from cherami.pipelines.base import Pipeline, PipelineConfig

logger = logging.getLogger(__name__)


class AmrPipeline(Pipeline):
    pipeline_name = "amr-pipeline"

    @property
    def config(self) -> PipelineConfig:
        return PipelineConfig(
            name=self.pipeline_name,
            version="0.1.0",
            path="/shared/team/projects/downstream_orchestration/gpha-mscape-nf-amr",
            cpus=4,
            mem="8G",
            cpu_limit=4,
            mem_limit="8G",
            nf_config_path=Path("/shared/team/projects/downstream_orchestration/gpha-mscape-nf-amr/nextflow.config"),
            nf_profiles=["docker"],
            nf_extra_args=[],
            work_dir=Path("/shared/team/projects/downstream_orchestration/nf_amr/work"),
            output_dir=Path("/shared/team/projects/downstream_orchestration/nf_amr/output"),
            namespace="ns-synthscape-ukhsa",
            container="quay.io/climb-tre/nextflow",
            backoff_limit=5,
            max_retries=1,
            retry_timeout=10,
            job_timeout=3600,
        )

    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        config = OnyxConfig(
            domain=os.environ[OnyxEnv.DOMAIN],
            token=os.environ[OnyxEnv.TOKEN],
        )

        rows = []
        with OnyxClient(config) as client:
            for climb_id in samples:
                try:
                    ## TODO: remove depdency on pandas in future
                    data = pd.DataFrame(client.filter(project="synthscape", climb_id=climb_id))
                    read_1_link = data["human_filtered_reads_1"][0]
                    read_2_link = data["human_filtered_reads_2"][0]
                    taxon_reports_dir = data["taxon_reports"][0]
                    taxon_reports_path = Path(str(taxon_reports_dir))
                    row = {
                        "climb_id": climb_id,
                        "human_filtered_reads_1": str(read_1_link),
                        "human_filtered_reads_2": str(read_2_link),
                        "taxon_reports_dir": str(taxon_reports_path),
                        "kraken_assignments": str(taxon_reports_path / f"{climb_id}_PlusPF.kraken_assignments.tsv"),
                        "kraken_report": str(taxon_reports_path / f"{climb_id}_PlusPF.kraken_report.json"),
                    }
                    rows.append(row)
                except (KeyError, IndexError):
                    logger.warning("Sample %s not found in database. Skipping.", climb_id)

        if not rows:
            raise ValueError("Samplesheet generation produced no records")

        samplesheet_dir = self.config.work_dir / "samplesheets"
        samplesheet_dir.mkdir(parents=True, exist_ok=True)
        samplesheet_path = samplesheet_dir / f"amr_samplesheet_{job_id}.csv"

        df = pd.DataFrame(rows)
        df.to_csv(samplesheet_path, index=False)

        logger.debug(
            "Generated AMR samplesheet at %s",
            samplesheet_path,
        )

        return samplesheet_path
