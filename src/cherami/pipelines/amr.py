import csv
import logging
from pathlib import Path

from onyx import OnyxClient

from cherami.pipelines.pipeline import Pipeline
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


class AmrPipeline(Pipeline):
    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        config = init_onyx()
        rows = []
        with OnyxClient(config) as client:
            for climb_id in samples:
                try:
                    climb_records = list(client.filter(project="synthscape", climb_id=climb_id))
                    record = climb_records[0]
                    read1_fastq = record["human_filtered_reads_1"]
                    read_2_fastq = record["human_filtered_reads_2"]
                    taxon_reports = record["taxon_reports"]
                    row = {
                        "climb_id": climb_id,
                        "human_filtered_reads_1": read1_fastq,
                        "human_filtered_reads_2": read_2_fastq,
                        "taxon_reports": taxon_reports,
                        # ## taxon_reports_dir comes with trailing slash
                        # "kraken_assignments": f"{taxon_reports}{climb_id}_PlusPF.kraken_assignments.tsv",
                        # "kraken_report": f"{taxon_reports}{climb_id}_PlusPF.kraken_report.json",
                    }
                    rows.append(row)
                except (KeyError, IndexError):
                    logger.warning("Sample %s not found in database. Skipping.", climb_id)

        if not rows:
            raise ValueError("Samplesheet generation produced no records")

        samplesheet_dir = self.config.work_dir / "samplesheets"
        samplesheet_dir.mkdir(parents=True, exist_ok=True)
        samplesheet_path = samplesheet_dir / f"amr_samplesheet_{job_id}.csv"

        fieldnames = list(rows[0].keys())

        with samplesheet_path.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.debug(
            "Generated AMR samplesheet at %s",
            samplesheet_path,
        )

        return samplesheet_path
