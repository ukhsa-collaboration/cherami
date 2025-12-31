import csv
import logging
from pathlib import Path

from cherami.pipelines.pipeline import Pipeline

logger = logging.getLogger(__name__)


class OrangeBoxPipeline(Pipeline):
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        rows = []
        for climb_id in samples:
            row = {
                "climb_id": climb_id,
            }
            rows.append(row)
        if not rows:
            raise ValueError("Samplesheet generation produced no records")

        fieldnames = list(rows[0].keys())
        with output_filepath.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.debug(
            "Generated orange_box samplesheet at %s",
            output_filepath,
        )
