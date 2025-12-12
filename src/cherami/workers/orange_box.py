from pathlib import Path

from cherami.config import WorkerConfig
from cherami.pipelines import OrangeBoxPipeline
from cherami.workers.worker import Worker


class OrangeBoxWorker(Worker):
    def __init__(
        self,
        worker_config: WorkerConfig,
        pipeline: OrangeBoxPipeline,
        work_dir: Path,
        output_dir: Path,
    ) -> None:
        super().__init__(worker_config=worker_config, pipeline=pipeline, work_dir=work_dir, output_dir=output_dir)
