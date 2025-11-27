from cherami.config import WorkerConfig
from cherami.pipelines import AmrPipeline
from cherami.workers.worker import Worker


class AmrWorker(Worker):
    def __init__(self, worker_config: WorkerConfig, pipeline: AmrPipeline) -> None:
        super().__init__(worker_config=worker_config, pipeline=pipeline)
