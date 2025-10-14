from pathlib import Path

from cherami.pipelines.implementations.amr import AmrPipeline
from cherami.workers.base import Worker

VARYS_CONFIG_PATH = Path("./conf/varys.cfg")
VARYS_LOG_PATH = Path("./amr_varys.log")


class AmrWorker(Worker):
    def __init__(self) -> None:
        super().__init__(
            worker_name="amr",
            listen_exchange="cherami_test",
            listen_queue_suffix="amr_pipeline",
            varys_config_path=VARYS_CONFIG_PATH,
            varys_log_path=VARYS_LOG_PATH,
        )

    @property
    def pipeline(self) -> AmrPipeline:
        return AmrPipeline()
