from pathlib import Path

from cherami.pipelines.implementations.orange_box import OrangeBoxPipeline
from cherami.workers.base import Worker

VARYS_CONFIG_PATH = Path("./conf/varys.cfg")
VARYS_LOG_PATH = Path("./orange_box_varys.log")


class OrangeBoxWorker(Worker):
    def __init__(self) -> None:
        super().__init__(
            worker_name="orange_box",
            listen_exchange="cherami_test",
            listen_queue_suffix="orange_box_queue",
            varys_config_path=VARYS_CONFIG_PATH,
            varys_log_path=VARYS_LOG_PATH,
        )

    @property
    def pipeline(self) -> OrangeBoxPipeline:
        return OrangeBoxPipeline()
