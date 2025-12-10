from cherami.workers import amr, orange_box
from cherami.workers.worker import Worker

WORKERS: dict[str, type[Worker]] = {
    "orange-box": orange_box.OrangeBoxWorker,
    "amr": amr.AmrWorker,
}
