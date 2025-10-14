from cherami.workers import amr, orange_box
from cherami.workers.base import Worker

WORKERS: dict[str, type[Worker]] = {
    "orange_box": orange_box.OrangeBoxWorker,
    "amr": amr.AmrWorker,
}

__all__ = [
    "WORKERS",
]
