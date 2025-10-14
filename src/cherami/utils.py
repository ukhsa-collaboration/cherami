import logging
import multiprocessing
import os
import signal
import sys
from logging.handlers import QueueHandler, TimedRotatingFileHandler
from multiprocessing import Queue
from multiprocessing.context import SpawnProcess
from pathlib import Path

from kubernetes.client import Configuration
from kubernetes.client.api import BatchV1Api
from onyx import OnyxConfig, OnyxEnv
from varys import Varys


def logging_process(log_queue: Queue, log_path: Path | None, log_level: str) -> None:
    """Entry point for the logging process.

    This process handles all log messages from worker processes via a multiprocessing.Queue.
    Writes them to either stderr (default) or a file (if log_path is specified).

    Args:
        log_queue: Queue from which to consume log records.
        log_path: Optional path to log file. If None, logs to stderr.
        log_level: Logging level as string (e.g., "INFO", "DEBUG").
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = TimedRotatingFileHandler(
            filename=log_path,
            when="midnight",
            utc=True,
        )
    else:
        handler = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    handler.setLevel(log_level)

    while True:
        record = log_queue.get()
        if record is None:
            break
        handler.handle(record)


def setup_queue_logging(log_queue: Queue, log_level: str) -> None:
    logger = logging.getLogger("cherami")
    logger.setLevel(log_level)
    logger.handlers.clear()
    logger.propagate = False

    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)


def init_logging(log_path: Path | None, log_level: str) -> tuple[Queue, SpawnProcess]:
    """Initalise logging

    Creates a multiprocessing queue and a logging process, then configures
    the current process to use queue-based logging. This should only be called once
    at the start of the application.

    Args:
        log_path: Optional path to log file. If None, logs to stderr.
        log_level: Logging level as a string (e.g., "INFO", "DEBUG").

    Returns:
        A tuple of (log_queue, log_process).
    """
    ctx = multiprocessing.get_context("spawn")
    log_queue = ctx.Queue()
    process = ctx.Process(
        target=logging_process,
        args=(log_queue, log_path, log_level),
    )
    process.start()
    setup_queue_logging(log_queue, log_level)
    return log_queue, process


def init_varys(config_path: Path, log_path: Path) -> Varys:
    """Initialise a Varys client for RabbitMQ.

    Creates a Varys instance configured to use the "cherami" profile from the config file.
    Auto-acknowledgment is disabled so workers can manually control when messages are acked
    or nacked based on pipeline results.

    Args:
        config_path: Path to Varys JSON config containing RabbitMQ credentials and connection details.
        log_path: Path where Varys should write its debug logs.

    Returns:
        Configured Varys client ready to send and receive messages.
    """
    return Varys(
        profile="cherami",
        logfile=str(log_path),
        log_level="DEBUG",
        config_path=str(config_path),
        auto_acknowledge=False,
    )


def init_kubernetes() -> BatchV1Api:
    """Initialise a Kubernetes client.

    Returns:
        Configured k8 client for creating and managing Kubernetes Jobs.
    """
    try:
        c = Configuration()
        with Path("/run/secrets/kubernetes.io/serviceaccount/token").open("rt") as token_fh:
            token = token_fh.read()
        c.api_key["authorization"] = token
        c.api_key_prefix["authorization"] = "Bearer"
        c.host = f"https://{os.getenv('KUBERNETES_SERVICE_HOST')}"
        c.ssl_ca_cert = "/run/secrets/kubernetes.io/serviceaccount/ca.crt"  # type: ignore

        Configuration.set_default(c)
        api_instance = BatchV1Api()
        return api_instance
    except Exception as e:
        raise RuntimeError(f"Failed to initialise Kubernetes client: {e}") from e


def init_onyx() -> OnyxConfig:
    """Initialise Onyx configuration from environment variables.

    Reads ONYX_DOMAIN and ONYX_TOKEN from the environment to create an Onyx config object.
    Pipelines use this to query sample metadata and file paths when generating samplesheets.

    Returns:
        Configured OnyxConfig ready to create an OnyxClient.
    """
    try:
        return OnyxConfig(
            domain=os.environ[OnyxEnv.DOMAIN],
            token=os.environ[OnyxEnv.TOKEN],
        )
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to initialise Onyx client: {e}") from e
