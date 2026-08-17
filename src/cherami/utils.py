import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from kubernetes.client import Configuration
from kubernetes.client.api import BatchV1Api
from onyx import OnyxConfig, OnyxEnv
from varys import Varys


def init_logging(log_path: Path | None, log_level: str) -> None:
    logger = logging.getLogger("cherami")
    logger.setLevel(log_level)
    logger.handlers.clear()
    logger.propagate = False

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
    logger.addHandler(handler)

    oa_logger = logging.getLogger("onyx_analysis_helper")
    # Hardcode error-level only messages for external lib.
    oa_logger.setLevel("ERROR")
    oa_logger.addHandler(handler)


def init_varys(config_path: Path, log_path: Path, profile: str) -> Varys:
    """Initialise a Varys client for RabbitMQ.

    Returns a Varys client configured to use the requested profile from the config file.
    Auto-acknowledgment is disabled so callers must explicitly ack or nack messages based
    on pipeline results.

    Args:
        config_path: Path to Varys config containing RabbitMQ credentials and connection details.
        log_path: Path where Varys should write its logs.
        profile: Varys profile name to use from the config file.

    Returns:
        Configured Varys client ready to send and receive RMQ messages.

    Raises:
        RuntimeError: If the Varys client cannot be initialised.
    """
    try:
        return Varys(
            profile=profile,
            logfile=str(log_path),
            log_level="DEBUG",
            config_path=str(config_path),
            auto_acknowledge=False,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to initialise Varys client: {e}") from e


def init_kubernetes() -> BatchV1Api:
    """Initialise a Kubernetes client.

    Returns an authorised k8 client

    Returns:
        Configured k8 client for creating and managing Kubernetes Jobs.

    Raises:
        RuntimeError: If the Kubernetes client cannot be initialised.
    """
    try:
        c = Configuration()
        with Path("/run/secrets/kubernetes.io/serviceaccount/token").open(
            "rt"
        ) as f:
            token = f.read()
        c.api_key["authorization"] = token
        c.api_key_prefix["authorization"] = "Bearer"
        c.host = f"https://{os.getenv('KUBERNETES_SERVICE_HOST')}"
        c.ssl_ca_cert = "/run/secrets/kubernetes.io/serviceaccount/ca.crt"  # type: ignore

        Configuration.set_default(c)
        api_instance = BatchV1Api()
        return api_instance
    except Exception as e:
        raise RuntimeError(
            f"Failed to initialise Kubernetes client: {e}"
        ) from e


def init_onyx() -> OnyxConfig:
    """Initialise Onyx configuration from environment variables.

    Returns an OnyxConfig built from required environment variables ONYX_DOMAIN and
    ONYX_TOKEN.

    Returns:
        Configured OnyxConfig ready to create an OnyxClient.

    Raises:
        ValueError: If required environment variables are missing.
        RuntimeError: If the Onyx configuration cannot be initialised.
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
