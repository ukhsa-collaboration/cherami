import logging
import os
from pathlib import Path
from typing import Any

from cherami.pipelines.pipeline import Pipeline

logger = logging.getLogger(__name__)


class OrangeBoxPipeline(Pipeline):
    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        # TODO
        return

    def create_job_manifest(self, samplesheet_path: Path | None, job_id: str, climb_id: str) -> dict[str, Any]:
        job_name = f"{self.config.name}-{job_id}"

        job_output_dir = self.config.output_dir / climb_id
        nxf_work_dir = self.config.work_dir / climb_id
        nxf_home_dir = self.config.work_dir / ".nextflow"

        pod_env_vars = [
            {"name": "NXF_WORK", "value": str(nxf_work_dir)},
            {"name": "NXF_HOME", "value": str(nxf_home_dir)},
            {"name": "ONYX_TOKEN", "value": str(os.environ.get("ONYX_TOKEN"))},
            {"name": "ONYX_DOMAIN", "value": str(os.environ.get("ONYX_DOMAIN"))},
            {
                "name": "AWS_SECRET_ACCESS_KEY",
                "value": str(os.environ.get("AWS_SECRET_ACCESS_KEY")),
            },
            {
                "name": "AWS_ACCESS_KEY_ID",
                "value": str(os.environ.get("AWS_ACCESS_KEY_ID")),
            },
            {
                "name": "AWS_ENDPOINT_URL",
                "value": str(os.environ.get("AWS_ENDPOINT_URL")),
            },
            {
                "name": "AWS_REQUEST_CHECKSUM_CALCULATION",
                "value": str(os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION")),
            },
        ]

        nextflow_cmd = ["nextflow"]
        nextflow_cmd.extend(["run", str(self.config.path)])

        if self.config.nf_config_path:
            nextflow_cmd.extend(["-c", str(self.config.nf_config_path)])
        if self.config.nf_profiles:
            nextflow_cmd.extend(["-profile", ",".join(self.config.nf_profiles)])
        if self.config.nf_extra_args:
            nextflow_cmd.extend(self.config.nf_extra_args)
        if self.config.output_dir:
            nextflow_cmd.extend(["--outdir", str(job_output_dir)])
        if samplesheet_path:
            nextflow_cmd.extend(["--samplesheet", str(samplesheet_path)])
        if climb_id:
            nextflow_cmd.extend(["--climbid", str(climb_id)])

        command = " ".join(nextflow_cmd)
        logger.debug("Nextflow command: %s", command)

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.config.namespace,
            },
            "spec": {
                "ttlSecondsAfterFinished": 120,
                "backoffLimit": self.config.backoff_limit,
                "template": {
                    "spec": {
                        "hostname": job_name,
                        "subdomain": self.config.namespace,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "runAsGroup": 1000,
                            "fsGroup": 1000,
                        },
                        "restartPolicy": "Never",
                        "volumes": [
                            {
                                "name": "shared-public",
                                "persistentVolumeClaim": {"claimName": "cephfs-shared-ro-public"},
                            },
                            {
                                "name": "shared-team",
                                "persistentVolumeClaim": {"claimName": "cephfs-shared-team"},
                            },
                        ],
                        "nodeSelector": {"hub.jupyter.org/node-purpose": "user-compute"},
                        "containers": [
                            {
                                "name": job_name,
                                "image": self.config.container,
                                "resources": {
                                    "requests": {
                                        "cpu": str(self.config.cpus),
                                        "memory": self.config.mem,
                                    },
                                    "limits": {
                                        "cpu": str(self.config.cpu_limit),
                                        "memory": self.config.mem_limit,
                                    },
                                },
                                "volumeMounts": [
                                    {
                                        "mountPath": "/shared/public/",
                                        "name": "shared-public",
                                        "readOnly": True,
                                    },
                                    {
                                        "mountPath": "/shared/team/",
                                        "name": "shared-team",
                                    },
                                ],
                                "workingDir": str(self.config.work_dir),
                                "env": pod_env_vars,
                                "args": ["/bin/sh", "-c", command],
                            },
                        ],
                    },
                },
            },
        }
