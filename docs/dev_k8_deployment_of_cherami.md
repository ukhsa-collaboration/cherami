# Kubernetes Deployment of Cherami

This guide details the process of deploying a configured Cherami characterisation pipeline to a Kubernetes cluster. This involves creating a deployment spec configuring the Cherami instance to run the Pipeline/Worker as a long lived kubernetes deployment.

## 1. Overview

Assuming you have followed the steps to createa new pipeline, deploying a Cherami worker involves:
1. **Creating the deployment spec**: A YAML specification for the deployment, configuring env vars, file mounts etc.
2. **Configuring API keys**: API keys will need to be added to the deployment

## 2. A note on API Keys

Characterisation pipelines likely will require access to external services like S3/Onyx. You must ensure the following environment variables are populated with valid keys.

### Required Variables
* `AWS_ACCESS_KEY_ID`: S3 Access Key.
* `AWS_SECRET_ACCESS_KEY`: S3 Secret Key.
* `ONYX_DOMAIN`: Onyx domain
* `ONYX_TOKEN`: Token the Onyx API.

In the deployment spec below, these are left empty. Currently we dont use kubernetes secrets and so should be added manually.

It is important these API keys are NOT committed to version control.

## 3. Deployment specification

Create a deployment file (e.g., `my-pipeline-deployment.yaml`). The following example demonstrates a deployment for the AMR pipeline.

### Example Manifest

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cherami-mscape-amr-runner
  labels:
    app: cherami-mscape-amr-runner
spec:
  replicas: 1
  strategy:
    type: "Recreate"
  selector:
    matchLabels:
      app: cherami-mscape-amr-runner
  template:
    metadata:
      name: cherami-mscape-amr-runner
      labels:
        app: cherami-mscape-amr-runner
    spec:
      hostname: cherami-mscape-amr-runner
      subdomain: ns-pipelinesukhsasy
      securityContext:
        runAsUser: 1000
        runAsGroup: 100
        fsGroup: 100
        runAsNonRoot: true
      volumes:
      - name: shared-public
        persistentVolumeClaim:
          claimName: cephfs-shared-ro-public
      - name: shared-team
        persistentVolumeClaim:
          claimName: cephfs-shared-team
      nodeSelector:
          hub.jupyter.org/node-purpose: user-compute
      containers:
      - name: cherami
        imagePullPolicy: Always
        resources:
          requests:
            memory: "4G"
            cpu: "1"
          limits:
            memory: "4G"
            cpu: "1"
        volumeMounts:
            - mountPath: "/shared/public"
              name: shared-public
              readOnly: true
            - mountPath: "/shared/team"
              name: shared-team
        workingDir: "/shared/team/nxf_work/cherami_amr"
        env:
        - name: AWS_ACCESS_KEY_ID
          value: ""  # FILL THIS IN
        - name: AWS_SECRET_ACCESS_KEY
          value: ""  # FILL THIS IN
        - name: AWS_ENDPOINT_URL
          value: "https://s3.climb.ac.uk"
        - name: AWS_REQUEST_CHECKSUM_CALCULATION
          value: "WHEN_REQUIRED"
        - name: ONYX_DOMAIN
          value: "https://onyx.climb.ac.uk"
        - name: ONYX_TOKEN
          value: ""  # FILL THIS IN
        - name: K8S_SECRETS_MOUNT
          value: "/run/secrets/kubernetes.io/serviceaccount"

        image: ghcr.io/ukhsa-collaboration/cherami:pre-release
        args:
            - /bin/sh
            - -c
            - cherami --log /shared/team/logs/cherami/amr_worker.log --audit_db /shared/team/cherami_outputs/audit_log/log.db serve /shared/team/configs/cherami/synthscape_amr.json
```

## 4. Deployment

Apply the manifest to the cluster:

```bash
kubectl apply -f my-pipeline-deployment.yaml
```
Check the status of the deployment:

```bash
kubectl get deployments
```