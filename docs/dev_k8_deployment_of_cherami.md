# Kubernetes Deployment of Cherami

This guide details the process of deploying a configured Cherami characterisation pipeline to a Kubernetes cluster. 
This involves creating a deployment spec configuring the Cherami instance to run the Pipeline/Worker as a long lived kubernetes deployment.

## 1. Overview

Assuming you have followed the steps to createa new pipeline, deploying a Cherami worker involves:
1. **Creating the deployment spec**: A YAML specification for the deployment, configuring env vars, file mounts etc.
2. **Deployment with a test image**: the test image is added to the deployment and deployed in the test environment. 

## 2. A note on API Keys

Characterisation pipelines likely will require access to external services like S3/Onyx. The dev environment should have the required 
AWS and Onyx credentials in K8 Secrets. 

It is important that any API keys or credentials are NOT committed to version control.

## 3. Deployment specification

Create a deployment file (e.g., `my-pipeline-deployment.yaml`). Be sure to replace all <pipeline>, paths, NAMESPACE and 
PACKAGE/IMAGE placeholders below. It is advised to look at other deployments already on the server.

The PACKAGE is likely to be cherami_dev initially, and the IMAGE is the name of your image created from your branch. 

### Example Manifest

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cherami-mscape-<pipeline>-runner
  labels:
    app: cherami-mscape-<pipeline>-runner
spec:
  replicas: 1
  strategy:
    type: "Recreate"
  selector:
    matchLabels:
      app: cherami-mscape-<pipeline>-runner
  template:
    metadata:
      name: cherami-mscape-<pipeline>-runner
      labels:
        app: cherami-mscape-<pipeline>-runner
    spec:
      hostname: cherami-mscape-<pipeline>-runner
      subdomain: ns-<NAMESPACE>
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
        workingDir: "/shared/team/nxf_work/<pipeline>"
        env:
        - name: K8S_SECRETS_MOUNT
          value: "/run/secrets/kubernetes.io/serviceaccount"
        envFrom:
        - secretRef:
            name: onyx
        - secretRef:
            name: aws

        image: ghcr.io/ukhsa-collaboration/PACKAGE:IMAGE
        args:
            - /bin/sh
            - -c
            - cherami --log <path/to/log_file> --audit_db <path/to/audir_db> serve <path/to/config>
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