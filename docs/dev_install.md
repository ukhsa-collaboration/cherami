# Development installation

## Installation for development
The reccomended way of installing this repo is using uv:

```bash
git clone https://github.com/ukhsa-collaboration/cherami
cd cherami
uv run pre-commit install
uv run pytest
```

However other methods (such as conda or venv) will work:

```bash
git clone https://github.com/ukhsa-collaboration/cherami
cd cherami
conda create -n cherami python=3.12 "pip>=25.1"
conda activate cherami
pip install --group dev
pre-commit install
pytest
```

## Setup for local dev

### Setting up a local RabbitMQ server for development

A RabbitMQ pod can be created using `deploy_rabbitmq.sh` helper in `./scripts`. This creates a kubernetes pod running a RabbitMQ server and prints its IP address.

To create a new exchange you can use the CLI tool `rabbitmqadmin` from the container:

```bash
kubectl exec -it rabbitmq -- /bin/bash 
rabbitmqadmin -u admin -p password declare exchange name=cherami_test type=fanout durable=true
```

You will need to update the varys config file to point to the IP of the local pod.

An example varys config file for this configuration:
```json
{
  "version": "0.1",
  "profiles": {
    "cherami": {
      "username": "admin",
      "password": "password",
      "amqp_url": "10.0.0.1",
      "port": 5672,
      "use_tls": false
    }
  }
}
```

Then run `cherami serve path/to/config.json` to listen to messages sent on the created exchange

An example helper script to test payloads is included in `./scripts/send.py` e.g:
```bash
uv run scripts/send.py
```

### Debug commands

#### serve
Run the worker defined in a pipeline config file.

```
cherami serve path/to/config.json
```

#### describe
Shows listen exchange/queue and publish exchange/queue for the worker described by a config file.
```
cherami describe path/to/config.json
```

For pipeline/worker implementation guidance see `docs/dev_adding_new_pipeline.md`.
