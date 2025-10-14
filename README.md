[![Docker](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml/badge.svg)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml)
[![Tests](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml)

# cherami

cherami is the mSCAPE orchestration module for pathogen pipelines.

## Usage

```
Usage: cherami [OPTIONS] COMMAND [ARGS]...

Commands:
  evaluate
  run
  watch
```

### Watch
```
Usage: cherami watch [OPTIONS]

Options:
  --max-samples INTEGER  Maximum number of concurrent samples
  --profile TEXT         Execution profile to use  [required]
  --varys-log FILE       Path to varys log
  --sample-log FILE      Path to JSONL file for per-sample results
  --help                 Show this message and exit.
  ```


## Development

For development notes please refer to [dev.md](dev.md)