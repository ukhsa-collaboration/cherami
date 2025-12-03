## Nextflow pipeline requirements for Cherami

This document describes what a Nextflow pipeline must implement in order to run via Cherami.

Cherami builds a Kubernetes Job manifest for each pipeline run, and constructs a Nextflow command that is executed inside that jobs container. Each Nextflow pipeline expected to run via Cherami therefore needs to implement certain things in a uniform way.

## The nextflow command

Cherami builds and runs a nextflow command using values from the config file like so:

```bash
nextflow run <path> -c <nf_config_path> -profile <nf_profiles> [nf_extra_args] --outdir <output_dir> --samplesheet <SAMPLESHEET>
```

Ensure the following is implemented in the Nextflow pipeline:

### Accept a `--samplesheet` parameter

Cherami always expects a pipeline to accept inputs via a samplesheet flag (`--samplesheet`). The samplesheet should contain all inputs the pipeline needs to run, with a documented example.

### Accept a `--outdir` parameter

Cherami always expects an `--outdir` flag to store output directories.

All outputs for the run must be output to this directory. Cherami does not care about the internal folder layout inside `--outdir` except for the trace file (see below). You are free to choose your own subdirectory structure under `--outdir` as long as the trace file requirement is met.

### Write a Nextflow trace file to `<output_dir>/pipeline_trace.txt`

Cherami validates pipeline success using the Nextflow trace file. To enable this in your nextflow pipeline see [docs](https://www.nextflow.io/docs/latest/reports.html)


#### Exit codes and success criteria

From Cheramis perspective a run is considered successful when:
- The Kubernetes job completes without exhausting retries or hitting a timeout.
- The trace file exists at `<output_dir>/pipeline_trace.txt`.
- All relevant processes in the trace file have an allowed exit code:
  - By default every process must have `0`.
  - Pipelines can optionally override this to only check a subset of processes, or allow for custom exit codes.

If implementing custom exit codes for processess - ensure these a well documented, otherwise all that is needed is to enable the generation of this file in the correct location.