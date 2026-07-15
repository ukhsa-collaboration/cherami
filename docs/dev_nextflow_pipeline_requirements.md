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

If your pipeline will be kicked off based on some information that Cherami has that your pipeline the needs, this can be included in the samplesheet, and parsed out in your Nextflow pipeline. For example, if your pipeline checks whether to run based on some Orange Box outputs, the version of the Orange Box needs to be included in the samplesheet.  

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

## Writing Analyses to Onyx 
If your pipeline creates any analysis results, these should be stored in Onyx in an analysis record. You should use the onyx_analysis_helper to do this. The context for running the analysis should also be captured - this includes the pipeline version, any database or dependency versions that if they were to change would cause different results. It is possible to pass the upstream context Cherami uses for the logic to determine whether the pipeline is run into the pipeline through the samplesheet. 
