# Nextflow pipeline requirements

Cherami will run every pipeline with a prebuilt `nextflow run` command. This page covers the requirements for each Nextflow pipeline to be able to run in Cherami.

tldr:

- [Accept a `--samplesheet` parameter](#accept-a---samplesheet-parameter) for all inputs.
- [Accept an `--outdir` parameter](#accept-an---outdir-parameter) and write all outputs under that folder.
- [Create a trace file](#create-a-trace-file) at `pipeline_trace.txt` at the top level of `--outdir`.
- [Release your pipeline with version tags](#release-your-pipeline-with-version-tags) so that Nextflow can pull with `-r`.
- [Write analysis records to Onyx](#write-analysis-records-to-onyx) that include the pipeline name, version and upstream versions.

## Nextflow command

For each sample to be processed, Cherami will create a Kubernetes job that runs a Nextflow command like below:

```bash
nextflow run <path> -c <nf_config_path> -profile <nf_profiles> -r <version> --outdir <run output directory> --samplesheet <samplesheet>
```

Most of the values in `< >` come from the Cherami config file for your pipeline, which step 02 covers. Cherami will generate the last two, `--outdir` and `--samplesheet`, for each run. For now the important part is that your pipeline needs to accept `--samplesheet` and `--outdir`.

## Accept a `--samplesheet` parameter

Cherami will pass all inputs to your pipeline through a samplesheet. Your pipeline should read everything it needs from this file, e.g. fastq paths and any values from Onyx.

The format is up to you, and it must match the format your pipeline reads. In general this is a CSV file with a column for each input. For an example, see the AMR samplesheet in [`amr.py`](../../src/cherami/pipelines/amr.py). In step 03 you will write the `generate_samplesheet` function that creates this file.

## Accept an `--outdir` parameter

Cherami will create a new output directory for each run and pass it to your pipeline as `--outdir`. The directory has the form:

```
<global_output_dir>/<pipeline name>/<climb_id>/<timestamp>/
```

Your pipeline should write all of its outputs under `--outdir`.

`<global_output_dir>` is the shared root set in the Cherami config, the same for all pipelines. You can use any folder layout inside `--outdir`, as long as the trace file is in the right place (see below).
