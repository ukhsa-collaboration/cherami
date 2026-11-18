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

## Create a trace file

Cherami will use the Nextflow [trace file](https://www.nextflow.io/docs/latest/reports.html#trace-file) to check whether a run succeeded. Your pipeline must write the trace file to `pipeline_trace.txt` at the top level of `--outdir`. To do this, add the following to your `nextflow.config`:

```
trace {
    enabled = true
    file    = "${params.outdir}/pipeline_trace.txt"
}
```

After the job finishes, Cherami will read the exit code of every process in the trace file. By default the run will pass if every process exited with `0`. Step 03 covers how to allow other exit codes. If the trace file is missing, the run will fail and Cherami will not retry it.

If you customise `trace.fields` you must keep the `name` and `exit` columns.

## Release your pipeline with version tags

Cherami will pass the `version` from the config to Nextflow as `-r`, so Nextflow will pull that exact tag from the repo.

In production, `version` should be a release tag, e.g. `v1.0.0`. During development, prefer a branch named with a version (`v0.1.0`, `v1.0.0-rc` etc) rather than `dev` or `main` as this is used to set the `pipeline_version` recorded in Onyx.

## Write analysis records to Onyx

Your pipeline should store its results in Onyx as analysis records. Cherami does not track which pipelines a sample has already been through, and uses these records in Onyx as the source of truth instead. For example a sample can come through Cherami more than once, e.g. after an upstream database update or a rerun. Each time, Cherami will query Onyx for existing analysis records from your pipeline, and will skip the sample if a record already exists for the current versions.

For Cherami to match a record, the record needs:

- `pipeline_name` set to the `name` in your Cherami config.
- `pipeline_version` set to the `version` in your Cherami config. This is the same string Cherami will pass as `-r`, so use the exact tag name, e.g. `v1.0.0` and not `1.0.0`.
- `methods.onyx_versions_hash`: a hash of the upstream tool and database versions for the sample.
- An `orange_box_version` entry in `methods.versions`: the version of orange box that produced your inputs.

If a record is missing any of these fields it will never match and your pipeline will run again every time the sample comes through.

Use [onyx-analysis-helper](https://github.com/ukhsa-collaboration/onyx-analysis-helper) to write the records. Its documentation explains how to set each field.

## Check your pipeline locally

Before adding your pipeline to Cherami you should run it yourself. Write a samplesheet and run:

```bash
nextflow run <my pipeline> --samplesheet test_samplesheet.csv --outdir test_output
```

Then check:

- [ ] All outputs are under `test_output/`.
- [ ] `test_output/pipeline_trace.txt` exists, and every process has an exit code of `0`.
- [ ] Your pipeline has a version tag (or a version-named branch) that Nextflow can pull with `-r`.
- [ ] The analysis records in Onyx have the four fields listed above.

Run local tests in devscape so that test records do not end up in production.
