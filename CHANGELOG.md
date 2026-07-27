# Changelog
## v26.07.0 "Racing Homer"

### Added
- _breaking_: required to add 'server' to cherami global config files and parse this into GlobalConfig.

- _breaking_: PipelineContext - a new class to store the pipeline context.

- _breaking_: Pipeline method build_context that returns a PipelineContext object. This should be overwritten by the subclassed pipelines to populate the context.

- _breaking_: PathCharPipeline - a template that subclasses Pipeline for pathogen characterisation pipelines (pathchars). This provides some default methods such as build_context to add the current_onyx_hash and orange box version from the payload, as well as querying Onyx for the current versions hash to make sure cherami and the pipeline states match. Also adds some basic should_run functionality to check whether there are any existing analysis tables for the upstream context AND the current pipeline version.

- unittests for orange box should_run logic.

- conftest.py to tests with placeholders for env vars.

- Nextflow command is build with '-r' and takes the version from the pipeline config. All pipelines must now utilise releases in production.

- Samplesheet can be used to convey inforamtion between workers, for example the orange box version required for PathChar analysis records.

- Dev docs on versioning cherami.

- Cherami CLI now has version argument (--version or -V). Version and codename are stored in _version.py.

### Changed
- _breaking_: allow the user to explicitly set the number of attempts for a pipeline - this is validated at config parse time that this value is >=1. This is a breaking to change to all configs.

- Added the onyx analysis helper library version to the pyproject and pinned version.

- _breaking_: Orange box pipeline should_run logic has been enhanced. It was previously set to always True, and now will query the analysis tables AND query the current Onyx database upstream context and check:
    - whether the combination of the current orange box version AND the current onyx versions hash is present in a set of all analysis tables.
  Cherami expects Orange Box to uphold certain assumptions:
    - if the Orange Box runs, it will publish either all the analysis tables or none.
    - A set of analysis tables that are results of the same run will have the same upstream context hash.
    - Any downstream analyses tables can only be results of an upstream orange box analysis table, so taking a 'set' will group these together.

  - The Pipeline object stores GlobalConfig as an attribute global_config. Build_worker builds pipeline from CheramiConfig object (instead of GlobalConfig and PipelineConfig separately) and accesses the global and pipeline config attributes needed to give to build_worker.

- _breaking_: The version field in the pipeline config now must be used to specify a Git branch, tag, or commit. Docs updated to reflect this.

- Strep pneumo pipeline should_run now takes ClasPar outputs for pipeline kick off logic.



---
---


## [0.1.0]

### Added

- Initial release
