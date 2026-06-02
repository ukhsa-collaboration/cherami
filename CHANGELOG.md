# Changelog
## unreleased v1.0.0

### Changed
- _breaking_: allow the user to explicitly set the number of attempts for a pipeline - this is validated at config parse time that this value is >=1. This is a breaking to change to all configs.
- Added the onyx analysis helper library version and pinned version.

- Orange box should run logic has been enhanced. It was previously set to always True, and now will
query the analysis tables AND query the current Onyx database upstream context and check:
    - whether the current orange box version is present in a set of all orange box versions from all the analysis tables.
    - whether the hash of the current onyx upstream context (versions) is presnet in a set of all upstream onyx version hashes from all the analysis tables.
  Cherami expected Orange Box to uphold certain assumptions:
    - if the Orange Box runs, it will publish either all the analysis tables or none.
    - A set of analysis tables that are results of the same run will have the same upstream context hash.

### Added
- _breaking_: required to add 'server' to cherami global config files and parse this into GlobalConfig.
- unittests for orange box should_run logic.
- conftest.py to tests with placeholders for env vars.

## [0.1.0]

### Added

- Initial release
