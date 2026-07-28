# General dev notes

### Code style

This repo uses ruff for formatting and linting, enforced via CI and a pre-commit hook both of which are included in the dev dependencies. Ensure these are installed (including the pre-commit) so that CI doesnt fail for format/linting errors.

In general it is enough to follow the ruff lint rules (defined in the `pyproject.toml`) and ensure there are no violations using `ruff check` and `ruff format`.

Every function should have its return signature and arguments type hinted (this is enforced via ruff).

Tests will be run in CI, but should be run locally first with `uv run pytest`

Docstrings should be in [google](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) style in full for any public or exported functions/classes. Private functions can have smaller single line docstrings without explictly documenting `Arguments` and `Returns`.

### Versioning
It was decided that SemVer didn't suit Cherami, so instead Cherami will use CalVer with patch, and a two-word codename, e.g.:

`YY.M.PATCH "code name"`

The `pyproject.toml` holds the version of the Cherami codebase, and the human readable codename is stored in `src/__init__.py`, giving access to the version and codename for the cherami CLI.
The release tag is the version `vYY.M.PATCH`. The release title is `YY.M.PATCH "code name"`. Release notes should define the version and codename, like in the codebase.
The container is then referenced by the release tag.

The codename is predominantly human-readable, with the CalVer offering inherent chronology. It should be incremented with every major update (not patch). Patching will fix any already implemented functionality or updates documentation, and does not introduce any new features.

The codename is the name of a pigeon breed, and should be chosen from Wikipedia (https://en.wikipedia.org/wiki/List_of_pigeon_breeds). For example:
* Red Carneau
* Blue Bar
* Aachen Cropper
* Ancient Tumbler

The first release is the breed of pigeon of Cherami, the WWI carrier pigeon.
