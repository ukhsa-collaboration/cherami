# General dev notes

### Code style

This repo uses ruff for formatting and linting, enforced via CI and a pre-commit hook both of which are included in the dev dependencies. Ensure these are installed (including the pre-commit) so that CI doesnt fail for format/linting errors.

In general it is enough to follow the ruff lint rules (defined in the `pyproject.toml`) and ensure there are no violations using `ruff check` and `ruff format`.

Every function should have its return signature and arguments type hinted (this is enforced via ruff).

Tests will be run in CI, but should be run locally first with `uv run pytest`

Docstrings should be in [google](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) style in full for any public or exported functions/classes. Private functions can have smaller single line docstrings without explictly documenting `Arguments` and `Returns`. 

