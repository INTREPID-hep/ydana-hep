# Contributing to YDANA-HEP

Thanks for contributing.

## How to contribute

1. Open an issue for bugs, feature requests, or questions.
2. Create a branch for your work.
3. Implement and test your changes locally.
4. Submit a pull request to `main`.

## Issue guidelines

- Search existing issues before opening a new one.
- Provide a minimal reproducible example when reporting bugs.
- Include environment details (Python version, install method, relevant dependencies).
- Use clear titles and expected vs actual behavior.
- Apply the most accurate label from the approved label catalog. https://github.com:uniovi-hepex/ydana-hep/ydana-hep/labels/bug, https://github.com:uniovi-hepex/ydana-hep/ydana-hep/labels/documentation, https://github.com:uniovi-hepex/ydana-hep/ydana-hep/labels/enhancement, https://github.com:uniovi-hepex/ydana-hep/ydana-hep/labels/question, etc.


## Pull request guidelines

- Keep PRs focused and small when possible.
- Add or update tests for behavior changes.
- Update docs for user-facing changes.
- Prefer clean, declarative commit messages with clear intent.
- Avoid committing generated artifacts, large binaries, or local environment files.

## Local workflow

Set up your environment first (create venv, run `uv sync --extra dev`).

```shell
# Create a branch and made your changes
git checkout -b feature/my-change
# test your changes
uv run pytest
# check code style and formatting
uv run ruff check ydana
uv run ruff format --check ydana
# commit and push your changes
git add <files>
git commit -m "Describe change"
git push origin feature/my-change
```

Before opening a PR with documentation changes, build docs locally to check for warnings:

```shell
uv sync --extra docs
cd ../docs
uv run make livehtml
```

## Helpful links

- Repository: [https://github.com:uniovi-hepex/ydana-hep/ydana-hep](https://github.com:uniovi-hepex/ydana-hep/ydana-hep)
- Issues: [https://github.com:uniovi-hepex/ydana-hep/ydana-hep/issues](https://github.com:uniovi-hepex/ydana-hep/ydana-hep/issues)
- New issue: [https://github.com:uniovi-hepex/ydana-hep/ydana-hep/issues/new](https://github.com:uniovi-hepex/ydana-hep/ydana-hep/issues/new)
