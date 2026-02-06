---
title: Contributing
description: Development setup, testing, and release process for time-agnostic-library
---

## Development setup

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
2. Clone the repository:

```bash
git clone https://github.com/opencitations/time-agnostic-library
cd time-agnostic-library
```

3. Install all dependencies:

```bash
uv sync --dev
```

4. Build the package (output: `dist/`):

```bash
uv build
```

## Running the tests

Tests require Docker to be running.

1. Start the test database:

```bash
# On Linux/macOS
chmod +x ./tests/start-test-database.sh
./tests/start-test-database.sh

# On Windows (PowerShell)
./tests/start-test-database.ps1
```

2. Run the tests with coverage:

```bash
uv run coverage run --rcfile=tests/coverage/.coveragerc
uv run coverage report
```

3. Generate an HTML coverage report:

```bash
uv run coverage html -d htmlcov
```

4. Stop the test database:

```bash
# On Linux/macOS
chmod +x ./tests/stop-test-database.sh
./tests/stop-test-database.sh

# On Windows (PowerShell)
./tests/stop-test-database.ps1
```

## Building the documentation

```bash
cd docs
npm install
npm run dev      # Local preview
npm run build    # Production build
```

## Release process

The project uses [semantic-release](https://github.com/semantic-release/semantic-release) for automated releases.

### Commit message format

Commit messages follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `fix: ...` triggers a PATCH version bump
- `feat: ...` triggers a MINOR version bump
- `feat!: ...` / `fix!: ...` / `refactor!: ...` triggers a MAJOR version bump

### Triggering a release

Include `[release]` in the commit message:

```bash
git commit -m "feat: add new feature [release]"
git push origin main
```

The release workflow will:

1. Run tests via GitHub Actions
2. Update `CHANGELOG.md` and `pyproject.toml`
3. Create a GitHub release with release notes
4. Publish the package to PyPI

## How to contribute

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Make your changes
4. Submit a pull request
