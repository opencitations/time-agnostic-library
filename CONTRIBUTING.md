# Contributing to time-agnostic-library

Thank you for your interest in contributing to time-agnostic-library! This document provides guidelines and instructions for contributing to this project.

## How to Contribute

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Make your changes
4. Submit a pull request

## Development Setup

To set up the development environment:

```bash
# Clone the repository
git clone https://github.com/opencitations/time-agnostic-library.git
cd time-agnostic-library

# Install dependencies using Poetry
poetry install
```

## Testing

Before submitting a pull request, make sure all tests pass:

```bash
poetry run coverage run --rcfile=tests/coverage/.coveragerc
```

## Release Process

The project uses [semantic-release](https://github.com/semantic-release/semantic-release) to automate the release process.

### Automatic Release Process

1. When changes are pushed to the `main` branch, the GitHub Actions workflow will run tests.
2. If the tests pass and the commit message contains the tag `[release]`, the release process will be triggered automatically.
3. The release workflow will:
   - Create a new version based on the commit messages
   - Update the CHANGELOG.md file
   - Update the version in pyproject.toml
   - Create a GitHub release with release notes
   - Publish the package to PyPI

### Commit Message Format

To trigger a release, include `[release]` in your commit message.

For semantic versioning to work correctly, your commit messages should follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

- `fix: ...` - for bug fixes (triggers a PATCH version bump)
- `feat: ...` - for new features (triggers a MINOR version bump)
- `feat!: ...` or `fix!: ...` or `refactor!: ...` - for breaking changes (triggers a MAJOR version bump)

Examples:
- `fix: correct handling of time intervals [release]`
- `feat: add new time traversal function [release]`
- `feat!: redesign API for better usability [release]`

### Manual Release

If you need to trigger a release manually:

1. Make your changes and commit them
2. Add `[release]` to your commit message
3. Push to the main branch

```bash
git add .
git commit -m "feat: add new feature [release]"
git push origin main
```