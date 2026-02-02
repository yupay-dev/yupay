# Versioning Policy

This project follows [Semantic Versioning 2.0.0](https://semver.org/).

## Version Format: `MAJOR.MINOR.PATCH`

- **MAJOR** version when you make incompatible API changes.
- **MINOR** version when you add functionality in a backward compatible manner.
- **PATCH** version when you make backward compatible bug fixes.

## Release Process

1. Create a release branch (e.g., `release/v0.2.0`).
2. Update `CHANGELOG.md` moving "Unreleased" items to the new version.
3. Update `pyproject.toml` version.
4. Run full test suite: `pytest tests/`.
5. Merge to `main`.
6. Tag the commit (e.g., `v0.2.0`).
