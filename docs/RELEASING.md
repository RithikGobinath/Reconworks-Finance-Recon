# Releasing ReconWorks

This is a practical checklist for creating a clean, portfolio-ready GitHub release.

## 0) Pick your release version

ReconWorks should follow Semantic Versioning:

- MAJOR: breaking changes to CLI/config/output schema
- MINOR: new features that are backward compatible
- PATCH: bug fixes only

Suggested first “public” release:
- v1.0.0 — Month-end Reconciliation MVP

## 1) Pre-release checklist

- [ ] `python -m reconworks --help` shows all commands (including `publish-pq`)
- [ ] Run end-to-end on sample data:
  - [ ] `init-sample-data`
  - [ ] `ingest → ... → build-excel`
  - [ ] optional: `publish-pq`
- [ ] Run tests: `pytest -q`
- [ ] Sanity check outputs:
  - [ ] `out/sqlite/reconworks.db` created and contains tables
  - [ ] `out/excel/recon_dashboard.xlsx` opens
  - [ ] `out/pq_drop/...` created (if using publish-pq)

## 2) Sync version numbers (IMPORTANT)

Update both files to the same version:

- `pyproject.toml` → `[project].version = "X.Y.Z"`
- `src/reconworks/__init__.py` → `__version__ = "X.Y.Z"`

## 3) Update CHANGELOG.md

- Move items from `[Unreleased]` into a new version section:
  - `## [X.Y.Z] - YYYY-MM-DD`
- Keep entries short and user-facing.

## 4) Build the package locally (optional but professional)

```bash
python -m pip install --upgrade build
python -m build
```

This should create:
- `dist/*.whl`
- `dist/*.tar.gz`

(Optional sanity test in a clean env)
```bash
pip install dist/*.whl
python -m reconworks --help
```

## 5) Create a git tag

Annotated tags are best for releases:

```bash
git tag -a vX.Y.Z -m "ReconWorks vX.Y.Z"
git push origin vX.Y.Z
```

## 6) Create the GitHub Release

On GitHub:
1. Go to **Releases**
2. **Draft a new release**
3. Choose tag `vX.Y.Z`
4. Title: `ReconWorks vX.Y.Z`
5. Paste release notes (or use auto-generated notes)

Attach artifacts (recommended for recruiters):
- `out/excel/recon_dashboard.xlsx`
- a tiny sample data zip (or screenshots)

## 7) Post-release

- [ ] Bump to next dev version in `[Unreleased]` (optional)
- [ ] Create an issue list / roadmap for next improvements

## Optional: CI on GitHub Actions

Add a CI workflow that runs tests on each push/PR:
- `.github/workflows/ci.yml`
