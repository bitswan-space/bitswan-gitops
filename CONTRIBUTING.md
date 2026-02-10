# Development Mode

When developing the GitOps service, you can enable dev mode to get hot-reload support.

## Enabling Dev Mode

Use the automation server CLI:

```bash
bitswan workspace update <workspace-name> --dev-mode --gitops-dev-source-dir /path/to/bitswan-gitops
```

This mounts your local source directory to `/src/app` inside the container.

## How It Works

The container automatically detects when source code is mounted at `/src/app`:

1. Checks if `/src/app/pyproject.toml` exists
2. Verifies it contains "bitswan-gitops"
3. Sets `DEBUG=true` for hot-reload

You'll see this output when dev mode activates:

```
========================================
AUTO-DETECTED DEV MODE: Found gitops source mounted at /src/app
Enabling DEBUG=true for hot-reload support
========================================
```

## Disabling Dev Mode

```bash
bitswan workspace update <workspace-name> --disable-dev-mode
```

## Running Tests

The project uses [pytest](https://docs.pytest.org/) with end-to-end tests that spin up a real uvicorn server against a temporary git repository.

### Setup

```bash
uv venv .venv
uv pip install -e ".[dev]"
```

### Run all tests

```bash
.venv/bin/python -m pytest tests -v
```

### What the tests do

The E2E suite (`tests/test_automation_history_e2e.py`):

1. Creates a temp git repo with known `bitswan.yaml` commits
2. Starts a real uvicorn server (including lifespan and background cache warm-up)
3. Makes HTTP requests against the running server
4. Verifies correctness, pagination, cache consistency, and concurrent request safety

### CI

Tests run automatically on every push and pull request via GitHub Actions (`.github/workflows/lint.yml`).
