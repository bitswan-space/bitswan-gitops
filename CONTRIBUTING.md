# Contributing to Bitswan GitOps

This document provides guidelines and instructions for contributing to the Bitswan GitOps project.

## Development Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.x
- External docker network `bitswan_network`

### Getting Started

1. Ensure the external docker network exists:
```bash
docker network create bitswan_network
```

2. Copy the environment file and configure:
```bash
cp .env.example .env
```

3. Start the development environment:

**Mac:**
```bash
docker compose --env-file .env -f docker-compose.mac.yaml up -d
```

**Linux:**
```bash
docker compose --env-file .env -f docker-compose.linux.yaml up -d
```

## Development Mode

The GitOps service supports an automatic development mode that enables hot-reload functionality for faster development cycles.

### How Dev Mode Works

Dev mode is **automatically detected** when the GitOps source code is mounted into the container at `/src/app`. The detection works as follows:

1. The container checks if `/src/app/pyproject.toml` exists
2. It verifies the file contains "bitswan-gitops" to confirm it's the correct source
3. If both conditions are met, `DEBUG=true` is automatically set

### Enabling Dev Mode

**Via the Automation Server CLI:**

The recommended way to enable dev mode is through the `bitswan` CLI:

```bash
bitswan workspace update <workspace-name> --dev-mode --gitops-dev-source-dir /path/to/bitswan-gitops
```

This mounts your local source directory to `/src/app` inside the container and enables automatic hot-reload.

**Manual Setup:**

When running locally with docker-compose, mount your source directory:

```yaml
volumes:
  - /path/to/bitswan-gitops:/src/app:z
```

The container will automatically detect the mounted source and enable debug mode.

### Dev Mode Output

When dev mode is activated, you'll see:

```
========================================
AUTO-DETECTED DEV MODE: Found gitops source mounted at /src/app
Enabling DEBUG=true for hot-reload support
========================================
```

### Disabling Dev Mode

To disable dev mode via the automation server:

```bash
bitswan workspace update <workspace-name> --disable-dev-mode
```

## Code Style

- Follow PEP 8 guidelines for Python code
- Use meaningful variable and function names
- Add docstrings to functions and classes

## Testing

Run tests before submitting changes:

```bash
pytest
```

## Submitting Changes

1. Create a feature branch from `main`
2. Make your changes
3. Ensure tests pass
4. Submit a pull request
