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
