# Automation Promotion Guide

This guide explains how to use the promotion workflow to deploy automations through different stages (dev → staging → production) and how to rollback deployments.

## Overview

The promotion workflow allows you to:
1. **Upload assets** (automation code) and get a checksum
2. **Deploy to dev** for initial testing
3. **Promote to staging** for integration testing
4. **Promote to production** for live deployment
5. **Rollback** to a previous version using historical checksums

## Key Concepts

### Stages

- **dev**: Development environment for initial testing
- **staging**: Staging environment for integration testing
- **production**: Production environment

### Checksums

Each uploaded asset receives a unique SHA256 checksum that identifies the exact version of your automation code. This checksum is used to:
- Track which version is deployed in each stage
- Enable rollback to previous versions
- Maintain deployment history

## Workflow

### Step 1: Upload an Asset

Upload your automation code as a ZIP file to get a checksum:

**Endpoint:** `POST /automations/assets/upload`

**Request:**
```bash
curl -X POST "https://your-gitops-server/automations/assets/upload" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@your-automation.zip"
```

**Response:**
```json
{
  "message": "Asset uploaded successfully",
  "output_directory": "/path/to/checksum",
  "checksum": "abc123def456..."
}
```

Save the `checksum` value for the next steps.

### Step 2: Deploy to Dev

Deploy the automation to the dev stage:

**Endpoint:** `POST /automations/{deployment_id}/deploy`

**Request:**
```bash
curl -X POST "https://your-gitops-server/automations/my-automation-dev/deploy" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "checksum=abc123def456..." \
  -F "stage=dev" \
  -F "relative_path=automations/my-automation"
```

**Parameters:**
- `deployment_id`: Unique identifier for your automation
- `checksum`: The checksum from Step 1 (optional if already set)
- `stage`: Set to `"dev"` for development
- `relative_path`: Optional path relative to workspace root

**Response:**
```json
{
  "message": "Deployed services successfully",
  "deployments": [...],
  "result": {...}
}
```

### Step 3: Promote to Staging

Once testing in dev is complete, promote to staging:

**Request:**
```bash
curl -X POST "https://your-gitops-server/automations/my-automation-staging/deploy" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "checksum=abc123def456..." \
  -F "stage=staging"
```

Note: You can use the same checksum or upload a new version.

### Step 4: Promote to Production

After staging validation, promote to production:

**Request:**
```bash
curl -X POST "https://your-gitops-server/automations/my-automation/deploy" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "checksum=abc123def456..." \
  -F "stage=production"
```

## Viewing Deployment History

Check the deployment history to see all changes:

**Endpoint:** `GET /automations/{deployment_id}/history`

**Request:**
```bash
curl -X GET "https://your-gitops-server/automations/my-automation/history?page=1&page_size=20" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response:**
```json
{
  "items": [
    {
      "commit": "abc123...",
      "author": "gitops",
      "date": "2024-01-15 10:30:00",
      "message": "deploy deployment my-automation",
      "checksum": "abc123def456...",
      "stage": "dev",
      "relative_path": "automations/my-automation",
      "active": true,
      "tag_checksum": "xyz789..."
    },
    ...
  ],
  "total": 10,
  "page": 1,
  "page_size": 20,
  "total_pages": 1
}
```

## Rollback

To rollback to a previous version:

1. **Get the history** to find the checksum of the version you want to rollback to:
   ```bash
   curl -X GET "https://your-gitops-server/automations/my-automation/history" \
     -H "Authorization: Bearer YOUR_TOKEN"
   ```

2. **Deploy with the old checksum**:
   ```bash
   curl -X POST "https://your-gitops-server/automations/my-automation/deploy" \
     -H "Authorization: Bearer YOUR_TOKEN" \
     -F "checksum=old-checksum-from-history" \
     -F "stage=production"
   ```

The system will redeploy using the specified checksum, effectively rolling back to that version.

## Listing Assets

View all uploaded assets:

**Endpoint:** `GET /automations/assets`

**Request:**
```bash
curl -X GET "https://your-gitops-server/automations/assets" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response:**
```json
[
  {
    "checksum": "abc123def456...",
    "path": "/path/to/checksum",
    "exists": true
  },
  ...
]
```

## Best Practices

1. **Always test in dev first**: Deploy to dev and validate before promoting
2. **Use meaningful checksums**: The checksum identifies your code version - keep track of what each checksum represents
3. **Monitor deployment history**: Regularly check the history endpoint to track changes
4. **Keep assets**: Don't delete assets that are in use - they're needed for rollback
5. **Stage progression**: Follow the dev → staging → production flow for safer deployments

## Common Workflows

### New Feature Deployment

```bash
# 1. Upload new version
curl -X POST ".../automations/assets/upload" -F "file=@new-feature.zip"
# Save checksum: NEW_CHECKSUM

# 2. Deploy to dev
curl -X POST ".../automations/my-automation/deploy" \
  -F "checksum=NEW_CHECKSUM" -F "stage=dev"

# 3. Test in dev, then promote to staging
curl -X POST ".../automations/my-automation/deploy" \
  -F "checksum=NEW_CHECKSUM" -F "stage=staging"

# 4. Test in staging, then promote to production
curl -X POST ".../automations/my-automation/deploy" \
  -F "checksum=NEW_CHECKSUM" -F "stage=production"
```

### Hotfix Deployment

```bash
# 1. Upload hotfix
curl -X POST ".../automations/assets/upload" -F "file=@hotfix.zip"
# Save checksum: HOTFIX_CHECKSUM

# 2. Deploy directly to production (skip dev/staging for urgent fixes)
curl -X POST ".../automations/my-automation/deploy" \
  -F "checksum=HOTFIX_CHECKSUM" -F "stage=production"
```

### Rollback Production

```bash
# 1. Find previous working checksum from history
curl -X GET ".../automations/my-automation/history"

# 2. Deploy previous checksum to production
curl -X POST ".../automations/my-automation/deploy" \
  -F "checksum=PREVIOUS_CHECKSUM" -F "stage=production"
```

## API Reference Summary

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/automations/assets/upload` | POST | Upload asset and get checksum |
| `/automations/assets` | GET | List all uploaded assets |
| `/automations/{deployment_id}/deploy` | POST | Deploy automation with checksum and stage |
| `/automations/{deployment_id}/history` | GET | Get deployment history with pagination |
| `/automations/` | GET | List all automations (includes stage info) |

## Notes

- The `stage` parameter is optional - if not provided, the existing stage is maintained
- The `checksum` parameter is optional - if not provided, the existing checksum is used
- The `relative_path` parameter is optional and can be set during initial deployment
- Production stage is stored as empty string (`""`) in bitswan.yaml but must be passed as `"production"` in API calls
- History entries only show changes - duplicate entries for commits without changes are filtered out

