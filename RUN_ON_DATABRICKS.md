# Run finpipe on Databricks

## Prerequisites

1. **Databricks CLI configured** (already done ✓)
2. **Snowflake password stored in Databricks Secrets**

## Setup Secrets (One-time)

```powershell
# Create secret scope for Snowflake credentials
databricks secrets create-scope snowflake

# Store your Snowflake password
databricks secrets put-secret snowflake password
# This will open a text editor - paste your password and save
```

## Deploy & Run

### Option 1: Deploy as a Job (Automated)

```powershell
# Validate configuration
databricks bundle validate

# Deploy to Databricks workspace
databricks bundle deploy -t dev

# Run the job immediately
databricks bundle run finpipe_medallion -t dev

# Check job status
databricks jobs list | Select-String finpipe
```

### Option 2: Upload and Run Manually in Notebook

```powershell
# Upload the wheel file you built
databricks fs cp dist/finpipe-0.1.0-py3-none-any.whl dbfs:/FileStore/finpipe/

# Upload the notebook
databricks workspace import finpipe/databricks/run.py /Users/adityaofficialuse@gmail.com/finpipe_runner --language PYTHON
```

Then in Databricks UI:
1. Go to **Workspace** → `/Users/adityaofficialuse@gmail.com/finpipe_runner`
2. Attach to a cluster (create one if needed)
3. Run all cells

### Option 3: Use Databricks Repos (Recommended)

```powershell
# If you have Git connected
# 1. Push this repo to GitHub/GitLab
# 2. In Databricks UI: Repos → Add Repo → paste URL
# 3. Open finpipe/databricks/run.py in the repo
# 4. Run cells
```

## Verify Deployment

```powershell
# Check uploaded files
databricks workspace list /Users/adityaofficialuse@gmail.com/.bundle/finpipe/dev

# View job runs
databricks jobs runs list --job-name finpipe-medallion-dev --output JSON | ConvertFrom-Json
```

## Cluster Configuration

The job uses:
- **Spark Version**: 14.3.x
- **Node Type**: i3.xlarge (AWS) - 4 cores, 30GB RAM
- **Workers**: 2
- **Total Capacity**: 3 nodes (1 driver + 2 workers)

Estimated cost: ~$0.50-1.00 per run (10-15 minutes)

## Schedule

Default schedule: Daily at 2 AM (PAUSED)

To enable:
```powershell
databricks jobs update <job-id> --schedule-pause-status UNPAUSED
```

## Troubleshooting

### "Secret not found"
```powershell
databricks secrets put-secret snowflake password
```

### "Cluster start failed"
- Check node_type_id in databricks.yml (use Azure VM types if on Azure)
- Verify workspace has capacity for requested cluster size

### "pip install failed"
```powershell
# Rebuild wheel
python -m build --wheel

# Redeploy
databricks bundle deploy -t dev --force
```
