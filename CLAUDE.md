# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ⛔ CRITICAL: NEVER Release to PyPI

**Claude Code must NEVER run `./build-and-distribute.sh` or release to PyPI.**

Only the human maintainer can trigger a PyPI release. When demos are ready:
1. Report to the human that bundling is complete
2. Wait for the human to run the release script manually

## ⛔ CRITICAL: Never Commit Secrets

**NEVER include PAT tokens, GitHub tokens, or any credentials in:**
- Commits
- PR descriptions
- Tool outputs shown to user
- Log messages

Tokens are stored in `local_conf.json` (gitignored) or environment variables.

## CRITICAL: Do Not Modify Bundle and Minisite Directories

**NEVER search, read, edit, or modify files under these directories unless explicitly asked:**
- `dbdemos/bundles/` - Contains packaged demo bundles (generated artifacts)
- `dbdemos/minisite/` - Contains generated minisite content

These directories contain packaged/generated demo content that should only be modified through the bundling workflow (`job_bundler.py` → `packager.py`). Direct edits to these files will be overwritten during the next bundling process and can break demo installations.

**Work on the source code in the core modules instead** (`installer.py`, `packager.py`, `job_bundler.py`, etc.) or on the source repository (`dbdemos-notebooks`).

## Project Overview

`dbdemos` is a Python toolkit for installing and packaging Databricks demos. It automates deployment of complete demo environments including notebooks, Spark Declarative Pipeline (SDP) pipelines, DBSQL dashboards, workflows, ML models, and AI/BI Genie spaces. The project serves two main purposes:

1. **End-user library**: Users install demos via `pip install dbdemos` and call `dbdemos.install('demo-name')`
2. **Demo packaging system**: Maintainers package demos from source repositories (usually `dbdemos-notebooks`) into distributable bundles

## Architecture

### Core Components

- **installer.py**: Main installation engine that deploys demos to Databricks workspaces
  - Creates clusters, SDP pipelines, workflows, dashboards, and ML models
  - Handles resource templating (replacing {{CURRENT_USER}}, {{DEMO_FOLDER}}, etc.)
  - Manages demo lifecycle from download to deployment

- **job_bundler.py**: Manages the demo bundling workflow
  - Scans repositories for demos with `_resources/bundle_config` files
  - Executes pre-run jobs to generate notebook outputs
  - Tracks execution state and commit history to avoid redundant runs

- **packager.py**: Packages demos into distributable bundles
  - Downloads notebooks (with or without pre-run results)
  - Extracts Lakeview dashboards from workspace
  - Processes notebook content (removes build tags, updates paths)
  - Generates minisite HTML for [dbdemos.ai](https://www.dbdemos.ai)

- **dbdemos.py**: User-facing API layer providing `help()`, `list_demos()`, `install()` functions

- **conf.py**: Configuration management including `DBClient` for Databricks REST API calls

- **installer_*.py modules**: Specialized installers for different resource types:
  - `installer_workflows.py`: Job/workflow deployment
  - `installer_dashboard.py`: DBSQL dashboard installation
  - `installer_genie.py`: AI/BI Genie space setup
  - `installer_repos.py`: Repository management

- **notebook_parser.py**: Parses and transforms notebook JSON/HTML content

### Demo Bundle Structure

Each demo lives in `dbdemos/bundles/{demo-name}/` with:
- `_resources/bundle_config`: JSON configuration defining demo metadata, notebooks, pipelines, workflows, dashboards
- Notebook files (`.html` format, pre-run with cell outputs)
- `_resources/dashboards/*.lvdash.json`: Dashboard definitions

Bundle configs use template keys that get replaced during installation:
- `{{CURRENT_USER}}`: Installing user's email
- `{{CURRENT_USER_NAME}}`: Sanitized username
- `{{DEMO_FOLDER}}`: Installation path
- `{{DEMO_NAME}}`: Demo identifier
- `{{TODAY}}`: Current date

Demos are sourced from external repositories (typically `databricks-demos/dbdemos-notebooks`) and bundled into this package for distribution.

## Common Development Commands

### Building the Package

```bash
# Build wheel distribution
python setup.py clean --all bdist_wheel

# Build script (used locally)
./build.sh
```

### Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest test/test_installer.py

# Run specific test
pytest test/test_installer.py::TestInstaller::test_method_name
```

### Bundling Demos (Maintainer Workflow)

Create a `local_conf.json` file with workspace credentials (see `local_conf_example.json`):

```json
{
  "username": "user@example.com",
  "url": "https://workspace.cloud.databricks.com",
  "org_id": "1234567890",
  "pat_token": "dapi...",
  "repo_staging_path": "/Repos/user@example.com",
  "repo_name": "dbdemos-notebooks",
  "repo_url": "https://github.com/databricks-demos/dbdemos-notebooks",
  "branch": "master",
  "github_token": "ghp_..."
}
```

Then use `main.py` to bundle demos:

```python
from dbdemos.job_bundler import JobBundler
from dbdemos.packager import Packager

bundler = JobBundler(conf)
bundler.reset_staging_repo(skip_pull=False)
bundler.add_bundle("product_demos/delta-lake")  # or use load_bundles_conf() to discover all
bundler.start_and_wait_bundle_jobs(force_execution=False)

packager = Packager(conf, bundler)
packager.package_all()
```

See `test_demo.py` for a complete bundling example.

### Distribution and Release

```bash
# Full release process (bumps version, builds, uploads to PyPI, creates GitHub releases)
./build-and-distribute.sh
```

This script:
1. Verifies GitHub CLI authentication and repository access
2. Auto-increments version in `setup.py` and `dbdemos/__init__.py`
3. Builds wheel package
4. Uploads to PyPI via `twine`
5. Creates release branch and pull request
6. Creates GitHub releases on multiple repositories (`dbdemos`, `dbdemos-notebooks`, `dbdemos-dataset`, `dbdemos-resources`)

## Key Implementation Details

### Dynamic Link Replacement

Notebooks contain special attributes in HTML links that get replaced during installation:
- `dbdemos-pipeline-id="pipeline-id"`: Links to SDP pipelines
- `dbdemos-workflow-id="workflow-id"`: Links to workflows
- `dbdemos-dashboard-id="dashboard-id"`: Links to dashboards

The installer updates these links with actual resource IDs/URLs after creation.

### Resource Creation Flow

1. Parse bundle configuration
2. Create/update Git repo if specified
3. Create demo cluster (with auto-termination)
4. Install notebooks to workspace
5. Create SDP pipelines
6. Create workflows
7. Create DBSQL dashboards
8. Create Genie spaces (for AI/BI demos)
9. Update notebook links to point to created resources
10. Track installation metrics

### Cluster Configuration

Default cluster configs are in `dbdemos/resources/`:
- `default_cluster_config.json`: Standard demo cluster
- `default_test_job_conf.json`: Job cluster configuration
- Cloud-specific variants for AWS/Azure/GCP

Demos can override cluster settings in their bundle config under the `cluster` key.

### Multi-Cloud Support

The project supports AWS, Azure, and GCP. Cloud-specific configurations include:
- Instance type selection
- Storage paths (S3/ADLS/GCS)
- Authentication mechanisms
- DBR version selection

Cloud is detected automatically from workspace or specified via `cloud` parameter in `install()`.

### Serverless Support

Some demos support serverless compute. Set `serverless=True` when installing to use:
- Serverless SDP pipelines
- Serverless SQL warehouses
- Serverless notebooks (where supported)

## Testing Considerations

- Tests use local configuration files (see `local_conf_*.json` examples)
- Tests require a Databricks workspace with appropriate permissions
- Most tests are in the `test/` directory
- `test_demo.py` in root is for bundling workflow testing

## Data Collection

By default, dbdemos collects usage metrics (views, installations) to improve demo quality. This can be disabled by setting `Tracker.enable_tracker = False` in `tracker.py`. No PII is collected; only aggregate usage data and org IDs.

## Important Constraints

- Users need cluster creation, SDP pipeline creation, and DBSQL dashboard permissions
- Unity Catalog demos require a UC metastore
- Some demos have resource quotas (compute, storage)
- Pre-run notebooks require job execution in staging workspace
- Dashboard API has rate limits (especially on GCP workspaces)

## Claude Code Release Workflow

For the full release workflow, use the `/release` command which provides detailed instructions.

### Quick Reference

**Remote Execution** (`ai_release/run_remote.py`) - Test fixes before committing:
```bash
# List clusters
python ai_release/run_remote.py --list-clusters

# Start/check cluster
python ai_release/run_remote.py --start-cluster --wait-for-cluster

# Execute code
python ai_release/run_remote.py --code "print(spark.version)"

# Execute SQL
python ai_release/run_remote.py --code "SELECT 1" --language sql
```

**Job Inspection** (`ai_release/inspect_jobs.py`) - Auto-extracts errors from notebook:
```bash
# List all jobs with status
python ai_release/inspect_jobs.py --list

# List only failed jobs
python ai_release/inspect_jobs.py --list --failed-only

# Get details for a demo (auto-fetches errors if failed)
python ai_release/inspect_jobs.py --demo <name>

# Show full error traces and code
python ai_release/inspect_jobs.py --demo <name> --errors
```

**Bundle CLI** (`ai_release/bundle.py`):
```bash
# Check demo status
python ai_release/bundle.py --demo <name> --status

# Bundle from main
python ai_release/bundle.py --demo <name>

# Bundle from feature branch
python ai_release/bundle.py --demo <name> --branch <branch>

# Repair failed job (quick iteration)
python ai_release/bundle.py --demo <name> --repair

# Force full re-run
python ai_release/bundle.py --demo <name> --force

# Bundle all demos
python ai_release/bundle.py --all
```

**Fix Workflow Summary**:
1. Inspect errors: `python ai_release/inspect_jobs.py --demo <name> --errors`
2. Test fix interactively: `python ai_release/run_remote.py --code "..."`
3. Create fix branch in `../dbdemos-notebooks`: `ai-fix-<demo>-<issue>`
4. Make fix, commit, push
5. Test: `--branch ai-fix-... --force`
6. If fails, iterate with `--repair` or `--force`
7. Create PR when green
8. Human merges PR
9. Final verification from main: `--force`
10. Human runs `./build-and-distribute.sh`

**GitHub CLI Account Switch** - Use the public account for PRs:
```bash
# List accounts
gh auth status

# Switch to public account (for creating PRs on public repos)
gh auth switch --user QuentinAmbard

# Switch to enterprise account (quentin-ambard_data is EMU, can't create PRs on public repos)
gh auth switch --user quentin-ambard_data
```

**PR Status Verification** - Always check PR state before assuming:
```bash
# Check if PR is merged before running tests from main
gh pr view <PR_NUMBER> --json state,mergedAt

# Never assume a PR is not merged - always verify first
```

### Environment

- **Workspace**: `https://e2-demo-tools.cloud.databricks.com/`
- **Config**: `local_conf_E2TOOL.json` (primary) or environment variables
- **Notebooks repo**: `../dbdemos-notebooks` (configurable)
- **Test cluster**: Matches `cluster_name_pattern` in config (default: "quentin")

### Key Files

- `ai_release/inspect_jobs.py` - Job inspection CLI (auto-extracts errors)
- `ai_release/jobs.py` - Job inspection library (uses Databricks SDK)
- `ai_release/bundle.py` - Bundle CLI for demo packaging
- `ai_release/run_remote.py` - Remote code execution on clusters
- `ai_release/compute.py` - Remote execution library
- `.claude/commands/release.md` - Full release workflow documentation
- `dbdemos/job_bundler.py` - Job creation and execution logic
- `dbdemos/packager.py` - Packaging logic
- `local_conf.json` - Local configuration (gitignored, contains secrets)

### Databricks SDK

All Databricks API operations use the Python SDK: https://databricks-sdk-py.readthedocs.io/en/latest/
