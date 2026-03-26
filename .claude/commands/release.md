# DBDemos Release Workflow

You are helping with the dbdemos release process. This involves bundling demos from the `dbdemos-notebooks` repository, testing them, fixing any issues, and preparing for release.

## ⛔ CRITICAL WARNINGS

1. **NEVER run a release to PyPI by yourself** - Only the human can trigger `./build-and-distribute.sh`
2. **NEVER commit secrets** - PAT tokens, GitHub tokens must never appear in commits or outputs
3. **NEVER push directly to main** - Always use feature branches and PRs
4. **NEVER cleanup workspace resources yourself** - Always ask the human to do cleanup

## 📚 Notebook Code Quality Principles (CRITICAL)

**The dbdemos-notebooks are state-of-the-art examples that customers will reuse.** Code must be:

1. **Clean and minimal** - No unnecessary code, no hacks, no workarounds
2. **Simple and readable** - Easy to understand for learning purposes
3. **Safe to re-run** - Notebooks must work when run multiple times (idempotent)
4. **No error handling hacks** - Don't add try/except blocks to work around specific errors
5. **No comments explaining errors** - Don't add comments like "handles BudgetPolicy error"

### What NOT to do:
```python
# BAD - Don't add error handling for specific workspace issues
try:
    agents.deploy(...)
except NotFound as e:
    if "BudgetPolicy" in str(e):
        # cleanup and retry...
```

### What TO do instead:
- If a job fails due to stale data/resources, **ASK THE HUMAN** to clean up the workspace
- Never attempt cleanup yourself - the human must do it
- Fix the root cause in the code, not the symptom

### Handling Stale Resource Errors:
**Note:** The bundler now automatically cleans up schemas before running (via `DROP SCHEMA CASCADE`).
This should prevent most stale resource errors. If you still encounter issues:

- `BudgetPolicy not found` → Schema cleanup should fix this, or ask human to delete the serving endpoint
- `Model version already exists` → Should be fixed by schema cleanup
- `Endpoint already exists` → Should be fixed by schema cleanup
- `Table already exists` → Should be fixed by schema cleanup

If automatic cleanup fails or you need manual intervention, **ask the human** to run:
```sql
DROP SCHEMA IF EXISTS main__build.<schema_name> CASCADE;
```
Or delete specific resources via the Databricks UI/API.

## Overview

The dbdemos package bundles notebooks from the `dbdemos-notebooks` repository. The bundling process:
1. Creates/updates jobs in a Databricks workspace that run the notebooks
2. Waits for job completion
3. Downloads executed notebooks with outputs
4. Packages them into the `dbdemos/bundles/` directory

## Environment Setup

Before starting, verify these are available:
- `DATABRICKS_TOKEN` or token in `local_conf_E2TOOL.json`
- `GITHUB_TOKEN` or token in `local_conf_E2TOOL.json`
- Workspace: `https://e2-demo-tools.cloud.databricks.com/`
- dbdemos-notebooks repo at: `../dbdemos-notebooks` (configurable)
- Test cluster: Matches `cluster_name_pattern` in config (default: "quentin")

## AI Release Tools Location

All AI-powered release tools are in `ai_release/`:
- `ai_release/bundle.py` - Bundle and test demos
- `ai_release/run_remote.py` - Execute code on Databricks clusters
- `ai_release/compute.py` - Remote execution library
- `ai_release/run_state.py` - Persistent state tracking for runs
- `ai_release/jobs.py` - Job inspection library (uses Databricks SDK)
- `ai_release/inspect_jobs.py` - CLI for job inspection

## ⏱️ Important: Job Run Times

**Bundle jobs typically take 15-30 minutes to complete.** Each job runs all notebooks in a demo on a Databricks cluster.

- Do NOT wait synchronously for jobs to complete
- Start the job, then work on other tasks or let the user know to check back later
- Use `--status` to check job progress without blocking
- The state tracking system persists progress across sessions

---

## Part 0: Run State Tracking

The AI release workflow tracks state persistently in `ai_release/runs/`:

```
ai_release/runs/
  <commit_id>/
    state.json           # Overall run state
    <demo_name>/
      status.json        # Demo-specific status
      errors.json        # Extracted errors from failed runs
      fix_attempts.json  # History of fix attempts
      job_output.log     # Raw job output
      notes.md           # AI notes and observations
```

### Using Run State in Python
```python
from ai_release.run_state import get_run_state, get_latest_run

# Get or create state for current commit
state = get_run_state()

# Update demo status
state.update_demo_status("ai-agent", "running", job_id=123, run_id=456)

# Save errors
state.save_errors("ai-agent", [{"cell": 5, "error": "ImportError..."}])

# Record a fix attempt
state.add_fix_attempt("ai-agent", "Remove protobuf constraint", "ai-fix-ai-agent-pip", ["01_create_first_billing_agent.py"])

# Add notes
state.add_note("ai-agent", "The pip install fails due to protobuf<5 conflict with grpcio-status")

# Get summary
print(state.get_summary())

# Resume from previous session
state = get_latest_run()
```

### When to Use State Tracking
- Before starting a bundle job: `state.update_demo_status(demo, "running", ...)`
- After job completes: `state.update_demo_status(demo, "success")` or `"failed"`
- When extracting errors: `state.save_errors(demo, errors)`
- When making a fix: `state.add_fix_attempt(demo, description, branch, files)`
- To add context for future sessions: `state.add_note(demo, note)`

---

## Part 1: Remote Code Execution (Testing Fixes)

Before committing a fix to dbdemos-notebooks, test it interactively on a cluster.

### List Available Clusters
```bash
python ai_release/run_remote.py --list-clusters
```

### Check/Start the Test Cluster
```bash
# Check status
python ai_release/run_remote.py --cluster-status

# Start if not running (will ask for confirmation)
python ai_release/run_remote.py --start-cluster --wait-for-cluster
```

### Execute Code for Testing
```bash
# Execute Python code
python ai_release/run_remote.py --code "print(spark.version)"

# Execute SQL
python ai_release/run_remote.py --code "SELECT current_catalog()" --language sql

# Execute a file
python ai_release/run_remote.py --file path/to/test_script.py

# With longer timeout (default 300s)
python ai_release/run_remote.py --code "long_running_code()" --timeout 600
```

### Context Reuse (Faster Follow-up Commands)
```bash
# First command - save context
python ai_release/run_remote.py --code "x = spark.range(100)" --save-context

# Follow-up commands reuse context (faster, keeps variables)
python ai_release/run_remote.py --code "x.count()" --load-context

# Clear context when done
python ai_release/run_remote.py --clear-context
```

---

## Part 2: Bundling Commands

### Check Configuration
```bash
python ai_release/bundle.py --check-config
```

### Check Status of a Demo
```bash
python ai_release/bundle.py --demo <demo-name> --status
```
This shows recent job runs, task status, and error details.

### Bundle a Specific Demo (from main)
```bash
python ai_release/bundle.py --demo <demo-name>
```

### Bundle from a Feature Branch
```bash
python ai_release/bundle.py --demo <demo-name> --branch <branch-name>
```

### Force Re-run (ignore diff optimization)
```bash
python ai_release/bundle.py --demo <demo-name> --force
```

### Repair Failed Job (re-run only failed tasks)
```bash
python ai_release/bundle.py --demo <demo-name> --repair
```
Use this for quick iteration when debugging. After fixing, always do a full re-run.

Add `--wait` to wait for completion:
```bash
python ai_release/bundle.py --demo <demo-name> --repair --wait
```

### Schema Cleanup (Default: Enabled)
By default, the bundler automatically drops the demo schema (`main__build.<schema>`) before running.
This ensures a clean state and avoids stale resource errors.

```bash
# Cleanup is enabled by default - these are equivalent:
python ai_release/bundle.py --demo <demo-name>
python ai_release/bundle.py --demo <demo-name> --cleanup-schema

# To skip cleanup (not recommended unless debugging):
python ai_release/bundle.py --demo <demo-name> --no-cleanup-schema
```

### Bundle All Demos
```bash
python ai_release/bundle.py --all
```
This uses GitHub diff API to only run demos with changed files.

### List Available Demos
```bash
python ai_release/bundle.py --list-demos
```

---

## Part 3: Fixing a Failed Demo - Complete Workflow

When a demo fails, follow this workflow:

### Step 1: Identify the Error
```bash
# Get job status with auto-extracted errors from notebook cells
python ai_release/inspect_jobs.py --demo <demo-name>

# For full error traces and failing code
python ai_release/inspect_jobs.py --demo <demo-name> --errors

# List all failed jobs
python ai_release/inspect_jobs.py --list --failed-only
```
The inspection tool automatically:
- Fetches the job run details
- Exports the notebook HTML
- Extracts cell-level errors with traceback
- Shows the exact code that failed
- Suggests a fix workflow

Common issues:
- Missing/incompatible dependencies (pip install failures)
- API changes in Databricks
- Data schema changes
- Cluster configuration issues

### Step 2: Test the Fix Interactively (Optional but Recommended)

Before touching the notebooks, test your fix on a cluster:
```bash
# Start cluster if needed
python ai_release/run_remote.py --start-cluster --wait-for-cluster

# Test your fix code
python ai_release/run_remote.py --code "
# Your fix code here
df = spark.read.table('your_table')
# ...
"
```

### Step 3: Create a Fix Branch in dbdemos-notebooks
```bash
cd ../dbdemos-notebooks
git checkout main
git pull origin main
git checkout -b ai-fix-<demo-name>-<issue>
```

### Step 4: Make the Fix
Edit the notebook files in `../dbdemos-notebooks`. The notebooks are `.py` files using Databricks notebook format.

### Step 5: Commit and Push
```bash
cd ../dbdemos-notebooks
git add .
git commit -m "fix: <description of fix>"
git push origin ai-fix-<demo-name>-<issue>
```

### Step 6: Test the Fix (Full Re-run)
```bash
cd ../dbdemos
python ai_release/bundle.py --demo <demo-name> --branch ai-fix-<demo-name>-<issue> --force
```

### Step 7: If Still Failing - Iterate
```bash
# Make more fixes in dbdemos-notebooks
cd ../dbdemos-notebooks
# ... edit files ...
git add . && git commit -m "fix: additional fixes" && git push

# Quick test with repair (faster, but use full re-run for final verification)
cd ../dbdemos
python ai_release/bundle.py --demo <demo-name> --repair --wait

# Or full re-run if dependencies changed
python ai_release/bundle.py --demo <demo-name> --branch ai-fix-<demo-name>-<issue> --force
```

### Step 8: Create PR (When Tests Pass)
```bash
cd ../dbdemos-notebooks
gh pr create --title "fix: <description>" --body "## Summary
- Fixed <issue>

## Testing
- Bundling job passed: <link to job run>

🤖 Generated with Claude Code"
```

### Step 9: After PR is Merged - Final Verification
Wait for the human to merge the PR, then:
```bash
cd ../dbdemos
python ai_release/bundle.py --demo <demo-name> --force
```
Report the result to the human.

---

## Part 4: Full Release Workflow

When all demos are working and you're asked to prepare a release:

### Step 1: Bundle All Demos from Main
```bash
python ai_release/bundle.py --all --force
```

### Step 2: Verify All Passed
Check output for any failures. If any failed, fix them first.

### Step 3: Report to Human
Tell the human:
- All demos bundled successfully
- Any changes made
- Ready for PyPI release

### Step 4: Human Runs Release
**The human will run:** `./build-and-distribute.sh`
**You must NEVER run this yourself.**

---

## Useful Information

### Demo Path Structure
Demos are located in paths like:
- `product_demos/Delta-Lake/delta-lake`
- `demo-retail/lakehouse-retail-c360`
- `aibi/aibi-marketing-campaign`

### Job Naming Convention
Jobs are named: `field-bundle_<demo-name>`

### Bundle Config Location
Each demo has a config at: `<demo-path>/_resources/bundle_config`

### Workspace URLs
- Jobs: `https://e2-demo-tools.cloud.databricks.com/#job/<job_id>`
- Runs: `https://e2-demo-tools.cloud.databricks.com/#job/<job_id>/run/<run_id>`

### Package Versioning Rules (IMPORTANT)

When fixing `%pip install` lines in notebooks, follow these rules for Databricks packages:

**Always use latest (no version pin):**
- `databricks-langchain` - use latest
- `databricks-agents` - use latest
- `databricks-feature-engineering` - use latest (NOT pinned like `==0.12.1`)
- `databricks-sdk` - use latest
- `databricks-mcp` - use latest

**Use minimum version (`>=`):**
- `mlflow>=3.10.1` - minimum version constraint is OK

**Never pin these constraints (they cause conflicts):**
- `protobuf<5` - REMOVE, conflicts with grpcio-status
- `cryptography<43` - REMOVE, unnecessary constraint

**Example - BAD:**
```
%pip install mlflow>=3.10.1 databricks-feature-engineering==0.12.1 protobuf<5 cryptography<43
```

**Example - GOOD:**
```
%pip install mlflow>=3.10.1 databricks-langchain databricks-agents databricks-feature-engineering
```

### Common Errors and Fixes

1. **"couldn't get notebook for run... You probably did a run repair"**
   - Solution: Do a full re-run with `--force`

2. **"last job failed for demo X. Can't package"**
   - Solution: Fix the failing notebook, then re-run

3. **API rate limits (429 errors)**
   - The script auto-retries. If persistent, wait a few minutes.

4. **"Couldn't pull the repo"**
   - Git conflicts in workspace. May need manual resolution.

5. **Cluster not running**
   - Use `python ai_release/run_remote.py --start-cluster --wait-for-cluster`

6. **pip install CalledProcessError with protobuf/cryptography conflicts**
   - Remove `protobuf<5` and `cryptography<43` constraints
   - Remove pinned versions like `databricks-feature-engineering==0.12.1`
   - See "Package Versioning Rules" above

---

## Files Reference

- `ai_release/inspect_jobs.py` - Job inspection CLI (auto-extracts errors from notebooks)
- `ai_release/jobs.py` - Job inspection library (uses Databricks SDK)
- `ai_release/bundle.py` - Main CLI for bundling
- `ai_release/run_remote.py` - Remote code execution CLI
- `ai_release/compute.py` - Remote execution library
- `ai_release/run_state.py` - Persistent state tracking for runs
- `ai_release/runs/` - Directory containing run state (gitignored)
- `dbdemos/job_bundler.py` - Job creation and execution
- `dbdemos/packager.py` - Packaging executed notebooks
- `local_conf_E2TOOL.json` - Local configuration (gitignored)

## SDK Documentation

Databricks SDK for Python: https://databricks-sdk-py.readthedocs.io/en/latest/
- `../dbdemos-notebooks/` - Source notebooks repository
