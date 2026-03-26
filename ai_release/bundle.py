#!/usr/bin/env python3
"""
DBDemos Bundle CLI - For bundling and testing demos

This script is designed to be run by Claude Code for the release workflow.
It supports bundling specific demos, running from feature branches, and job repair.

Usage:
    # Bundle a specific demo from main branch
    python ai_release/bundle.py --demo lakehouse-retail-c360

    # Bundle from a feature branch
    python ai_release/bundle.py --demo lakehouse-retail-c360 --branch fix/retail-bug

    # Bundle all demos (uses GitHub diff to only run changed ones)
    python ai_release/bundle.py --all

    # Repair a failed job (re-run only failed tasks)
    python ai_release/bundle.py --demo lakehouse-retail-c360 --repair

    # Force full re-run (ignore commit diff optimization)
    python ai_release/bundle.py --demo lakehouse-retail-c360 --force

    # Get job status and error details
    python ai_release/bundle.py --demo lakehouse-retail-c360 --status

    # List all available demos
    python ai_release/bundle.py --list-demos

Environment Variables:
    DATABRICKS_HOST: Workspace URL (default: https://e2-demo-tools.cloud.databricks.com/)
    DATABRICKS_TOKEN: PAT token for Databricks
    GITHUB_TOKEN: GitHub token for API access
    DBDEMOS_NOTEBOOKS_PATH: Path to dbdemos-notebooks repo (default: ../dbdemos-notebooks)

Config File:
    Can also use local_conf.json in the repo root for configuration.
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dbdemos.conf import Conf, DemoConf
from dbdemos.job_bundler import JobBundler
from dbdemos.packager import Packager


def load_config(args):
    """Load configuration from environment variables and/or local_conf.json"""
    config = {}

    # Try to load from local_conf.json files (E2TOOL is primary for bundling)
    repo_root = Path(__file__).parent.parent
    conf_files = [
        repo_root / "local_conf_E2TOOL.json",  # Primary for bundling/testing
        repo_root / "local_conf.json",
    ]

    for conf_file in conf_files:
        if conf_file.exists():
            with open(conf_file, "r") as f:
                config = json.load(f)
            print(f"Loaded config from {conf_file}")
            break

    # dbdemos-notebooks path
    default_notebooks_path = str(repo_root.parent / "dbdemos-notebooks")
    notebooks_path = os.environ.get("DBDEMOS_NOTEBOOKS_PATH", config.get("dbdemos_notebooks_path", default_notebooks_path))
    config["dbdemos_notebooks_path"] = notebooks_path

    # Branch override from CLI
    if args.branch:
        config["branch"] = args.branch
    elif "branch" not in config:
        config["branch"] = "main"

    # Validate required fields
    required = ["pat_token", "github_token", "url"]
    missing = [f for f in required if not config.get(f)]
    if missing:
        print(f"ERROR: Missing required config: {missing}")
        print("Set via environment variables or local_conf.json")
        sys.exit(1)

    return config


def load_cluster_templates():
    """Load default cluster configuration templates"""
    repo_root = Path(__file__).parent.parent

    with open(repo_root / "dbdemos/resources/default_cluster_config.json", "r") as f:
        default_cluster_template = f.read()

    with open(repo_root / "dbdemos/resources/default_test_job_conf.json", "r") as f:
        default_cluster_job_template = f.read()

    return default_cluster_template, default_cluster_job_template


def create_conf(config):
    """Create Conf object from config dict"""
    default_cluster_template, default_cluster_job_template = load_cluster_templates()

    # Strip .git from repo_url if present (Conf doesn't allow it)
    repo_url = config.get("repo_url", "https://github.com/databricks-demos/dbdemos-notebooks")
    if repo_url.endswith(".git"):
        repo_url = repo_url[:-4]

    return Conf(
        username=config.get("username", "claude-code@databricks.com"),
        workspace_url=config["url"],
        org_id=config.get("org_id", ""),
        pat_token=config["pat_token"],
        default_cluster_template=default_cluster_template,
        default_cluster_job_template=default_cluster_job_template,
        repo_staging_path=config.get("repo_staging_path", "/Repos/quentin.ambard@databricks.com"),
        repo_name=config.get("repo_name", "dbdemos-notebooks"),
        repo_url=repo_url,
        branch=config["branch"],
        github_token=config["github_token"],
        run_test_as_username=config.get("run_test_as_username", "quentin.ambard@databricks.com")
    )


def list_demos(bundler: JobBundler):
    """List all available demos"""
    print("Scanning for available demos...")
    bundler.reset_staging_repo(skip_pull=False)
    bundler.load_bundles_conf()

    print(f"\nFound {len(bundler.bundles)} demos:\n")
    for path, demo_conf in sorted(bundler.bundles.items()):
        print(f"  - {demo_conf.name:<40} ({path})")

    return bundler.bundles


def get_job_status(bundler: JobBundler, demo_name: str):
    """Get detailed job status for a demo"""
    # Find the demo
    bundler.reset_staging_repo(skip_pull=True)

    # Try to find the job
    job_name = f"field-bundle_{demo_name}"
    job = bundler.db.find_job(job_name)

    if not job:
        print(f"No job found for demo: {demo_name}")
        return None

    job_id = job["job_id"]
    print(f"\n{'='*80}")
    print(f"Job: {job_name}")
    print(f"Job ID: {job_id}")
    print(f"URL: {bundler.conf.workspace_url}/#job/{job_id}")
    print(f"{'='*80}\n")

    # Get recent runs
    runs = bundler.db.get("2.1/jobs/runs/list", {"job_id": job_id, "limit": 5, "expand_tasks": "true"})

    if "runs" not in runs or len(runs["runs"]) == 0:
        print("No runs found for this job.")
        return None

    for i, run in enumerate(runs["runs"]):
        run_id = run["run_id"]
        state = run["state"]
        status = run.get("status", {})

        print(f"\n--- Run {i+1}: {run_id} ---")
        print(f"State: {state.get('life_cycle_state', 'N/A')} / {state.get('result_state', 'N/A')}")
        print(f"URL: {bundler.conf.workspace_url}/#job/{job_id}/run/{run_id}")

        if "termination_details" in status:
            print(f"Termination: {status['termination_details']}")

        # Show task details for the most recent run
        if i == 0 and "tasks" in run:
            print(f"\nTasks ({len(run['tasks'])} total):")
            for task in run["tasks"]:
                task_key = task["task_key"]
                task_state = task.get("state", {})
                task_result = task_state.get("result_state", "PENDING")

                # Get error info if failed
                error_info = ""
                if task_result == "FAILED":
                    # Try to get run output for error details
                    task_run_id = task.get("run_id")
                    if task_run_id:
                        task_output = bundler.db.get("2.1/jobs/runs/get-output", {"run_id": task_run_id})
                        if "error" in task_output:
                            error_info = f"\n    Error: {task_output['error'][:200]}..."
                        if "error_trace" in task_output:
                            error_info += f"\n    Trace: {task_output['error_trace'][:500]}..."

                status_icon = "✓" if task_result == "SUCCESS" else "✗" if task_result == "FAILED" else "○"
                print(f"  {status_icon} {task_key}: {task_result}{error_info}")

    return runs["runs"][0] if runs["runs"] else None


def wait_for_run(bundler: JobBundler, job_id: int, run_id: int):
    """Wait for a job run to complete"""
    import time
    print(f"Waiting for job completion...")
    print(f"URL: {bundler.conf.workspace_url}/#job/{job_id}/run/{run_id}")

    i = 0
    while True:
        run = bundler.db.get("2.1/jobs/runs/get", {"run_id": run_id})
        state = run.get("state", {})
        life_cycle = state.get("life_cycle_state", "UNKNOWN")

        if life_cycle not in ["RUNNING", "PENDING"]:
            result = state.get("result_state", "UNKNOWN")
            print(f"\nJob finished: {life_cycle} / {result}")
            return result == "SUCCESS"

        if i % 60 == 0:  # Print every 5 minutes
            print(f"  Still running... ({i * 5}s elapsed)")
        i += 1
        time.sleep(5)


def repair_job(bundler: JobBundler, demo_name: str, wait: bool = False):
    """Repair a failed job (re-run only failed tasks)"""
    job_name = f"field-bundle_{demo_name}"
    job = bundler.db.find_job(job_name)

    if not job:
        print(f"No job found for demo: {demo_name}")
        return False

    job_id = job["job_id"]

    # Get the most recent run
    runs = bundler.db.get("2.1/jobs/runs/list", {"job_id": job_id, "limit": 1, "expand_tasks": "true"})

    if "runs" not in runs or len(runs["runs"]) == 0:
        print("No runs found to repair.")
        return False

    latest_run = runs["runs"][0]
    run_id = latest_run["run_id"]

    # Check if run is in a repairable state
    state = latest_run["state"]
    if state.get("life_cycle_state") != "TERMINATED":
        print(f"Run is not terminated (state: {state.get('life_cycle_state')}). Cannot repair.")
        return False

    if state.get("result_state") == "SUCCESS":
        print("Run already succeeded. No repair needed.")
        return True

    # Find failed tasks
    failed_tasks = []
    for task in latest_run.get("tasks", []):
        task_state = task.get("state", {})
        if task_state.get("result_state") in ["FAILED", "CANCELED", "TIMEDOUT"]:
            failed_tasks.append(task["task_key"])

    if not failed_tasks:
        print("No failed tasks found to repair.")
        return True

    print(f"Repairing run {run_id} - re-running tasks: {failed_tasks}")

    # Call repair API
    repair_response = bundler.db.post("2.1/jobs/runs/repair", {
        "run_id": run_id,
        "rerun_tasks": failed_tasks
    })

    if "repair_id" in repair_response:
        print(f"Repair started. Repair ID: {repair_response['repair_id']}")
        print(f"URL: {bundler.conf.workspace_url}/#job/{job_id}/run/{run_id}")

        if wait:
            return wait_for_run(bundler, job_id, run_id)
        return True
    else:
        print(f"Failed to repair: {repair_response}")
        return False


def cleanup_demo_schema(bundler: JobBundler, demo_conf):
    """Drop the demo schema to ensure clean state before running.

    Uses main__build as the catalog (bundling catalog) and the demo's default_schema.
    Uses Databricks SDK: w.schemas.delete(full_name=schema_full_name, force=True)
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound

    # Bundling uses main__build catalog
    catalog = "main__build"
    schema = demo_conf.default_schema

    if not schema:
        print(f"  No default_schema defined for {demo_conf.name}, skipping cleanup")
        return

    full_schema = f"{catalog}.{schema}"
    print(f"  Cleaning up schema: {full_schema}")

    try:
        w = WorkspaceClient(
            host=bundler.conf.workspace_url,
            token=bundler.conf.pat_token
        )
        # force=True is equivalent to CASCADE
        w.schemas.delete(full_name=full_schema, force=True)
        print(f"  ✓ Schema {full_schema} dropped successfully")
    except NotFound:
        print(f"  ✓ Schema {full_schema} does not exist (nothing to clean)")
    except Exception as e:
        print(f"  WARNING: Error during schema cleanup: {e}")


def bundle_demo(bundler: JobBundler, demo_path: str, force: bool = False, skip_packaging: bool = False, cleanup_schema: bool = True):
    """Bundle a specific demo"""
    print(f"\nBundling demo: {demo_path}")
    print(f"Branch: {bundler.conf.branch}")

    bundler.reset_staging_repo(skip_pull=False)
    bundler.add_bundle(demo_path)

    if len(bundler.bundles) == 0:
        print(f"ERROR: Demo not found or not configured for bundling: {demo_path}")
        return False

    # Clean up schema before running if requested
    if cleanup_schema:
        print("\nCleaning up demo schemas...")
        for path, demo_conf in bundler.bundles.items():
            cleanup_demo_schema(bundler, demo_conf)

    # Run the job
    bundler.start_and_wait_bundle_jobs(
        force_execution=force,
        skip_execution=False,
        recreate_jobs=False
    )

    # Check results
    for path, demo_conf in bundler.bundles.items():
        if demo_conf.run_id:
            run = bundler.db.get("2.1/jobs/runs/get", {"run_id": demo_conf.run_id})
            result_state = run.get("state", {}).get("result_state", "UNKNOWN")

            if result_state == "SUCCESS":
                print(f"\n✓ Job succeeded for {demo_conf.name}")

                if not skip_packaging:
                    print("Packaging demo...")
                    packager = Packager(bundler.conf, bundler)
                    packager.package_all()
                    print(f"✓ Demo packaged successfully")

                return True
            else:
                print(f"\n✗ Job failed for {demo_conf.name}: {result_state}")
                print(f"Check: {bundler.conf.workspace_url}/#job/{demo_conf.job_id}/run/{demo_conf.run_id}")
                return False

    return False


def bundle_all(bundler: JobBundler, force: bool = False, cleanup_schema: bool = True):
    """Bundle all demos (uses diff optimization)"""
    print("\nBundling all demos...")
    print(f"Branch: {bundler.conf.branch}")

    bundler.reset_staging_repo(skip_pull=False)
    bundler.load_bundles_conf()

    print(f"Found {len(bundler.bundles)} demos")

    # Clean up schemas before running if requested
    if cleanup_schema:
        print("\nCleaning up demo schemas...")
        for path, demo_conf in bundler.bundles.items():
            cleanup_demo_schema(bundler, demo_conf)

    # Run jobs (will skip unchanged demos unless force=True)
    bundler.start_and_wait_bundle_jobs(
        force_execution=force,
        skip_execution=False,
        recreate_jobs=False
    )

    # Check results
    success_count = 0
    fail_count = 0
    skip_count = 0

    for path, demo_conf in bundler.bundles.items():
        if demo_conf.run_id:
            run = bundler.db.get("2.1/jobs/runs/get", {"run_id": demo_conf.run_id})
            result_state = run.get("state", {}).get("result_state", "UNKNOWN")

            if result_state == "SUCCESS":
                success_count += 1
            else:
                fail_count += 1
                print(f"✗ {demo_conf.name} failed: {result_state}")
        else:
            skip_count += 1

    print(f"\nResults: {success_count} succeeded, {fail_count} failed, {skip_count} skipped")

    if fail_count == 0:
        print("\nPackaging all demos...")
        packager = Packager(bundler.conf, bundler)
        packager.package_all()
        print("✓ All demos packaged successfully")
        return True
    else:
        print("\n✗ Some jobs failed. Fix errors before packaging.")
        return False


def find_demo_path(bundler: JobBundler, demo_name: str) -> str:
    """Find the full path for a demo by name"""
    bundler.reset_staging_repo(skip_pull=True)
    bundler.load_bundles_conf()

    # Check if it's already a path
    if demo_name in bundler.bundles:
        return demo_name

    # Search by demo name
    for path, demo_conf in bundler.bundles.items():
        if demo_conf.name == demo_name:
            return path

    # Partial match
    matches = []
    for path, demo_conf in bundler.bundles.items():
        if demo_name in demo_conf.name or demo_name in path:
            matches.append((path, demo_conf.name))

    if len(matches) == 1:
        return matches[0][0]
    elif len(matches) > 1:
        print(f"Multiple matches for '{demo_name}':")
        for path, name in matches:
            print(f"  - {name} ({path})")
        print("\nPlease be more specific.")
        return None

    print(f"Demo not found: {demo_name}")
    return None


def main():
    parser = argparse.ArgumentParser(
        description="DBDemos Bundle CLI - Bundle and test demos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Actions
    parser.add_argument("--demo", "-d", help="Demo name or path to bundle")
    parser.add_argument("--all", "-a", action="store_true", help="Bundle all demos")
    parser.add_argument("--list-demos", "-l", action="store_true", help="List all available demos")
    parser.add_argument("--status", "-s", action="store_true", help="Get job status for a demo")

    # Options
    parser.add_argument("--branch", "-b", help="Git branch to use (overrides config)")
    parser.add_argument("--force", "-f", action="store_true", help="Force re-run (ignore diff optimization)")
    parser.add_argument("--repair", "-r", action="store_true", help="Repair failed job (re-run failed tasks only)")
    parser.add_argument("--skip-packaging", action="store_true", help="Skip packaging step (useful for debugging)")
    parser.add_argument("--check-config", action="store_true", help="Verify configuration without running anything")
    parser.add_argument("--wait", "-w", action="store_true", help="Wait for job/repair completion")
    parser.add_argument("--no-cleanup-schema", action="store_true", help="Skip schema cleanup (default: cleanup enabled)")
    parser.add_argument("--cleanup-schema", action="store_true", default=True, help="Clean up demo schema before running (default: True)")

    args = parser.parse_args()

    # Load config
    config = load_config(args)
    conf = create_conf(config)
    bundler = JobBundler(conf)

    print(f"Workspace: {conf.workspace_url}")
    print(f"Branch: {conf.branch}")

    # Check config only
    if args.check_config:
        print(f"\n✓ Configuration valid")
        print(f"  - Username: {conf.username}")
        print(f"  - Repo: {conf.repo_url}")
        print(f"  - Repo path: {conf.get_repo_path()}")
        print(f"  - Notebooks path: {config.get('dbdemos_notebooks_path', 'N/A')}")
        return 0

    # Execute action
    if args.list_demos:
        list_demos(bundler)
        return 0

    if args.status:
        if not args.demo:
            print("ERROR: --status requires --demo")
            return 1
        get_job_status(bundler, args.demo)
        return 0

    if args.repair:
        if not args.demo:
            print("ERROR: --repair requires --demo")
            return 1
        success = repair_job(bundler, args.demo, wait=args.wait)
        return 0 if success else 1

    # Determine cleanup_schema setting (--no-cleanup-schema disables it)
    cleanup_schema = not args.no_cleanup_schema

    if args.demo:
        demo_path = find_demo_path(bundler, args.demo)
        if not demo_path:
            return 1

        # Reset bundler after find_demo_path used it
        bundler = JobBundler(conf)
        success = bundle_demo(bundler, demo_path, force=args.force, skip_packaging=args.skip_packaging, cleanup_schema=cleanup_schema)
        return 0 if success else 1

    if args.all:
        success = bundle_all(bundler, force=args.force, cleanup_schema=cleanup_schema)
        return 0 if success else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
