#!/usr/bin/env python3
"""
Job Inspection CLI for DBDemos

Inspect bundle jobs, check their status, and get detailed failure information.
Automatically extracts errors from notebook HTML when API doesn't provide them.

Usage:
    # List all bundle jobs with their status
    python ai_release/inspect_jobs.py --list

    # List only failed jobs
    python ai_release/inspect_jobs.py --list --failed-only

    # Get detailed info for a specific demo (auto-fetches errors)
    python ai_release/inspect_jobs.py --demo ai-agent

    # Get detailed failure info with fix suggestions
    python ai_release/inspect_jobs.py --demo ai-agent --errors

    # Export notebook path for the failed task
    python ai_release/inspect_jobs.py --demo ai-agent --notebook-path

    # Check if job is up-to-date with HEAD commit
    python ai_release/inspect_jobs.py --demo ai-agent --check-commit

    # Get task output for debugging
    python ai_release/inspect_jobs.py --task-output <task_run_id>

    # Export failure summary to file
    python ai_release/inspect_jobs.py --demo ai-agent --errors --output errors.txt
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ai_release.jobs import JobInspector, load_inspector_from_config, JobInfo


def format_timestamp(ts: int) -> str:
    """Format a millisecond timestamp to human-readable string."""
    if not ts:
        return "N/A"
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")


def format_duration(start: int, end: int) -> str:
    """Format duration between two timestamps."""
    if not start or not end:
        return "N/A"
    duration_sec = (end - start) / 1000
    if duration_sec < 60:
        return f"{duration_sec:.0f}s"
    elif duration_sec < 3600:
        return f"{duration_sec / 60:.1f}m"
    else:
        return f"{duration_sec / 3600:.1f}h"


def print_job_list(jobs: list, failed_only: bool = False):
    """Print a formatted list of jobs."""
    if failed_only:
        jobs = [j for j in jobs if j.latest_run and j.latest_run.failed]

    if not jobs:
        print("No jobs found.")
        return

    print(f"\n{'Demo Name':<40} {'State':<12} {'Result':<10} {'Run Time':<12} {'Commit':<10}")
    print("=" * 90)

    for job in sorted(jobs, key=lambda j: j.demo_name):
        run = job.latest_run
        if run:
            state_icon = "🟢" if run.succeeded else "🔴" if run.failed else "🟡" if run.running else "⚪"
            result = run.result_state or run.state
            duration = format_duration(run.start_time, run.end_time)
            commit = (run.used_commit or "")[:8]
        else:
            state_icon = "⚪"
            result = "NO RUNS"
            duration = "N/A"
            commit = "N/A"

        print(f"{state_icon} {job.demo_name:<38} {result:<12} {duration:<12} {commit:<10}")

    # Summary
    total = len(jobs)
    succeeded = len([j for j in jobs if j.latest_run and j.latest_run.succeeded])
    failed = len([j for j in jobs if j.latest_run and j.latest_run.failed])
    running = len([j for j in jobs if j.latest_run and j.latest_run.running])

    print(f"\nTotal: {total} | ✓ Succeeded: {succeeded} | ✗ Failed: {failed} | ◐ Running: {running}")


def print_fix_workflow(job: JobInfo, inspector: JobInspector):
    """Print suggested fix workflow for a failed job."""
    print("\n" + "=" * 80)
    print("SUGGESTED FIX WORKFLOW")
    print("=" * 80)

    demo_name = job.demo_name
    run = job.latest_run

    # Get the notebook path from the first failed task
    notebook_path = None
    if run and run.failed_tasks:
        notebook_path = run.failed_tasks[0].notebook_path

    print(f"""
1. TEST FIX INTERACTIVELY (optional but recommended):
   python ai_release/run_remote.py --start-cluster --wait-for-cluster
   python ai_release/run_remote.py --code "# test your fix code here"

2. CREATE FIX BRANCH in dbdemos-notebooks:
   cd ../dbdemos-notebooks
   git checkout main && git pull origin main
   git checkout -b ai-fix-{demo_name}-<issue>

3. EDIT THE NOTEBOOK:
   {notebook_path or 'Check the failed task notebook path above'}

4. COMMIT AND PUSH:
   git add . && git commit -m "fix: <description>" && git push origin ai-fix-{demo_name}-<issue>

5. TEST THE FIX:
   cd ../dbdemos
   python ai_release/bundle.py --demo {demo_name} --branch ai-fix-{demo_name}-<issue> --force

6. IF STILL FAILING - iterate with repair (faster):
   python ai_release/bundle.py --demo {demo_name} --repair --wait

7. CREATE PR when tests pass:
   cd ../dbdemos-notebooks
   gh pr create --title "fix: {demo_name} <issue>" --body "Fixed <issue>"

8. AFTER PR MERGED - final verification:
   cd ../dbdemos
   python ai_release/bundle.py --demo {demo_name} --force
""")


def print_job_details(job: JobInfo, inspector: JobInspector, show_errors: bool = False,
                      check_commit: bool = False, show_workflow: bool = True):
    """Print detailed information about a job."""
    print(f"\n{'=' * 80}")
    print(f"Demo: {job.demo_name}")
    print(f"Job ID: {job.job_id}")
    print(f"Job URL: {inspector.get_job_url(job.job_id)}")
    print(f"{'=' * 80}")

    run = job.latest_run
    if not run:
        print("\nNo runs found for this job.")
        return

    print(f"\nLatest Run: {run.run_id}")
    print(f"Run URL: {inspector.get_job_url(job.job_id, run.run_id)}")
    print(f"State: {run.state}")
    print(f"Result: {run.result_state or 'N/A'}")
    print(f"Started: {format_timestamp(run.start_time)}")
    print(f"Ended: {format_timestamp(run.end_time)}")
    print(f"Duration: {format_duration(run.start_time, run.end_time)}")
    print(f"Git Commit: {run.used_commit or 'N/A'}")

    if run.state_message:
        print(f"Message: {run.state_message}")

    # Check commit status
    if check_commit:
        print(f"\n--- Git Commit Check ---")
        head = inspector.get_head_commit()
        if head:
            print(f"HEAD Commit: {head}")
            if run.used_commit:
                if run.used_commit == head:
                    print("✓ Job is UP-TO-DATE with HEAD")
                else:
                    print("✗ Job is OUTDATED - HEAD has newer commits")
            else:
                print("? Cannot determine - no commit info in job run")
        else:
            print("Could not fetch HEAD commit from GitHub")

    # Print tasks
    print(f"\n--- Tasks ({len(run.tasks)} total) ---")
    for task in run.tasks:
        icon = "✓" if task.state == "SUCCESS" else "✗" if task.failed else "○"
        notebook = task.notebook_path.split("/")[-1] if task.notebook_path else "N/A"
        print(f"  {icon} {task.task_key}: {task.state} ({notebook})")

    # Print errors if job failed (always show for failed jobs, more detail with --errors)
    if run.failed_tasks:
        print(f"\n{'=' * 80}")
        print("FAILURE DETAILS")
        print(f"{'=' * 80}")

        for task in run.failed_tasks:
            print(f"\n--- Task: {task.task_key} ---")
            if task.notebook_path:
                print(f"Notebook: {task.notebook_path}")

            # Show error summary
            if task.error_message:
                print(f"\nError: {task.error_message}")

            # Show notebook errors if available
            if task.notebook_errors:
                print(f"\n--- Notebook Cell Errors ({len(task.notebook_errors)} found) ---")
                for err in task.notebook_errors:
                    print(f"\n[Cell {err.cell_index}] {err.error_name}: {err.error_message}")
                    if err.cell_source and show_errors:
                        # Show the code that caused the error
                        src = err.cell_source
                        if len(src) > 500:
                            src = src[:500] + "\n... (truncated)"
                        print(f"\nCode:\n{src}")
                    if err.error_trace and show_errors:
                        trace = err.error_trace
                        if len(trace) > 2000:
                            trace = trace[:2000] + "\n... (truncated)"
                        print(f"\nTraceback:\n{trace}")

            # Fallback to API trace if no notebook errors
            elif task.error_trace and show_errors:
                trace = task.error_trace
                if len(trace) > 3000:
                    trace = trace[:3000] + "\n... (truncated, use --task-output for full trace)"
                print(f"\nStack Trace:\n{trace}")

        # Show fix workflow for failed jobs
        if show_workflow:
            print_fix_workflow(job, inspector)


def print_task_output(inspector: JobInspector, task_run_id: int):
    """Print the full output from a task run, including exported notebook errors."""
    print(f"\n{'=' * 80}")
    print(f"Task Run ID: {task_run_id}")
    print(f"{'=' * 80}")

    # First try standard API output
    output = inspector.get_task_output(task_run_id)
    if output:
        if output.get("error"):
            print(f"\nAPI Error:\n{output['error']}")
        if output.get("error_trace"):
            print(f"\nAPI Stack Trace:\n{output['error_trace']}")
        if output.get("notebook_output"):
            print(f"\nNotebook Output:\n{output['notebook_output']}")

    # Also export and parse notebook HTML for cell-level errors
    print("\n--- Extracting errors from notebook HTML ---")
    html = inspector.export_notebook_html(task_run_id)
    if html:
        errors = inspector.extract_errors_from_html(html)
        if errors:
            print(f"Found {len(errors)} error(s) in notebook cells:")
            for err in errors:
                print(f"\n[Cell {err.cell_index}] {err.error_name}: {err.error_message}")
                if err.cell_source:
                    print(f"\nCode:\n{err.cell_source}")
                if err.error_trace:
                    print(f"\nTraceback:\n{err.error_trace}")
        else:
            print("No cell errors found in notebook HTML")
    else:
        print("Could not export notebook HTML")


def main():
    parser = argparse.ArgumentParser(
        description="Inspect DBDemos bundle jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Main actions
    parser.add_argument("--list", "-l", action="store_true", help="List all bundle jobs")
    parser.add_argument("--demo", "-d", help="Get details for a specific demo")
    parser.add_argument("--task-output", type=int, help="Get output from a specific task run ID")

    # Options
    parser.add_argument("--failed-only", "-f", action="store_true", help="Only show failed jobs")
    parser.add_argument("--errors", "-e", action="store_true", help="Show detailed error traces and code")
    parser.add_argument("--check-commit", "-c", action="store_true", help="Check if job is up-to-date with HEAD")
    parser.add_argument("--no-workflow", action="store_true", help="Don't show fix workflow suggestions")
    parser.add_argument("--notebook-path", action="store_true", help="Print only the notebook path for the first failed task")
    parser.add_argument("--output", "-o", help="Write output to file")
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    # Load inspector
    try:
        inspector = load_inspector_from_config()
        print(f"Workspace: {inspector.host}")
    except Exception as e:
        print(f"Error loading config: {e}")
        return 1

    # Redirect output to file if requested
    output_file = None
    if args.output:
        output_file = open(args.output, "w")
        sys.stdout = output_file

    try:
        # List jobs
        if args.list:
            print("\nFetching bundle jobs...")
            jobs = inspector.list_bundle_jobs(include_run_details=True)
            print_job_list(jobs, failed_only=args.failed_only)
            return 0

        # Get demo details
        if args.demo:
            print(f"\nFetching job for demo: {args.demo}")
            job = inspector.find_job(args.demo)
            if not job:
                print(f"No job found for demo: {args.demo}")
                return 1

            # Always get full details for failed jobs (to get errors)
            if job.latest_run and (job.latest_run.failed or args.errors or args.check_commit):
                print("Fetching error details...")
                job.latest_run = inspector.get_job_run_details(job.job_id, job.latest_run.run_id)

            # Just print notebook path if requested
            if args.notebook_path:
                if job.latest_run and job.latest_run.failed_tasks:
                    for task in job.latest_run.failed_tasks:
                        if task.notebook_path:
                            print(task.notebook_path)
                return 0

            if args.json:
                # Output as JSON for programmatic use
                data = {
                    "demo_name": job.demo_name,
                    "job_id": job.job_id,
                    "job_url": inspector.get_job_url(job.job_id),
                }
                if job.latest_run:
                    data["latest_run"] = {
                        "run_id": job.latest_run.run_id,
                        "state": job.latest_run.state,
                        "result_state": job.latest_run.result_state,
                        "used_commit": job.latest_run.used_commit,
                        "failed_tasks": [
                            {
                                "task_key": t.task_key,
                                "run_id": t.run_id,
                                "notebook_path": t.notebook_path,
                                "error_message": t.error_message,
                                "error_trace": t.error_trace,
                                "notebook_errors": [
                                    {
                                        "cell_index": e.cell_index,
                                        "error_name": e.error_name,
                                        "error_message": e.error_message,
                                        "cell_source": e.cell_source,
                                    }
                                    for e in t.notebook_errors
                                ] if t.notebook_errors else []
                            }
                            for t in job.latest_run.failed_tasks
                        ]
                    }
                print(json.dumps(data, indent=2))
            else:
                print_job_details(job, inspector,
                                show_errors=args.errors,
                                check_commit=args.check_commit,
                                show_workflow=not args.no_workflow)
            return 0

        # Get task output
        if args.task_output:
            print_task_output(inspector, args.task_output)
            return 0

        # No action specified
        parser.print_help()
        return 1

    finally:
        if output_file:
            output_file.close()
            sys.stdout = sys.__stdout__
            print(f"Output written to: {args.output}")


if __name__ == "__main__":
    sys.exit(main())
