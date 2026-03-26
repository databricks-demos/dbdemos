"""
Job Inspection Module for DBDemos

Provides functions to inspect bundle jobs, get failure details, and compare git commits.
Uses the Databricks SDK for all API operations.

SDK Documentation: https://databricks-sdk-py.readthedocs.io/en/latest/
"""

import json
import re
import requests
import urllib.parse
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path
from html import unescape

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState, RunLifeCycleState, ViewsToExport


@dataclass
class NotebookError:
    """Error extracted from a notebook cell."""
    cell_index: int
    cell_type: str  # "code", "markdown"
    error_name: str  # e.g., "NameError", "ValueError"
    error_message: str
    error_trace: Optional[str] = None
    cell_source: Optional[str] = None  # The code that caused the error


@dataclass
class TaskResult:
    """Result from a single task in a job run."""
    task_key: str
    run_id: int
    state: str  # SUCCESS, FAILED, SKIPPED, etc.
    notebook_path: Optional[str] = None
    error_message: Optional[str] = None
    error_trace: Optional[str] = None
    used_commit: Optional[str] = None
    notebook_errors: List[NotebookError] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.state in ("FAILED", "TIMEDOUT", "CANCELED")

    def get_error_summary(self) -> str:
        """Get a summary of all errors for this task."""
        if not self.failed:
            return "Task succeeded"

        lines = []
        if self.error_message:
            lines.append(f"Error: {self.error_message}")
        if self.error_trace:
            lines.append(f"Trace: {self.error_trace[:500]}...")

        for err in self.notebook_errors:
            lines.append(f"\n[Cell {err.cell_index}] {err.error_name}: {err.error_message}")
            if err.cell_source:
                # Show first 200 chars of source
                src = err.cell_source[:200]
                if len(err.cell_source) > 200:
                    src += "..."
                lines.append(f"Code: {src}")
            if err.error_trace:
                lines.append(f"Traceback:\n{err.error_trace}")

        return "\n".join(lines) if lines else "Unknown error"


@dataclass
class JobRunResult:
    """Result from a job run with all task details."""
    job_id: int
    job_name: str
    run_id: int
    state: str  # RUNNING, TERMINATED, etc.
    result_state: Optional[str] = None  # SUCCESS, FAILED, etc.
    state_message: Optional[str] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    tasks: List[TaskResult] = field(default_factory=list)
    used_commit: Optional[str] = None  # Most recent commit from tasks

    @property
    def succeeded(self) -> bool:
        return self.result_state == "SUCCESS"

    @property
    def failed(self) -> bool:
        return self.result_state in ("FAILED", "TIMEDOUT", "CANCELED")

    @property
    def running(self) -> bool:
        return self.state == "RUNNING"

    @property
    def failed_tasks(self) -> List[TaskResult]:
        return [t for t in self.tasks if t.failed]

    def get_failure_summary(self) -> str:
        """Get a human-readable summary of failures."""
        if self.succeeded:
            return "Job succeeded"

        lines = [f"Job {self.job_name} FAILED"]
        if self.state_message:
            lines.append(f"Message: {self.state_message}")

        for task in self.failed_tasks:
            lines.append(f"\n--- Task: {task.task_key} ---")
            if task.notebook_path:
                lines.append(f"Notebook: {task.notebook_path}")
            if task.error_message:
                lines.append(f"Error: {task.error_message}")
            if task.error_trace:
                # Truncate long traces
                trace = task.error_trace
                if len(trace) > 2000:
                    trace = trace[:2000] + "\n... (truncated)"
                lines.append(f"Trace:\n{trace}")

        return "\n".join(lines)


@dataclass
class JobInfo:
    """Information about a bundle job."""
    job_id: int
    job_name: str
    demo_name: str
    latest_run: Optional[JobRunResult] = None
    head_commit: Optional[str] = None
    is_up_to_date: Optional[bool] = None  # True if latest run used HEAD commit


class JobInspector:
    """
    Inspects bundle jobs and retrieves detailed failure information.
    Uses the Databricks SDK for all API operations.

    Usage:
        inspector = JobInspector(host, token, github_token, repo_url)

        # List all bundle jobs
        jobs = inspector.list_bundle_jobs()

        # Get detailed failure info
        result = inspector.get_job_run_details(job_id, run_id)
        print(result.get_failure_summary())

        # Get task error output
        output = inspector.get_task_output(task_run_id)
    """

    # Both prefixes are used - field-demos_ for demos, field-bundle_ for bundling
    JOB_PREFIXES = ["field-demos_", "field-bundle_"]

    def __init__(self, host: str, token: str, github_token: str = None, repo_url: str = None):
        self.host = host.rstrip("/")
        self.token = token
        self.github_token = github_token
        self.repo_url = repo_url

        # Create Databricks SDK client
        self.ws = WorkspaceClient(
            host=host,
            token=token,
            auth_type="pat",
            product="dbdemos-ai-release",
            product_version="0.1.0"
        )

    def _github_get(self, path: str) -> dict:
        """Make a GET request to the GitHub API."""
        if not self.github_token:
            raise ValueError("GitHub token required for this operation")
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.github_token}"
        }
        url = f"https://api.github.com/{path}"
        resp = requests.get(url, headers=headers, timeout=60)
        return resp.json()

    def list_bundle_jobs(self, include_run_details: bool = True) -> List[JobInfo]:
        """
        List all bundle jobs (jobs with 'field-bundle_' prefix).

        Args:
            include_run_details: If True, fetches latest run details for each job

        Returns:
            List of JobInfo objects
        """
        jobs = []

        # List all jobs using SDK
        for job in self.ws.jobs.list():
            name = job.settings.name if job.settings else None
            if not name:
                continue

            # Check all known prefixes
            demo_name = None
            for prefix in self.JOB_PREFIXES:
                if name.startswith(prefix):
                    demo_name = name[len(prefix):]
                    break

            if demo_name:
                job_info = JobInfo(
                    job_id=job.job_id,
                    job_name=name,
                    demo_name=demo_name
                )

                if include_run_details:
                    # Get latest run using SDK
                    runs = list(self.ws.jobs.list_runs(job_id=job.job_id, limit=1, expand_tasks=True))
                    if runs:
                        job_info.latest_run = self._parse_run(runs[0], name)

                jobs.append(job_info)

        return jobs

    def find_job(self, demo_name: str) -> Optional[JobInfo]:
        """Find a bundle job by demo name. Tries all known prefixes."""
        # Try each prefix
        for prefix in self.JOB_PREFIXES:
            job_name = f"{prefix}{demo_name}"

            # Search with name filter using SDK
            for job in self.ws.jobs.list(name=job_name):
                if job.settings and job.settings.name == job_name:
                    job_info = JobInfo(
                        job_id=job.job_id,
                        job_name=job_name,
                        demo_name=demo_name
                    )

                    # Get latest run
                    runs = list(self.ws.jobs.list_runs(job_id=job.job_id, limit=1, expand_tasks=True))
                    if runs:
                        job_info.latest_run = self._parse_run(runs[0], job_name)

                    return job_info

        return None

    def get_job_run_details(self, job_id: int, run_id: int = None) -> Optional[JobRunResult]:
        """
        Get detailed information about a job run.

        Args:
            job_id: The job ID
            run_id: Specific run ID. If None, gets the latest run.

        Returns:
            JobRunResult with full task details and errors
        """
        if run_id is None:
            # Get latest run
            runs = list(self.ws.jobs.list_runs(job_id=job_id, limit=1, expand_tasks=True))
            if not runs:
                return None
            run = runs[0]
        else:
            run = self.ws.jobs.get_run(run_id=run_id)

        # Get job name
        job = self.ws.jobs.get(job_id=job_id)
        job_name = job.settings.name if job.settings else f"job_{job_id}"

        result = self._parse_run(run, job_name)

        # For failed tasks, get detailed error output (API + notebook HTML)
        for task in result.failed_tasks:
            self.get_task_errors(task)

        return result

    def _parse_run(self, run, job_name: str) -> JobRunResult:
        """Parse a run object from SDK into a JobRunResult."""
        # Get state info
        state = run.state
        lifecycle_state = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
        result_state = state.result_state.value if state and state.result_state else None
        state_message = state.state_message if state else None

        # Parse tasks
        tasks = []
        most_recent_commit = None

        for task in (run.tasks or []):
            task_state = task.state
            task_result_state = task_state.result_state.value if task_state and task_state.result_state else "UNKNOWN"

            # Get commit from git_source
            used_commit = None
            if task.git_source and task.git_source.git_snapshot:
                used_commit = task.git_source.git_snapshot.used_commit
            if used_commit and (not most_recent_commit or used_commit > most_recent_commit):
                most_recent_commit = used_commit

            notebook_path = None
            if task.notebook_task:
                notebook_path = task.notebook_task.notebook_path

            task_result = TaskResult(
                task_key=task.task_key or "unknown",
                run_id=task.run_id or 0,
                state=task_result_state,
                notebook_path=notebook_path,
                used_commit=used_commit
            )
            tasks.append(task_result)

        return JobRunResult(
            job_id=run.job_id or 0,
            job_name=job_name,
            run_id=run.run_id or 0,
            state=lifecycle_state,
            result_state=result_state,
            state_message=state_message,
            start_time=run.start_time,
            end_time=run.end_time,
            tasks=tasks,
            used_commit=most_recent_commit
        )

    def get_task_output(self, task_run_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the output/error from a specific task run.

        Args:
            task_run_id: The task's run_id (not the job run_id)

        Returns:
            Dict with 'error' and 'error_trace' if available
        """
        try:
            output = self.ws.jobs.get_run_output(run_id=task_run_id)
            return {
                "error": output.error,
                "error_trace": output.error_trace,
                "metadata": str(output.metadata) if output.metadata else None,
                "notebook_output": str(output.notebook_output) if output.notebook_output else None
            }
        except Exception as e:
            return {"error": str(e)}

    def export_notebook_html(self, task_run_id: int) -> Optional[str]:
        """
        Export the notebook HTML from a task run.

        Args:
            task_run_id: The task's run_id

        Returns:
            HTML content of the notebook with outputs, or None if failed
        """
        try:
            export = self.ws.jobs.export_run(run_id=task_run_id, views_to_export=ViewsToExport.ALL)
            if export.views and len(export.views) > 0:
                return export.views[0].content
            return None
        except Exception as e:
            print(f"Failed to export notebook: {e}")
            return None

    def extract_errors_from_html(self, html_content: str) -> List[NotebookError]:
        """
        Parse notebook HTML and extract error information from failed cells.
        The HTML contains a base64+URL encoded JSON model with command details.

        Args:
            html_content: The HTML content from export_notebook_html

        Returns:
            List of NotebookError objects for each failed cell
        """
        import base64

        errors = []

        # Find the notebook model in the HTML - it's base64 then URL encoded JSON
        match = re.search(r'__DATABRICKS_NOTEBOOK_MODEL = \'([^\']+)\'', html_content)
        if not match:
            return errors

        try:
            encoded = match.group(1)
            # Decode: base64 -> URL encoding -> JSON
            decoded_bytes = base64.b64decode(encoded)
            url_encoded = decoded_bytes.decode('utf-8')
            json_str = urllib.parse.unquote(url_encoded)
            model = json.loads(json_str)
        except Exception as e:
            print(f"Failed to parse notebook model: {e}")
            return errors

        # Extract errors from commands
        for idx, cmd in enumerate(model.get('commands', [])):
            state = cmd.get('state')
            error_summary = cmd.get('errorSummary')
            error = cmd.get('error')

            # Skip non-error commands and "Command skipped" errors (not the root cause)
            if not (error or error_summary) or state != 'error':
                continue
            if error_summary == 'Command skipped':
                continue

            # Get command source
            cell_source = cmd.get('command', '')

            # Parse error name and message
            error_name = "Error"
            error_message = error_summary or "Unknown error"
            error_trace = None

            # Try to parse Python exception from error_summary
            exc_match = re.search(r'(\w+Error|\w+Exception):\s*(.+)', error_summary or '')
            if exc_match:
                error_name = exc_match.group(1)
                error_message = exc_match.group(2).strip()

            # Clean ANSI codes from error trace
            if error:
                # Remove ANSI escape codes
                error_trace = re.sub(r'\x1b\[[0-9;]*m', '', str(error))

            errors.append(NotebookError(
                cell_index=idx,
                cell_type="code",
                error_name=error_name,
                error_message=error_message,
                error_trace=error_trace,
                cell_source=cell_source[:500] if cell_source else None  # Truncate source
            ))

        return errors

    def get_task_errors(self, task: TaskResult) -> TaskResult:
        """
        Get comprehensive error information for a failed task.
        First tries API, then falls back to exporting and parsing notebook HTML.

        Args:
            task: TaskResult to enrich with error information

        Returns:
            The same TaskResult with error fields populated
        """
        # First try the standard API
        output = self.get_task_output(task.run_id)
        if output:
            task.error_message = output.get("error")
            task.error_trace = output.get("error_trace")

        # If no error from API, export and parse the notebook
        if not task.error_message and not task.error_trace:
            html = self.export_notebook_html(task.run_id)
            if html:
                errors = self.extract_errors_from_html(html)
                task.notebook_errors = errors

                # Set primary error from first notebook error
                if errors:
                    first_err = errors[0]
                    task.error_message = f"{first_err.error_name}: {first_err.error_message}"
                    task.error_trace = first_err.error_trace

        return task

    def get_head_commit(self) -> Optional[str]:
        """Get the HEAD commit SHA from the GitHub repo."""
        if not self.repo_url or not self.github_token:
            return None

        # Extract owner/repo from URL
        match = re.search(r'github\.com[/:]([^/]+)/([^/\.]+)', self.repo_url)
        if not match:
            return None

        owner, repo = match.groups()
        resp = self._github_get(f"repos/{owner}/{repo}/commits/HEAD")
        return resp.get("sha")

    def check_job_up_to_date(self, job_info: JobInfo) -> bool:
        """
        Check if a job's latest run used the HEAD commit.

        Args:
            job_info: JobInfo with latest_run populated

        Returns:
            True if the job was run with the latest commit
        """
        if not job_info.latest_run or not job_info.latest_run.used_commit:
            return False

        head_commit = self.get_head_commit()
        if not head_commit:
            return False

        job_info.head_commit = head_commit
        job_info.is_up_to_date = job_info.latest_run.used_commit == head_commit
        return job_info.is_up_to_date

    def get_failed_jobs(self) -> List[JobInfo]:
        """Get all bundle jobs that have a failed latest run."""
        all_jobs = self.list_bundle_jobs(include_run_details=True)
        return [j for j in all_jobs if j.latest_run and j.latest_run.failed]

    def get_job_url(self, job_id: int, run_id: int = None) -> str:
        """Get the workspace URL for a job or run."""
        if run_id:
            return f"{self.host}/#job/{job_id}/run/{run_id}"
        return f"{self.host}/#job/{job_id}"


def load_inspector_from_config() -> JobInspector:
    """Load a JobInspector using the local config file."""
    repo_root = Path(__file__).parent.parent
    conf_files = [
        repo_root / "local_conf_E2TOOL.json",
        repo_root / "local_conf.json",
    ]

    config = None
    for conf_file in conf_files:
        if conf_file.exists():
            with open(conf_file, "r") as f:
                config = json.load(f)
            break

    if not config:
        raise FileNotFoundError("No config file found")

    # Clean repo_url
    repo_url = config.get("repo_url", "")
    if repo_url.endswith(".git"):
        repo_url = repo_url[:-4]

    return JobInspector(
        host=config["url"],
        token=config["pat_token"],
        github_token=config.get("github_token"),
        repo_url=repo_url
    )
