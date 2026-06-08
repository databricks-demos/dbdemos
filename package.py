"""
package.py — bundle & package ALL dbdemos from environment-variable config.

Equivalent to the bundling flow in main.py, but driven entirely by env vars so
it can run unattended (CI, scheduled job, etc.). It scans the staging repo for
every demo, runs their bundle jobs, and packages all of them. There is no final
install test — it bundles everything and packages.

Required environment variables
-------------------------------
  DATABRICKS_HOST   Workspace URL, e.g. https://e2-demo-tools.cloud.databricks.com
  DATABRICKS_PAT    Personal access token for that workspace
  GITHUB_TOKEN      GitHub token used to pull / diff the notebooks repo

Optional environment variables (sensible defaults shown)
--------------------------------------------------------
  DBDEMOS_USERNAME        Workspace user (default: auto-detected from the token)
  DBDEMOS_ORG_ID          Workspace org/id (default: auto-detected from the token)
  DBDEMOS_REPO_STAGING    Staging /Repos path (default: /Repos/<username>)
  DBDEMOS_REPO_NAME       Repo name (default: dbdemos-notebooks)
  DBDEMOS_REPO_URL        Repo URL (default: https://github.com/databricks-demos/dbdemos-notebooks)
  DBDEMOS_BRANCH          Branch to bundle from (default: main)
  DBDEMOS_FORCE           "1"/"true" to force job re-execution (default: false)

Exit codes
----------
  0  all demos bundled & packaged successfully
  1  a clear, stage-tagged error was raised (see message)
"""
import os
import sys
import json
import traceback

from dbdemos.conf import Conf
from dbdemos.job_bundler import JobBundler
from dbdemos.packager import Packager


class PackagingError(Exception):
    """Raised when a packaging stage fails, with the stage name and root cause."""

    def __init__(self, stage: str, cause: BaseException):
        self.stage = stage
        self.cause = cause
        super().__init__(f"[stage: {stage}] {cause}")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or not value.strip():
        raise PackagingError(
            "config",
            ValueError(
                f"Missing required environment variable '{name}'. "
                f"Set DATABRICKS_HOST, DATABRICKS_PAT and GITHUB_TOKEN before running."
            ),
        )
    return value.strip()


def _optional_env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else default


def _truthy(value: str) -> bool:
    return value.lower() in ("1", "true", "yes", "on")


def _read_resource(path: str) -> str:
    try:
        with open(path, "r") as f:
            return f.read()
    except OSError as e:
        raise PackagingError("config", FileNotFoundError(f"Couldn't read resource file '{path}': {e}"))


def _detect_workspace_identity(host: str, pat_token: str):
    """Auto-detect (username, org_id) from the workspace so the caller only needs host+token."""
    try:
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient(host=host, token=pat_token)
        username = ws.current_user.me().user_name
        # workspace_id is the org_id used throughout dbdemos
        org_id = str(ws.get_workspace_id())
        return username, org_id
    except Exception as e:
        raise PackagingError(
            "config",
            RuntimeError(
                f"Couldn't authenticate to '{host}' and detect the workspace identity "
                f"(username / org_id) from DATABRICKS_PAT. Verify the host URL and token. "
                f"Root cause: {e}"
            ),
        )


def build_conf() -> Conf:
    """Build a dbdemos Conf from environment variables, auto-detecting what we can."""
    host = _require_env("DATABRICKS_HOST").rstrip("/")
    pat_token = _require_env("DATABRICKS_PAT")
    github_token = _require_env("GITHUB_TOKEN")

    username = os.environ.get("DBDEMOS_USERNAME", "").strip()
    org_id = os.environ.get("DBDEMOS_ORG_ID", "").strip()
    if not username or not org_id:
        detected_user, detected_org = _detect_workspace_identity(host, pat_token)
        username = username or detected_user
        org_id = org_id or detected_org

    repo_staging_path = _optional_env("DBDEMOS_REPO_STAGING", f"/Repos/{username}")
    repo_name = _optional_env("DBDEMOS_REPO_NAME", "dbdemos-notebooks")
    repo_url = _optional_env("DBDEMOS_REPO_URL", "https://github.com/databricks-demos/dbdemos-notebooks")
    branch = _optional_env("DBDEMOS_BRANCH", "main")

    default_cluster_template = _read_resource("./dbdemos/resources/default_cluster_config.json")
    default_cluster_job_template = _read_resource("./dbdemos/resources/default_test_job_conf.json")

    print(
        "dbdemos packaging configuration:\n"
        f"  host           : {host}\n"
        f"  username       : {username}\n"
        f"  org_id         : {org_id}\n"
        f"  repo           : {repo_url} (branch: {branch})\n"
        f"  repo_name      : {repo_name}\n"
        f"  staging_path   : {repo_staging_path}\n"
        "  (DATABRICKS_PAT / GITHUB_TOKEN loaded from env — not displayed)"
    )

    return Conf(
        username,
        host,
        org_id,
        pat_token,
        default_cluster_template,
        default_cluster_job_template,
        repo_staging_path,
        repo_name,
        repo_url,
        branch,
        github_token=github_token,
    )


def _run_stage(stage: str, fn):
    """Run a packaging stage, converting any failure into a clear PackagingError."""
    print(f"\n========== STAGE: {stage} ==========")
    try:
        return fn()
    except PackagingError:
        raise
    except Exception as e:
        raise PackagingError(stage, e) from e


def package_all_demos(conf: Conf):
    force = _truthy(_optional_env("DBDEMOS_FORCE", "false"))

    bundler = JobBundler(conf)

    _run_stage(
        "reset staging repo",
        lambda: bundler.reset_staging_repo(skip_pull=False),
    )

    _run_stage(
        "scan & load all bundles",
        bundler.load_bundles_conf,
    )

    bundle_count = len(bundler.bundles)
    if bundle_count == 0:
        raise PackagingError(
            "scan & load all bundles",
            RuntimeError(
                "No bundles found in the staging repo. Check the repo / branch / staging path."
            ),
        )
    print(f"Found {bundle_count} demo bundles to package.")

    _run_stage(
        "run & wait for all bundle jobs",
        lambda: bundler.start_and_wait_bundle_jobs(
            force_execution=force, skip_execution=False, recreate_jobs=False
        ),
    )

    packager = Packager(conf, bundler)
    _run_stage(
        "package all demos",
        packager.package_all,
    )

    print(f"\n✅ Successfully bundled & packaged all {bundle_count} demos.")


def main():
    try:
        conf = build_conf()
        package_all_demos(conf)
    except PackagingError as e:
        # Clear, stage-tagged failure. Print the chain but never the token.
        print("\n❌ PACKAGING FAILED", file=sys.stderr)
        print(f"   Stage : {e.stage}", file=sys.stderr)
        print(f"   Error : {e.cause}", file=sys.stderr)
        print("\n--- traceback ---", file=sys.stderr)
        traceback.print_exception(type(e.cause), e.cause, e.cause.__traceback__, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Anything unexpected that escaped stage handling — still fail clearly.
        print("\n❌ PACKAGING FAILED (unexpected error)", file=sys.stderr)
        print(f"   Error : {e}", file=sys.stderr)
        print("\n--- traceback ---", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
