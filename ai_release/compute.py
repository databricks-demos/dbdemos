"""
Remote Code Execution on Databricks Clusters

This module provides functions to execute code on Databricks clusters for testing
notebook fixes before committing them to dbdemos-notebooks.

Based on databricks-tools-core from ai-dev-kit.
"""

import datetime
import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    CommandStatus,
    ClusterSource,
    Language,
    ListClustersFilterBy,
    State,
)


class ExecutionResult:
    """Result from code execution on a Databricks cluster."""

    def __init__(
        self,
        success: bool,
        output: Optional[str] = None,
        error: Optional[str] = None,
        cluster_id: Optional[str] = None,
        cluster_name: Optional[str] = None,
        context_id: Optional[str] = None,
        context_destroyed: bool = True,
    ):
        self.success = success
        self.output = output
        self.error = error
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.context_id = context_id
        self.context_destroyed = context_destroyed

        if success and context_id and not context_destroyed:
            self.message = (
                f"Execution successful. Reuse context_id='{context_id}' with "
                f"cluster_id='{cluster_id}' for follow-up commands."
            )
        elif success:
            self.message = "Execution successful."
        else:
            self.message = f"Execution failed: {error}"

    def __repr__(self):
        if self.success:
            return f"ExecutionResult(success=True, output={repr(self.output[:100] if self.output else None)}...)"
        return f"ExecutionResult(success=False, error={repr(self.error)})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "cluster_id": self.cluster_id,
            "cluster_name": self.cluster_name,
            "context_id": self.context_id,
            "context_destroyed": self.context_destroyed,
            "message": self.message,
        }


_LANGUAGE_MAP = {
    "python": Language.PYTHON,
    "scala": Language.SCALA,
    "sql": Language.SQL,
    "r": Language.R,
}


def get_workspace_client(host: str, token: str) -> WorkspaceClient:
    """Create a WorkspaceClient with explicit credentials."""
    return WorkspaceClient(
        host=host,
        token=token,
        auth_type="pat",
        product="dbdemos-ai-release",
        product_version="0.1.0",
    )


def list_clusters(client: WorkspaceClient, include_terminated: bool = False) -> List[Dict[str, Any]]:
    """List user-created clusters in the workspace."""
    clusters = []

    # Only list user-created clusters
    user_sources = [ClusterSource.UI, ClusterSource.API]

    # Running clusters
    running_filter = ListClustersFilterBy(
        cluster_sources=user_sources,
        cluster_states=[State.RUNNING, State.PENDING, State.RESIZING, State.RESTARTING],
    )
    for cluster in client.clusters.list(filter_by=running_filter):
        clusters.append({
            "cluster_id": cluster.cluster_id,
            "cluster_name": cluster.cluster_name,
            "state": cluster.state.value if cluster.state else None,
            "creator_user_name": cluster.creator_user_name,
        })

    if include_terminated:
        terminated_filter = ListClustersFilterBy(
            cluster_sources=user_sources,
            cluster_states=[State.TERMINATED, State.TERMINATING, State.ERROR],
        )
        for cluster in client.clusters.list(filter_by=terminated_filter):
            clusters.append({
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": cluster.state.value if cluster.state else None,
                "creator_user_name": cluster.creator_user_name,
            })

    return clusters


def find_cluster_by_name(client: WorkspaceClient, name_pattern: str) -> Optional[Dict[str, Any]]:
    """
    Find a cluster by name pattern (case-insensitive).

    Args:
        client: WorkspaceClient
        name_pattern: Pattern to match (e.g., "quentin")

    Returns:
        Cluster info dict or None
    """
    clusters = list_clusters(client, include_terminated=True)
    pattern_lower = name_pattern.lower()

    # First try running clusters
    for cluster in clusters:
        if cluster["state"] == "RUNNING" and pattern_lower in cluster["cluster_name"].lower():
            return cluster

    # Then try any cluster
    for cluster in clusters:
        if pattern_lower in cluster["cluster_name"].lower():
            return cluster

    return None


def start_cluster(client: WorkspaceClient, cluster_id: str) -> Dict[str, Any]:
    """Start a terminated cluster."""
    cluster = client.clusters.get(cluster_id)
    cluster_name = cluster.cluster_name or cluster_id
    current_state = cluster.state.value if cluster.state else "UNKNOWN"

    if current_state == "RUNNING":
        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "state": "RUNNING",
            "message": f"Cluster '{cluster_name}' is already running.",
        }

    if current_state not in ("TERMINATED", "ERROR"):
        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "state": current_state,
            "message": f"Cluster '{cluster_name}' is in state {current_state}.",
        }

    client.clusters.start(cluster_id)

    return {
        "cluster_id": cluster_id,
        "cluster_name": cluster_name,
        "previous_state": current_state,
        "state": "PENDING",
        "message": f"Cluster '{cluster_name}' is starting (3-8 minutes).",
    }


def get_cluster_status(client: WorkspaceClient, cluster_id: str) -> Dict[str, Any]:
    """Get cluster status."""
    cluster = client.clusters.get(cluster_id)
    return {
        "cluster_id": cluster_id,
        "cluster_name": cluster.cluster_name or cluster_id,
        "state": cluster.state.value if cluster.state else "UNKNOWN",
    }


def wait_for_cluster(client: WorkspaceClient, cluster_id: str, timeout: int = 600) -> bool:
    """Wait for cluster to reach RUNNING state."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = get_cluster_status(client, cluster_id)
        state = status["state"]

        if state == "RUNNING":
            print(f"✓ Cluster '{status['cluster_name']}' is running")
            return True
        elif state in ("TERMINATED", "ERROR"):
            print(f"✗ Cluster '{status['cluster_name']}' is {state}")
            return False

        print(f"  Cluster state: {state}... waiting")
        time.sleep(30)

    print(f"✗ Timeout waiting for cluster")
    return False


def create_context(client: WorkspaceClient, cluster_id: str, language: str = "python") -> str:
    """Create an execution context on a cluster."""
    lang_enum = _LANGUAGE_MAP.get(language.lower(), Language.PYTHON)
    result = client.command_execution.create(
        cluster_id=cluster_id, language=lang_enum
    ).result()
    return result.id


def destroy_context(client: WorkspaceClient, cluster_id: str, context_id: str) -> None:
    """Destroy an execution context."""
    client.command_execution.destroy(cluster_id=cluster_id, context_id=context_id)


def execute_command(
    client: WorkspaceClient,
    code: str,
    cluster_id: str,
    context_id: Optional[str] = None,
    language: str = "python",
    timeout: int = 300,
    destroy_context_on_completion: bool = False,
) -> ExecutionResult:
    """
    Execute code on a Databricks cluster.

    Args:
        client: WorkspaceClient
        code: Code to execute
        cluster_id: Cluster ID
        context_id: Optional existing context ID (for state preservation)
        language: "python", "scala", "sql", or "r"
        timeout: Timeout in seconds
        destroy_context_on_completion: Whether to destroy context after execution

    Returns:
        ExecutionResult
    """
    # Get cluster name for better output
    try:
        cluster_info = client.clusters.get(cluster_id)
        cluster_name = cluster_info.cluster_name
    except Exception:
        cluster_name = cluster_id

    # Create context if not provided
    context_created = False
    if context_id is None:
        context_id = create_context(client, cluster_id, language)
        context_created = True

    lang_enum = _LANGUAGE_MAP.get(language.lower(), Language.PYTHON)

    try:
        result = client.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context_id,
            language=lang_enum,
            command=code,
        ).result(timeout=datetime.timedelta(seconds=timeout))

        if result.status == CommandStatus.FINISHED:
            # Check for error in results
            if result.results and result.results.result_type and result.results.result_type.value == "error":
                error_msg = result.results.cause if result.results.cause else "Unknown error"
                return ExecutionResult(
                    success=False,
                    error=error_msg,
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    context_id=context_id,
                    context_destroyed=False,
                )

            output = result.results.data if result.results and result.results.data else "Success (no output)"
            exec_result = ExecutionResult(
                success=True,
                output=str(output),
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                context_id=context_id,
                context_destroyed=False,
            )
        elif result.status in [CommandStatus.ERROR, CommandStatus.CANCELLED]:
            error_msg = result.results.cause if result.results and result.results.cause else "Unknown error"
            exec_result = ExecutionResult(
                success=False,
                error=error_msg,
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                context_id=context_id,
                context_destroyed=False,
            )
        else:
            exec_result = ExecutionResult(
                success=False,
                error=f"Unexpected status: {result.status}",
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                context_id=context_id,
                context_destroyed=False,
            )

        # Destroy context if requested
        if destroy_context_on_completion:
            try:
                destroy_context(client, cluster_id, context_id)
                exec_result.context_destroyed = True
            except Exception:
                pass

        return exec_result

    except TimeoutError:
        return ExecutionResult(
            success=False,
            error=f"Command timed out after {timeout}s",
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            context_id=context_id,
            context_destroyed=False,
        )
    except Exception as e:
        if context_created and destroy_context_on_completion:
            try:
                destroy_context(client, cluster_id, context_id)
            except Exception:
                pass
        return ExecutionResult(
            success=False,
            error=str(e),
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            context_id=context_id if not destroy_context_on_completion else None,
            context_destroyed=destroy_context_on_completion,
        )


def execute_file(
    client: WorkspaceClient,
    file_path: str,
    cluster_id: str,
    context_id: Optional[str] = None,
    timeout: int = 600,
    destroy_context_on_completion: bool = False,
) -> ExecutionResult:
    """
    Execute a local Python file on a Databricks cluster.

    Args:
        client: WorkspaceClient
        file_path: Path to the Python file
        cluster_id: Cluster ID
        context_id: Optional existing context ID
        timeout: Timeout in seconds
        destroy_context_on_completion: Whether to destroy context after

    Returns:
        ExecutionResult
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            code = f.read()
    except FileNotFoundError:
        return ExecutionResult(success=False, error=f"File not found: {file_path}")
    except Exception as e:
        return ExecutionResult(success=False, error=f"Failed to read file: {e}")

    if not code.strip():
        return ExecutionResult(success=False, error=f"File is empty: {file_path}")

    return execute_command(
        client=client,
        code=code,
        cluster_id=cluster_id,
        context_id=context_id,
        language="python",
        timeout=timeout,
        destroy_context_on_completion=destroy_context_on_completion,
    )
