#!/usr/bin/env python3
"""
Remote Code Execution CLI for DBDemos

Execute Python code on a Databricks cluster for testing notebook fixes.

Usage:
    # Execute code directly
    python ai_release/run_remote.py --code "print('Hello from Databricks!')"

    # Execute a file
    python ai_release/run_remote.py --file path/to/script.py

    # Execute SQL
    python ai_release/run_remote.py --code "SELECT 1" --language sql

    # List available clusters
    python ai_release/run_remote.py --list-clusters

    # Start a cluster
    python ai_release/run_remote.py --start-cluster

    # Check cluster status
    python ai_release/run_remote.py --cluster-status

    # Reuse context for faster follow-up commands
    python ai_release/run_remote.py --code "x = 1" --save-context
    python ai_release/run_remote.py --code "print(x)" --load-context

Environment Variables / Config:
    Uses local_conf_E2TOOL.json for credentials.
    Cluster is auto-selected by matching "cluster_name_pattern" (default: "quentin")
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ai_release.compute import (
    get_workspace_client,
    list_clusters,
    find_cluster_by_name,
    start_cluster,
    get_cluster_status,
    wait_for_cluster,
    execute_command,
    execute_file,
)

CONTEXT_FILE = Path(__file__).parent / ".execution_context.json"


def load_config():
    """Load configuration from local_conf_E2TOOL.json"""
    repo_root = Path(__file__).parent.parent
    conf_files = [
        repo_root / "local_conf_E2TOOL.json",
        repo_root / "local_conf.json",
    ]

    for conf_file in conf_files:
        if conf_file.exists():
            with open(conf_file, "r") as f:
                config = json.load(f)
            print(f"Loaded config from {conf_file.name}")
            return config

    print("ERROR: No config file found (local_conf_E2TOOL.json or local_conf.json)")
    sys.exit(1)


def save_context(cluster_id: str, context_id: str):
    """Save execution context for reuse."""
    with open(CONTEXT_FILE, "w") as f:
        json.dump({"cluster_id": cluster_id, "context_id": context_id}, f)
    print(f"Context saved to {CONTEXT_FILE}")


def load_context():
    """Load saved execution context."""
    if CONTEXT_FILE.exists():
        with open(CONTEXT_FILE, "r") as f:
            return json.load(f)
    return None


def clear_context():
    """Clear saved context."""
    if CONTEXT_FILE.exists():
        CONTEXT_FILE.unlink()
        print("Context cleared")


def main():
    parser = argparse.ArgumentParser(
        description="Execute code on Databricks clusters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Execution options
    parser.add_argument("--code", "-c", help="Code to execute")
    parser.add_argument("--file", "-f", help="Python file to execute")
    parser.add_argument("--language", "-l", default="python", choices=["python", "sql", "scala", "r"])
    parser.add_argument("--timeout", "-t", type=int, default=300, help="Timeout in seconds")

    # Cluster management
    parser.add_argument("--list-clusters", action="store_true", help="List available clusters")
    parser.add_argument("--start-cluster", action="store_true", help="Start the configured cluster")
    parser.add_argument("--cluster-status", action="store_true", help="Check cluster status")
    parser.add_argument("--wait-for-cluster", action="store_true", help="Wait for cluster to be running")
    parser.add_argument("--cluster-name", help="Cluster name pattern to match (default: from config or 'quentin')")

    # Context management
    parser.add_argument("--save-context", action="store_true", help="Save context for reuse")
    parser.add_argument("--load-context", action="store_true", help="Reuse saved context")
    parser.add_argument("--clear-context", action="store_true", help="Clear saved context")
    parser.add_argument("--destroy-context", action="store_true", help="Destroy context after execution")

    args = parser.parse_args()

    # Load config
    config = load_config()
    host = config.get("url", os.environ.get("DATABRICKS_HOST"))
    token = config.get("pat_token", os.environ.get("DATABRICKS_TOKEN"))
    cluster_pattern = args.cluster_name or config.get("cluster_name_pattern", "quentin")

    if not host or not token:
        print("ERROR: Missing workspace URL or token")
        sys.exit(1)

    # Create client
    client = get_workspace_client(host, token)
    print(f"Workspace: {host}")

    # Clear context
    if args.clear_context:
        clear_context()
        return 0

    # List clusters
    if args.list_clusters:
        clusters = list_clusters(client, include_terminated=True)
        print(f"\nFound {len(clusters)} clusters:\n")
        for c in clusters:
            state_icon = "🟢" if c["state"] == "RUNNING" else "🔴" if c["state"] == "TERMINATED" else "🟡"
            print(f"  {state_icon} {c['cluster_name']:<40} {c['state']:<12} {c['cluster_id']}")
        return 0

    # Find cluster
    cluster = find_cluster_by_name(client, cluster_pattern)
    if not cluster:
        print(f"ERROR: No cluster found matching '{cluster_pattern}'")
        print("Use --list-clusters to see available clusters")
        return 1

    print(f"Cluster: {cluster['cluster_name']} ({cluster['state']})")
    cluster_id = cluster["cluster_id"]

    # Cluster status
    if args.cluster_status:
        status = get_cluster_status(client, cluster_id)
        print(f"  State: {status['state']}")
        return 0

    # Start cluster
    if args.start_cluster:
        result = start_cluster(client, cluster_id)
        print(f"  {result['message']}")
        if args.wait_for_cluster and result.get("state") != "RUNNING":
            wait_for_cluster(client, cluster_id)
        return 0

    # Wait for cluster
    if args.wait_for_cluster:
        success = wait_for_cluster(client, cluster_id)
        return 0 if success else 1

    # Execute code
    if args.code or args.file:
        # Check cluster is running
        if cluster["state"] != "RUNNING":
            print(f"ERROR: Cluster is {cluster['state']}, not RUNNING")
            print("Use --start-cluster --wait-for-cluster to start it")
            return 1

        # Load context if requested
        context_id = None
        if args.load_context:
            saved = load_context()
            if saved and saved.get("cluster_id") == cluster_id:
                context_id = saved.get("context_id")
                print(f"Reusing context: {context_id}")
            else:
                print("No saved context found or cluster changed, creating new context")

        # Execute
        if args.file:
            print(f"\nExecuting file: {args.file}")
            result = execute_file(
                client=client,
                file_path=args.file,
                cluster_id=cluster_id,
                context_id=context_id,
                timeout=args.timeout,
                destroy_context_on_completion=args.destroy_context,
            )
        else:
            print(f"\nExecuting {args.language} code...")
            result = execute_command(
                client=client,
                code=args.code,
                cluster_id=cluster_id,
                context_id=context_id,
                language=args.language,
                timeout=args.timeout,
                destroy_context_on_completion=args.destroy_context,
            )

        # Print result
        print("\n" + "=" * 60)
        if result.success:
            print("✓ SUCCESS")
            print("=" * 60)
            print(result.output)
        else:
            print("✗ FAILED")
            print("=" * 60)
            print(result.error)

        # Save context if requested
        if args.save_context and result.context_id and not result.context_destroyed:
            save_context(cluster_id, result.context_id)
            print(f"\nContext saved. Use --load-context to reuse.")

        return 0 if result.success else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
