"""
Run state management for AI release workflow.

Tracks job runs, errors, and fixes in a persistent folder structure:
  ai_release/runs/
    <commit_id>/
      state.json           - Overall run state
      <demo_name>/
        status.json        - Demo-specific status
        errors.json        - Extracted errors from failed runs
        fix_attempts.json  - History of fix attempts
        job_output.log     - Raw job output
        notes.md           - AI notes and observations
"""

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any


RUNS_DIR = Path(__file__).parent / "runs"


@dataclass
class DemoRunState:
    """State for a single demo run."""
    demo_name: str
    status: str = "pending"  # pending, running, success, failed, fixing
    job_id: Optional[int] = None
    run_id: Optional[int] = None
    branch: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_summary: Optional[str] = None
    fix_attempts: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "DemoRunState":
        return cls(**data)


@dataclass
class RunState:
    """Overall state for a release run."""
    commit_id: str
    branch: str = "main"
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    demos: Dict[str, DemoRunState] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "commit_id": self.commit_id,
            "branch": self.branch,
            "started_at": self.started_at,
            "demos": {k: v.to_dict() for k, v in self.demos.items()}
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RunState":
        demos = {k: DemoRunState.from_dict(v) for k, v in data.get("demos", {}).items()}
        return cls(
            commit_id=data["commit_id"],
            branch=data.get("branch", "main"),
            started_at=data.get("started_at", ""),
            demos=demos
        )


class RunStateManager:
    """Manages persistent run state for AI release workflow."""

    def __init__(self, commit_id: Optional[str] = None):
        """Initialize with a specific commit or auto-detect from git."""
        if commit_id is None:
            commit_id = self._get_current_commit()
        self.commit_id = commit_id
        self.run_dir = RUNS_DIR / commit_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.state = self._load_or_create_state()

    def _get_current_commit(self) -> str:
        """Get current git commit from dbdemos-notebooks."""
        import subprocess
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=Path(__file__).parent.parent.parent / "dbdemos-notebooks",
                capture_output=True, text=True
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _load_or_create_state(self) -> RunState:
        """Load existing state or create new one."""
        state_file = self.run_dir / "state.json"
        if state_file.exists():
            with open(state_file) as f:
                return RunState.from_dict(json.load(f))
        return RunState(commit_id=self.commit_id)

    def save(self):
        """Save current state to disk."""
        state_file = self.run_dir / "state.json"
        with open(state_file, "w") as f:
            json.dump(self.state.to_dict(), f, indent=2)

    def get_demo_dir(self, demo_name: str) -> Path:
        """Get or create directory for a demo."""
        demo_dir = self.run_dir / demo_name
        demo_dir.mkdir(parents=True, exist_ok=True)
        return demo_dir

    def get_demo_state(self, demo_name: str) -> DemoRunState:
        """Get state for a specific demo."""
        if demo_name not in self.state.demos:
            self.state.demos[demo_name] = DemoRunState(demo_name=demo_name)
        return self.state.demos[demo_name]

    def update_demo_status(self, demo_name: str, status: str, **kwargs):
        """Update demo status and save."""
        demo_state = self.get_demo_state(demo_name)
        demo_state.status = status
        for key, value in kwargs.items():
            if hasattr(demo_state, key):
                setattr(demo_state, key, value)

        if status == "running" and not demo_state.started_at:
            demo_state.started_at = datetime.now().isoformat()
        elif status in ("success", "failed"):
            demo_state.completed_at = datetime.now().isoformat()

        self.save()
        self._save_demo_status(demo_name, demo_state)

    def _save_demo_status(self, demo_name: str, state: DemoRunState):
        """Save demo-specific status file."""
        demo_dir = self.get_demo_dir(demo_name)
        with open(demo_dir / "status.json", "w") as f:
            json.dump(state.to_dict(), f, indent=2)

    def save_errors(self, demo_name: str, errors: List[Dict[str, Any]]):
        """Save extracted errors for a demo."""
        demo_dir = self.get_demo_dir(demo_name)
        with open(demo_dir / "errors.json", "w") as f:
            json.dump({
                "extracted_at": datetime.now().isoformat(),
                "errors": errors
            }, f, indent=2)

    def save_job_output(self, demo_name: str, output: str):
        """Save raw job output."""
        demo_dir = self.get_demo_dir(demo_name)
        with open(demo_dir / "job_output.log", "w") as f:
            f.write(output)

    def add_fix_attempt(self, demo_name: str, description: str, branch: str, files_changed: List[str]):
        """Record a fix attempt."""
        demo_state = self.get_demo_state(demo_name)
        attempt = {
            "timestamp": datetime.now().isoformat(),
            "description": description,
            "branch": branch,
            "files_changed": files_changed,
            "result": "pending"
        }
        demo_state.fix_attempts.append(attempt)
        self.save()

        # Also save to fix_attempts.json
        demo_dir = self.get_demo_dir(demo_name)
        with open(demo_dir / "fix_attempts.json", "w") as f:
            json.dump(demo_state.fix_attempts, f, indent=2)

    def update_fix_result(self, demo_name: str, result: str):
        """Update the result of the latest fix attempt."""
        demo_state = self.get_demo_state(demo_name)
        if demo_state.fix_attempts:
            demo_state.fix_attempts[-1]["result"] = result
            self.save()

    def add_note(self, demo_name: str, note: str):
        """Add a note to the demo's notes.md file."""
        demo_dir = self.get_demo_dir(demo_name)
        notes_file = demo_dir / "notes.md"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(notes_file, "a") as f:
            f.write(f"\n## {timestamp}\n\n{note}\n")

    def get_summary(self) -> str:
        """Get a summary of all demo states."""
        lines = [
            f"# Release Run: {self.commit_id}",
            f"Branch: {self.state.branch}",
            f"Started: {self.state.started_at}",
            "",
            "## Demo Status",
            ""
        ]

        for name, demo in sorted(self.state.demos.items()):
            status_emoji = {
                "pending": "⏳",
                "running": "🔄",
                "success": "✅",
                "failed": "❌",
                "fixing": "🔧"
            }.get(demo.status, "❓")

            line = f"- {status_emoji} **{name}**: {demo.status}"
            if demo.error_summary:
                line += f" - {demo.error_summary[:50]}..."
            if demo.fix_attempts:
                line += f" ({len(demo.fix_attempts)} fix attempts)"
            lines.append(line)

        return "\n".join(lines)

    @classmethod
    def list_runs(cls) -> List[str]:
        """List all existing run directories."""
        if not RUNS_DIR.exists():
            return []
        return sorted([d.name for d in RUNS_DIR.iterdir() if d.is_dir()])

    @classmethod
    def get_latest_run(cls) -> Optional["RunStateManager"]:
        """Get the most recent run state manager."""
        runs = cls.list_runs()
        if not runs:
            return None
        return cls(runs[-1])


# Convenience functions
def get_run_state(commit_id: Optional[str] = None) -> RunStateManager:
    """Get or create a run state manager."""
    return RunStateManager(commit_id)


def get_latest_run() -> Optional[RunStateManager]:
    """Get the latest run state."""
    return RunStateManager.get_latest_run()
