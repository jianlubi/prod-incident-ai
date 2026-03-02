#!/usr/bin/env python3
"""Run end-to-end incident pipeline: generate -> analyze -> summarize.

This script auto-loads environment variables from .env at repository root.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


PLACEHOLDER_KEYS = {
    "your_openai_api_key_here",
    "your_key_here",
    "YOUR_KEY",
    "changeme",
}


def load_dotenv(path: Path) -> Dict[str, str]:
    env_updates: Dict[str, str] = {}
    if not path.exists():
        return env_updates

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        if key == "OPENAI_API_KEY" and (not value or value in PLACEHOLDER_KEYS):
            continue
        env_updates[key] = value
    return env_updates


def run_step(cmd: List[str], cwd: Path, env: Dict[str, str]) -> None:
    printable = " ".join(shlex.quote(part) for part in cmd)
    print(f"[pipeline] Running: {printable}")
    subprocess.run(cmd, cwd=str(cwd), env=env, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run generate/analyze/summarize pipeline.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config.yaml"),
        help="Config YAML path (used by summarizer github toggle).",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Path to .env file (default: repo-root .env).",
    )
    parser.add_argument(
        "--scenario-file",
        type=Path,
        default=Path("data/scenarios/default_scenarios.json"),
        help="Scenario file for generator.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/generated"),
        help="Output directory for generated files.",
    )
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--start-time", type=str, default=None)
    parser.add_argument("--duration-minutes", type=int, default=None)
    parser.add_argument("--baseline-rps", type=float, default=None)
    parser.add_argument("--incident-error-ratio", type=float, default=None)
    parser.add_argument(
        "--strict-llm",
        action="store_true",
        help="Fail if OpenAI summary generation fails (no fallback).",
    )
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-analyze", action="store_true")
    parser.add_argument("--skip-summarize", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    scripts_dir = repo_root / "scripts"

    env = os.environ.copy()
    dotenv_path = args.env_file if args.env_file.is_absolute() else (repo_root / args.env_file)
    updates = load_dotenv(dotenv_path)
    for key, value in updates.items():
        env.setdefault(key, value)

    out_dir = args.out_dir if args.out_dir.is_absolute() else (repo_root / args.out_dir)
    scenario_file = args.scenario_file if args.scenario_file.is_absolute() else (repo_root / args.scenario_file)
    config_file = args.config if args.config.is_absolute() else (repo_root / args.config)
    adx_file = out_dir / "adx_logs.jsonl"
    report_json = out_dir / "incident_report.json"
    report_txt = out_dir / "incident_report.txt"
    summary_md = out_dir / "incident_summary.md"

    if not args.skip_generate:
        cmd = [
            sys.executable,
            str(scripts_dir / "generate_logs.py"),
            "--scenario-file",
            str(scenario_file),
            "--out-dir",
            str(out_dir),
        ]
        if args.seed is not None:
            cmd += ["--seed", str(args.seed)]
        if args.start_time is not None:
            cmd += ["--start-time", args.start_time]
        if args.duration_minutes is not None:
            cmd += ["--duration-minutes", str(args.duration_minutes)]
        if args.baseline_rps is not None:
            cmd += ["--baseline-rps", str(args.baseline_rps)]
        if args.incident_error_ratio is not None:
            cmd += ["--incident-error-ratio", str(args.incident_error_ratio)]
        run_step(cmd, repo_root, env)

    if not args.skip_analyze:
        cmd = [
            sys.executable,
            str(scripts_dir / "analyze_adx.py"),
            "--input",
            str(adx_file),
            "--out-json",
            str(report_json),
            "--out-text",
            str(report_txt),
        ]
        run_step(cmd, repo_root, env)

    if not args.skip_summarize:
        cmd = [
            sys.executable,
            str(scripts_dir / "summarize_incident.py"),
            "--report-json",
            str(report_json),
            "--out-md",
            str(summary_md),
            "--config",
            str(config_file),
        ]
        if not args.strict_llm:
            cmd.append("--allow-fallback")
        run_step(cmd, repo_root, env)

    print("[pipeline] Completed successfully.")
    print(f"[pipeline] ADX logs: {adx_file}")
    print(f"[pipeline] Analysis JSON: {report_json}")
    print(f"[pipeline] Analysis text: {report_txt}")
    print(f"[pipeline] Incident summary: {summary_md}")


if __name__ == "__main__":
    main()
