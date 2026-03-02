#!/usr/bin/env python3
"""Heuristic evaluation for incident summary quality against expected PR/fix signals."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def split_incident_sections(markdown: str) -> List[str]:
    parts = re.split(r"(?m)^##\s+Incident\s+\d+\s*$", markdown)
    # parts[0] is preamble; remaining are incident sections in order.
    return [p for p in parts[1:]]


def normalize(text: str) -> str:
    return text.lower()


def count_keyword_hits(text: str, keywords: List[str]) -> int:
    norm = normalize(text)
    return sum(1 for kw in keywords if kw.lower() in norm)


def marker_hit(text: str, markers: List[str]) -> bool:
    norm = normalize(text)
    return any(marker.lower() in norm for marker in markers)


def evaluate(summary_md: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    sections = split_incident_sections(summary_md)
    full_text = summary_md
    case_results: List[Dict[str, Any]] = []

    for case in spec.get("cases", []):
        idx = int(case.get("incident_index", 0)) - 1
        section = sections[idx] if 0 <= idx < len(sections) else full_text
        pr_ok = marker_hit(section, case.get("required_pr_markers", []))
        fix_hit_count = count_keyword_hits(section, case.get("required_fix_keywords", []))
        min_hits = int(case.get("min_fix_keyword_hits", 1))
        fix_ok = fix_hit_count >= min_hits
        passed = pr_ok and fix_ok

        case_results.append(
            {
                "incident_index": case.get("incident_index"),
                "name": case.get("name"),
                "passed": passed,
                "pr_reference_detected": pr_ok,
                "fix_keyword_hits": fix_hit_count,
                "fix_keyword_min_required": min_hits,
            }
        )

    total = len(case_results)
    passed = sum(1 for row in case_results if row["passed"])
    score = (passed / total) if total else 0.0

    return {
        "generated_at": iso_now(),
        "total_cases": total,
        "passed_cases": passed,
        "score": round(score, 4),
        "all_passed": passed == total and total > 0,
        "cases": case_results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate incident summary against mock PR/fix expectations.")
    parser.add_argument(
        "--summary-md",
        type=Path,
        default=Path("data/generated/incident_summary.md"),
        help="Summary markdown path.",
    )
    parser.add_argument(
        "--eval-spec",
        type=Path,
        default=Path("data/eval/incident_eval_cases.json"),
        help="Evaluation spec JSON path.",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=Path("data/generated/incident_eval_result.json"),
        help="Output evaluation JSON path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary_path = args.summary_md
    spec_path = args.eval_spec
    out_path = args.out_json

    summary_md = summary_path.read_text(encoding="utf-8")
    spec = load_json(spec_path)
    result = evaluate(summary_md, spec)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"Evaluated summary: {summary_path}")
    print(f"Cases passed: {result['passed_cases']}/{result['total_cases']} (score={result['score']:.4f})")
    print(f"All passed: {result['all_passed']}")
    print(f"Wrote eval result to {out_path}")


if __name__ == "__main__":
    main()
