#!/usr/bin/env python3
"""Evaluate incident summaries with heuristic checks and taxonomy category accuracy."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .taxonomy import (
    ROOT_CAUSE_CATEGORIES,
    is_valid_root_cause_category,
    normalize_root_cause_category,
)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def split_incident_sections(markdown: str) -> List[str]:
    legacy_parts = re.split(r"(?m)^##\s+Incident\s+\d+\s*$", markdown)
    if len(legacy_parts) > 1:
        # parts[0] is preamble; remaining are incident sections in order.
        return [p for p in legacy_parts[1:] if p.strip()]

    plain_parts = re.split(r"(?m)^Incident\s+\d+\s*$", markdown)
    if len(plain_parts) > 1:
        return [p for p in plain_parts[1:] if p.strip()]

    # Fallback for summaries that omit explicit incident numbering.
    blocks = re.split(r"(?m)^Incident Summary\s*$", markdown)
    if len(blocks) > 1:
        return [f"Incident Summary\n{b}" for b in blocks[1:] if b.strip()]

    return [markdown]


def normalize(text: str) -> str:
    return text.lower()


def count_keyword_hits(text: str, keywords: List[str]) -> int:
    norm = normalize(text)
    return sum(1 for kw in keywords if kw.lower() in norm)


def marker_hit(text: str, markers: List[str]) -> bool:
    norm = normalize(text)
    return any(marker.lower() in norm for marker in markers)


def extract_json_objects(text: str) -> List[Dict[str, Any]]:
    objects: List[Dict[str, Any]] = []
    for match in re.finditer(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.IGNORECASE | re.DOTALL):
        snippet = match.group(1)
        try:
            parsed = json.loads(snippet)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            objects.append(parsed)
    return objects


def extract_root_cause_category(section: str) -> str | None:
    for obj in extract_json_objects(section):
        if "root_cause_category" not in obj:
            continue
        normalized = normalize_root_cause_category(str(obj.get("root_cause_category")))
        if normalized:
            return normalized

    marker = re.search(
        r'(?im)"?root_cause_category"?\s*[:=]\s*"?(?P<value>[A-Za-z0-9_\- ]+)"?',
        section,
    )
    if not marker:
        return None

    value = marker.group("value").strip().strip(",.;")
    return normalize_root_cause_category(value)


def expected_category_for_case(case: Dict[str, Any]) -> str | None:
    for key in ("expected_root_cause_category", "expected_category", "root_cause_category"):
        if key not in case:
            continue
        normalized = normalize_root_cause_category(str(case.get(key)))
        if normalized:
            return normalized
    return None


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
        expected_category = expected_category_for_case(case)
        predicted_category = extract_root_cause_category(section)
        predicted_valid = is_valid_root_cause_category(predicted_category)
        category_correct = bool(
            expected_category and predicted_valid and predicted_category == expected_category
        )

        case_results.append(
            {
                "incident_index": case.get("incident_index"),
                "name": case.get("name"),
                "passed": passed,
                "pr_reference_detected": pr_ok,
                "fix_keyword_hits": fix_hit_count,
                "fix_keyword_min_required": min_hits,
                "expected_root_cause_category": expected_category,
                "predicted_root_cause_category": predicted_category,
                "predicted_root_cause_category_valid": predicted_valid,
                "category_correct": category_correct,
            }
        )

    total = len(case_results)
    passed = sum(1 for row in case_results if row["passed"])
    score = (passed / total) if total else 0.0
    category_cases = [row for row in case_results if row["expected_root_cause_category"] is not None]
    category_total = len(category_cases)
    category_correct = sum(1 for row in category_cases if row["category_correct"])
    category_accuracy = (category_correct / category_total) if category_total else 0.0
    valid_category_predictions = sum(1 for row in case_results if row["predicted_root_cause_category_valid"])

    return {
        "generated_at": iso_now(),
        "total_cases": total,
        "passed_cases": passed,
        "score": round(score, 4),
        "heuristic_score": round(score, 4),
        "all_passed": passed == total and total > 0,
        "allowed_root_cause_categories": ROOT_CAUSE_CATEGORIES,
        "root_cause_category_cases": category_total,
        "root_cause_category_correct": category_correct,
        "category_accuracy": round(category_accuracy, 4),
        "valid_root_cause_category_predictions": valid_category_predictions,
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
    print(
        "Root-cause category accuracy: "
        f"{result['root_cause_category_correct']}/{result['root_cause_category_cases']} "
        f"(accuracy={result['category_accuracy']:.4f})"
    )
    print(f"All passed: {result['all_passed']}")
    print(f"Wrote eval result to {out_path}")


if __name__ == "__main__":
    main()
