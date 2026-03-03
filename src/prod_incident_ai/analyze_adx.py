#!/usr/bin/env python3
"""Analyze ADX AppTraces logs and produce an incident report."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .taxonomy import ROOT_CAUSE_CATEGORIES, category_for_error_code

CODE_AREA_HINTS = {
    "DB_TIMEOUT": ("order-service", "postgres-orders"),
    "UPSTREAM_TIMEOUT": ("order-service", "order-service"),
    "CACHE_FALLBACK_FAIL": ("inventory-service", "postgres-orders"),
    "PAYMENT_429": ("payment-service", "payment-provider"),
    "RETRY_BUDGET_EXCEEDED": ("payment-service", "payment-provider"),
    "UPSTREAM_503": ("payment-service", "payment-service"),
    "CLIENT_ABORT": ("api-gateway", "edge-lb"),
    "TOKEN_EXPIRED": ("auth-service", "identity-provider"),
    "ORDER_INPUT_INVALID": ("order-service", "order-api"),
    "CARD_DECLINED": ("payment-service", "payment-provider"),
    "SKU_NOT_FOUND": ("inventory-service", "inventory-store"),
}


@dataclass
class MinuteStat:
    minute: datetime
    total: int = 0
    errors: int = 0

    @property
    def error_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.errors / self.total


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def safe_get_props(row: Dict[str, Any]) -> Dict[str, Any]:
    props = row.get("Properties")
    return props if isinstance(props, dict) else {}


def load_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def build_minute_stats(rows: List[Dict[str, Any]]) -> List[MinuteStat]:
    minute_map: Dict[datetime, MinuteStat] = {}

    for row in rows:
        ts = parse_time(row["TimeGenerated"])
        minute = ts.replace(second=0, microsecond=0)
        if minute not in minute_map:
            minute_map[minute] = MinuteStat(minute=minute)
        stat = minute_map[minute]
        stat.total += 1
        if row.get("Level") == "ERROR":
            stat.errors += 1

    if not minute_map:
        return []

    start = min(minute_map)
    end = max(minute_map)
    out: List[MinuteStat] = []
    cursor = start
    while cursor <= end:
        out.append(minute_map.get(cursor, MinuteStat(minute=cursor)))
        cursor += timedelta(minutes=1)
    return out


def detect_windows(stats: List[MinuteStat]) -> Tuple[List[Tuple[int, int]], Dict[str, Any]]:
    if not stats:
        return [], {
            "baseline_error_rate": 0.0,
            "threshold_error_rate": 0.0,
            "average_logs_per_minute": 0.0,
            "min_errors_per_minute": 0,
        }

    rates = [s.error_rate for s in stats if s.total > 0]
    totals = [s.total for s in stats if s.total > 0]
    if rates:
        # Estimate baseline from the lower 60% of rates to reduce incident contamination.
        sorted_rates = sorted(rates)
        baseline_slice = sorted_rates[: max(1, int(len(sorted_rates) * 0.6))]
        baseline_rate = statistics.median(baseline_slice)
    else:
        baseline_rate = 0.0
    avg_total = statistics.mean(totals) if totals else 0.0

    threshold_rate = max(baseline_rate * 1.9, baseline_rate + 0.03)
    min_errors = max(4, int(avg_total * 0.03))

    spike = [s.error_rate >= threshold_rate and s.errors >= min_errors for s in stats]
    windows: List[Tuple[int, int]] = []
    start = None
    for idx, active in enumerate(spike):
        if active and start is None:
            start = idx
        elif not active and start is not None:
            windows.append((start, idx - 1))
            start = None
    if start is not None:
        windows.append((start, len(stats) - 1))

    # Merge windows separated by <=3 minute gaps.
    merged: List[Tuple[int, int]] = []
    for window in windows:
        if not merged:
            merged.append(window)
            continue
        prev_start, prev_end = merged[-1]
        cur_start, cur_end = window
        if cur_start - prev_end <= 4:
            merged[-1] = (prev_start, cur_end)
        else:
            merged.append(window)

    return merged, {
        "baseline_error_rate": round(baseline_rate, 4),
        "threshold_error_rate": round(threshold_rate, 4),
        "average_logs_per_minute": round(avg_total, 2),
        "min_errors_per_minute": min_errors,
    }


def error_fingerprint(row: Dict[str, Any]) -> str:
    props = safe_get_props(row)
    if props.get("errorSignature"):
        return str(props["errorSignature"])
    if props.get("errorCode"):
        return str(props["errorCode"])
    if row.get("ExceptionType"):
        return str(row["ExceptionType"])
    msg = str(row.get("Message", "unknown_error"))
    return msg.split("\n", 1)[0][:120]


def analyze_window(rows: List[Dict[str, Any]], stats: List[MinuteStat], start_idx: int, end_idx: int) -> Dict[str, Any]:
    start_minute = stats[start_idx].minute
    end_minute = stats[end_idx].minute + timedelta(minutes=1)

    in_window = []
    for row in rows:
        ts = parse_time(row["TimeGenerated"])
        if start_minute <= ts < end_minute:
            in_window.append(row)

    error_rows = [row for row in in_window if row.get("Level") == "ERROR"]
    if not error_rows:
        return {}

    first_error = min(parse_time(row["TimeGenerated"]) for row in error_rows)
    last_error = max(parse_time(row["TimeGenerated"]) for row in error_rows)

    error_counts: Counter[str] = Counter()
    error_first_seen: Dict[str, str] = {}
    service_counts: Counter[str] = Counter()
    upstream_counts: Counter[str] = Counter()
    code_counts: Counter[str] = Counter()
    fp_service_counts: Dict[str, Counter[str]] = defaultdict(Counter)
    fp_upstream_counts: Dict[str, Counter[str]] = defaultdict(Counter)

    for row in error_rows:
        fp = error_fingerprint(row)
        error_counts[fp] += 1
        if fp not in error_first_seen:
            error_first_seen[fp] = row["TimeGenerated"]
        service = str(row.get("AppRoleName", "unknown-service"))
        service_counts[service] += 1

        props = safe_get_props(row)
        upstream = props.get("upstreamTarget")
        if upstream:
            upstream_counts[str(upstream)] += 1
            fp_upstream_counts[fp][str(upstream)] += 1

        code = props.get("errorCode")
        if code:
            code_counts[str(code)] += 1
        fp_service_counts[fp][service] += 1

    top_errors = [
        {"error": err, "count": count, "first_seen": error_first_seen[err]}
        for err, count in error_counts.most_common(5)
    ]
    top_services = [{"service": svc, "errors": cnt} for svc, cnt in service_counts.most_common(5)]
    top_upstreams = [{"target": up, "errors": cnt} for up, cnt in upstream_counts.most_common(5)]

    slice_stats = stats[start_idx : end_idx + 1]
    peak = max(slice_stats, key=lambda s: s.error_rate)
    dominant_code = code_counts.most_common(1)[0][0] if code_counts else None
    primary_error = top_errors[0]["error"] if top_errors else None
    root_cause_category = category_for_error_code(dominant_code)

    if dominant_code and dominant_code in CODE_AREA_HINTS:
        likely_service, likely_upstream = CODE_AREA_HINTS[dominant_code]
        reason = (
            f"Dominant errorCode={dominant_code} maps to service={likely_service}, upstream={likely_upstream}, "
            f"root_cause_category={root_cause_category}."
        )
    elif primary_error:
        service_for_primary = fp_service_counts[primary_error].most_common(1)
        upstream_for_primary = fp_upstream_counts[primary_error].most_common(1)
        likely_service = service_for_primary[0][0] if service_for_primary else (top_services[0]["service"] if top_services else "unknown-service")
        likely_upstream = upstream_for_primary[0][0] if upstream_for_primary else (top_upstreams[0]["target"] if top_upstreams else "unknown-target")
        reason = (
            f"Primary signature '{primary_error}' is most concentrated in service={likely_service}, "
            f"upstream={likely_upstream}."
        )
    else:
        likely_service = top_services[0]["service"] if top_services else "unknown-service"
        likely_upstream = top_upstreams[0]["target"] if top_upstreams else "unknown-target"
        reason = f"Highest error concentration in {likely_service}, upstream={likely_upstream}."

    return {
        "start_time": iso(first_error),
        "end_time": iso(last_error),
        "window_start_minute": iso(start_minute),
        "window_end_minute": iso(end_minute),
        "total_logs": len(in_window),
        "total_errors": len(error_rows),
        "error_rate": round(len(error_rows) / max(len(in_window), 1), 4),
        "peak_minute": iso(peak.minute),
        "peak_error_rate": round(peak.error_rate, 4),
        "peak_errors": peak.errors,
        "dominant_error_code": dominant_code,
        "root_cause_category": root_cause_category,
        "top_errors": top_errors,
        "impacted_services": top_services,
        "likely_area": {
            "service": likely_service,
            "upstream_target": likely_upstream,
            "reason": reason,
        },
    }


def build_human_summary(report: Dict[str, Any]) -> str:
    incidents = report["incidents"]
    lines = [
        f"Input: {report['input_file']}",
        f"Total logs: {report['total_logs']}",
        f"Total errors: {report['total_errors']}",
        (
            "Baseline error rate: "
            f"{report['baseline']['baseline_error_rate']:.4f} "
            f"(threshold {report['baseline']['threshold_error_rate']:.4f})"
        ),
        f"Detected incidents: {len(incidents)}",
        "",
    ]
    for idx, incident in enumerate(incidents, start=1):
        lines.append(f"Incident {idx}")
        lines.append(f"  Start: {incident['start_time']}")
        lines.append(f"  End: {incident['end_time']}")
        lines.append(
            "  Errors: "
            f"{incident['total_errors']} / {incident['total_logs']} "
            f"(rate={incident['error_rate']:.4f})"
        )
        lines.append(
            "  Likely area: "
            f"{incident['likely_area']['service']} -> {incident['likely_area']['upstream_target']}"
        )
        if incident["top_errors"]:
            lines.append("  Top error: " f"{incident['top_errors'][0]['error']} ({incident['top_errors'][0]['count']})")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def analyze(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = build_minute_stats(rows)
    windows, baseline = detect_windows(stats)

    incidents = []
    for start_idx, end_idx in windows:
        incident = analyze_window(rows, stats, start_idx, end_idx)
        if incident:
            incidents.append(incident)

    if not incidents and stats:
        # Fallback: pick the single most error-heavy minute window so report is never empty.
        peak_idx = max(range(len(stats)), key=lambda i: stats[i].errors)
        fallback = analyze_window(rows, stats, max(0, peak_idx - 1), min(len(stats) - 1, peak_idx + 1))
        if fallback:
            fallback["fallback_detected"] = True
            incidents.append(fallback)

    return {
        "generated_at": iso(datetime.now(timezone.utc)),
        "total_logs": len(rows),
        "total_errors": sum(1 for row in rows if row.get("Level") == "ERROR"),
        "root_cause_taxonomy": ROOT_CAUSE_CATEGORIES,
        "baseline": baseline,
        "incidents": incidents,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze ADX logs and detect incident windows.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/generated/adx_logs.jsonl"),
        help="Input ADX jsonl file.",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=Path("data/generated/incident_report.json"),
        help="Output report JSON path.",
    )
    parser.add_argument(
        "--out-text",
        type=Path,
        default=Path("data/generated/incident_report.txt"),
        help="Output human-readable report path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_rows(args.input)
    report = analyze(rows)
    report["input_file"] = str(args.input)

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    args.out_text.write_text(build_human_summary(report), encoding="utf-8")

    print(f"Analyzed {len(rows)} rows from {args.input}")
    print(f"Detected {len(report['incidents'])} incident window(s)")
    print(f"Wrote JSON report to {args.out_json}")
    print(f"Wrote text report to {args.out_text}")


if __name__ == "__main__":
    main()
