#!/usr/bin/env python3
"""Generate an on-call markdown incident summary from incident_report.json.

If OPENAI_API_KEY is set, this script will call OpenAI for richer summaries.
If no API key is present (or API call fails with --allow-fallback), it uses local templating.
GitHub mock PR context is only fetched when configs/config.yaml sets github.enabled=true.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import unquote, urlparse

from .config_loader import load_config
from .taxonomy import (
    ROOT_CAUSE_CATEGORIES,
    category_for_error_code,
    is_valid_root_cause_category,
    normalize_root_cause_category,
)


FIX_HINTS = {
    "DB_TIMEOUT": [
        "Review recent DB timeout/retry changes in order-service data access layer.",
        "Increase command timeout carefully and add jittered backoff on retries.",
        "Inspect connection pool limits and query latency regressions around incident start.",
    ],
    "PAYMENT_429": [
        "Throttle outbound payment requests and honor provider retry-after semantics.",
        "Add circuit-breaker guardrails to avoid retry storms during provider limits.",
        "Verify recent changes to payment provider client retry policy and concurrency caps.",
    ],
    "UPSTREAM_503": [
        "Check upstream health probes and timeout budgets between gateway and payment-service.",
        "Validate recent load balancer or routing config changes.",
    ],
    "TOKEN_EXPIRED": [
        "Reintroduce reasonable JWT clock skew tolerance and validate token-expiry guardrails.",
        "Review JWKS cache TTL and refresh behavior to avoid sudden auth rejection spikes.",
        "Verify identity-provider and service clock synchronization.",
    ],
    "SKU_NOT_FOUND": [
        "Audit recent catalog sync filters and restore expected SKU inclusion criteria.",
        "Validate inventory projection/caching consistency after catalog rollout.",
        "Rollback aggressive SKU lookup timeout changes if fallback path is timing out.",
    ],
}


def load_report(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def load_adx_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        ts = row.get("TimeGenerated")
        if not ts:
            continue
        try:
            row["_ts"] = parse_time(str(ts))
        except Exception:
            continue
        rows.append(row)
    rows.sort(key=lambda r: r["_ts"])
    return rows


def normalize_tokens(text: str) -> List[str]:
    return re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.lower())


def incident_terms(incident: Dict[str, Any]) -> List[str]:
    terms: List[str] = []
    dominant = incident.get("dominant_error_code")
    if dominant:
        terms.extend(normalize_tokens(str(dominant)))
    likely = incident.get("likely_area", {})
    terms.extend(normalize_tokens(str(likely.get("service", ""))))
    terms.extend(normalize_tokens(str(likely.get("upstream_target", ""))))
    for row in incident.get("top_errors", [])[:5]:
        terms.extend(normalize_tokens(str(row.get("error", ""))))
    return list(dict.fromkeys(terms))


def incident_anchor_terms(incident: Dict[str, Any]) -> List[str]:
    terms: List[str] = []
    dominant = incident.get("dominant_error_code")
    if dominant:
        terms.extend(normalize_tokens(str(dominant)))
    likely = incident.get("likely_area", {})
    terms.extend(normalize_tokens(str(likely.get("service", ""))))
    terms.extend(normalize_tokens(str(likely.get("upstream_target", ""))))
    return list(dict.fromkeys(terms))


def incident_root_cause_category(incident: Dict[str, Any]) -> str:
    raw_category = incident.get("root_cause_category")
    candidate = normalize_root_cause_category(str(raw_category)) if raw_category is not None else None
    if candidate and is_valid_root_cause_category(candidate):
        return candidate
    dominant_code = incident.get("dominant_error_code")
    return category_for_error_code(str(dominant_code)) if dominant_code is not None else category_for_error_code(None)


def pr_text(pr: Dict[str, Any]) -> str:
    chunks: List[str] = [
        str(pr.get("title", "")),
        str(pr.get("description", "")),
    ]
    for tag in pr.get("risk_tags", []):
        chunks.append(str(tag))
    for file_row in pr.get("files", []):
        chunks.append(str(file_row.get("path", "")))
        chunks.append(str(file_row.get("summary", "")))
    return " ".join(chunks).lower()


def pr_relevance_confidence(score: int, term_count: int, anchor_hits: int, rank_index: int) -> str:
    if term_count <= 0:
        return "Low"
    coverage = score / term_count
    if rank_index == 0:
        if anchor_hits >= 2 and (score >= 4 or coverage >= 0.3):
            return "High"
        if anchor_hits >= 1 or score >= 3:
            return "Medium"
        return "Low"

    # Secondary candidates are held to stricter directness standards.
    if anchor_hits >= 2 and score >= 5:
        return "Medium"
    return "Low"


def rank_related_prs(incident: Dict[str, Any], prs: List[Dict[str, Any]], max_items: int = 3) -> List[Dict[str, Any]]:
    terms = incident_terms(incident)
    anchors = incident_anchor_terms(incident)
    if not terms or not prs:
        return []

    scored: List[Tuple[int, int, List[str], List[str], Dict[str, Any]]] = []
    for pr in prs:
        text = pr_text(pr)
        matched_terms = [term for term in terms if term in text]
        score = len(matched_terms)
        if score > 0:
            matched_anchors = [term for term in anchors if term in text]
            anchor_hits = len(matched_anchors)
            scored.append((anchor_hits, score, matched_terms, matched_anchors, pr))

    scored.sort(key=lambda row: (row[0], row[1]), reverse=True)
    top = scored[:max_items]

    out: List[Dict[str, Any]] = []
    for idx, (anchor_hits, score, matched_terms, matched_anchors, pr) in enumerate(top):
        confidence = pr_relevance_confidence(
            score=score,
            term_count=len(terms),
            anchor_hits=anchor_hits,
            rank_index=idx,
        )
        if idx == 0:
            relevance_label = "Primary suspect"
        else:
            relevance_label = "Secondary correlation"
        out.append(
            {
                "pr": pr,
                "score": score,
                "anchor_hits": anchor_hits,
                "matched_terms": matched_terms[:8],
                "matched_anchor_terms": matched_anchors[:6],
                "confidence": confidence,
                "relevance_label": relevance_label,
            }
        )
    return out


def parse_merged_at(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def fetch_json_endpoint(endpoint: str, repo_root: Path, timeout_seconds: int) -> Dict[str, Any]:
    if endpoint.startswith(("http://", "https://")):
        req = urllib.request.Request(url=endpoint, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)

    file_path: Path
    if endpoint.startswith("file://"):
        parsed = urlparse(endpoint)
        candidate = unquote(parsed.path)
        if candidate.startswith("/") and len(candidate) > 2 and candidate[2] == ":":
            candidate = candidate[1:]
        file_path = Path(candidate)
    else:
        raw = Path(endpoint)
        file_path = raw if raw.is_absolute() else (repo_root / raw)

    return json.loads(file_path.read_text(encoding="utf-8"))


def load_github_context(config: Dict[str, Any], repo_root: Path, timeout_default: int) -> Dict[str, Any]:
    github_cfg = config.get("github", {})
    if not isinstance(github_cfg, dict):
        github_cfg = {}

    enabled = bool(github_cfg.get("enabled", False))
    context: Dict[str, Any] = {
        "enabled": enabled,
        "endpoint": github_cfg.get("mock_endpoint"),
        "pull_requests": [],
        "error": None,
    }
    if not enabled:
        return context

    endpoint = str(github_cfg.get("mock_endpoint", "")).strip()
    if not endpoint:
        context["error"] = "github.enabled=true but github.mock_endpoint is empty."
        return context

    timeout_seconds = int(github_cfg.get("timeout_seconds", timeout_default))
    max_prs = int(github_cfg.get("max_pull_requests", 5))

    try:
        payload = fetch_json_endpoint(endpoint, repo_root=repo_root, timeout_seconds=timeout_seconds)
        prs = payload.get("pull_requests", []) if isinstance(payload, dict) else []
        if not isinstance(prs, list):
            prs = []
        prs = [pr for pr in prs if isinstance(pr, dict)]
        prs.sort(key=lambda pr: parse_merged_at(str(pr.get("merged_at", "1970-01-01T00:00:00Z"))), reverse=True)
        context["pull_requests"] = prs[:max_prs]
    except Exception as exc:
        context["error"] = str(exc)
    return context


def compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    props = row.get("Properties", {})
    message = str(row.get("Message", "")).splitlines()[0]
    if len(message) > 200:
        message = message[:197] + "..."
    return {
        "time": row.get("TimeGenerated"),
        "level": row.get("Level"),
        "service": row.get("AppRoleName"),
        "route": props.get("httpRoute"),
        "status": props.get("httpStatusCode"),
        "error_code": props.get("errorCode"),
        "message": message,
    }


def select_context_rows(
    adx_rows: List[Dict[str, Any]],
    start: datetime,
    end: datetime,
    limit: int,
    service: str | None = None,
    levels: Tuple[str, ...] | None = None,
    error_code: str | None = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in adx_rows:
        ts = row["_ts"]
        if ts < start or ts > end:
            continue
        if service and row.get("AppRoleName") != service:
            continue
        if levels and row.get("Level") not in levels:
            continue
        props = row.get("Properties", {})
        if error_code and props.get("errorCode") != error_code:
            continue
        out.append(compact_row(row))
        if len(out) >= limit:
            break
    return out


def build_log_context(
    report: Dict[str, Any],
    adx_rows: List[Dict[str, Any]],
    max_logs_per_incident: int,
) -> Dict[str, Any]:
    if not adx_rows:
        return {"available": False, "incidents": []}

    context_incidents: List[Dict[str, Any]] = []
    for idx, incident in enumerate(report.get("incidents", []), start=1):
        start_ts = parse_time(str(incident.get("start_time")))
        end_ts = parse_time(str(incident.get("end_time")))
        peak_ts = parse_time(str(incident.get("peak_minute", incident.get("start_time"))))
        likely = incident.get("likely_area", {})
        likely_service = str(likely.get("service", ""))
        dominant_code = incident.get("dominant_error_code")
        top_errors = incident.get("top_errors", [])
        first_error_ts = parse_time(str(top_errors[0]["first_seen"])) if top_errors else start_ts

        pre_limit = max(2, max_logs_per_incident // 8)
        onset_limit = max(5, max_logs_per_incident // 3)
        peak_limit = max(5, max_logs_per_incident // 3)
        code_limit = max(4, max_logs_per_incident // 4)

        snippets = {
            "pre_incident_normal": select_context_rows(
                adx_rows,
                start=start_ts - timedelta(minutes=2),
                end=start_ts - timedelta(seconds=1),
                limit=pre_limit,
                service=likely_service or None,
                levels=("INFO", "WARN"),
            ),
            "error_onset_window": select_context_rows(
                adx_rows,
                start=first_error_ts - timedelta(minutes=1),
                end=first_error_ts + timedelta(minutes=1),
                limit=onset_limit,
            ),
            "peak_window": select_context_rows(
                adx_rows,
                start=peak_ts - timedelta(minutes=1),
                end=peak_ts + timedelta(minutes=1),
                limit=peak_limit,
            ),
            "dominant_error_samples": select_context_rows(
                adx_rows,
                start=start_ts,
                end=end_ts,
                limit=code_limit,
                error_code=str(dominant_code) if dominant_code else None,
            ),
        }
        context_incidents.append(
            {
                "incident_index": idx,
                "incident_start": incident.get("start_time"),
                "incident_end": incident.get("end_time"),
                "likely_service": likely_service,
                "dominant_error_code": dominant_code,
                "snippets": snippets,
            }
        )

    return {"available": True, "incidents": context_incidents}


def local_summary(report: Dict[str, Any], github_context: Dict[str, Any], log_context: Dict[str, Any]) -> str:
    incidents = report.get("incidents", [])
    prs = github_context.get("pull_requests", []) if isinstance(github_context, dict) else []

    lines: List[str] = []
    if not incidents:
        lines.append("Incident Summary")
        lines.append("----------------")
        lines.append("Service: unknown")
        lines.append("Symptoms: no incident windows detected")
        lines.append("")
        lines.append("Likely Causes")
        lines.append("-------------")
        lines.append("1. No elevated incident window was detected from the provided report.")
        lines.append("")
        lines.append("Recommended Actions")
        lines.append("-------------------")
        lines.append("- Verify incident detection thresholds and input log coverage.")
        lines.append("")
        lines.append("Evidence")
        lines.append("--------")
        lines.append("- Report: zero incident windows in incident_report.json")
        lines.append("")
        return "\n".join(lines)

    context_incidents = log_context.get("incidents", [])

    for idx, incident in enumerate(incidents, start=1):
        likely = incident.get("likely_area", {})
        top_errors = incident.get("top_errors", [])[:5]
        dominant_code = str(incident.get("dominant_error_code", "unknown"))
        root_cause_category = incident_root_cause_category(incident)
        impacted = [x.get("service", "unknown") for x in incident.get("impacted_services", [])[:5]]
        related = rank_related_prs(incident, prs, max_items=3) if prs else []
        snippets = context_incidents[idx - 1].get("snippets", {}) if idx - 1 < len(context_incidents) else {}
        onset_logs = snippets.get("error_onset_window", [])
        dominant_logs = snippets.get("dominant_error_samples", [])

        service = str(likely.get("service") or (impacted[0] if impacted else "unknown"))
        evidence_row = None
        if dominant_logs:
            evidence_row = dominant_logs[0]
        elif onset_logs:
            for row in onset_logs:
                status = row.get("status")
                level = str(row.get("level", "")).upper()
                has_error_code = bool(row.get("error_code"))
                is_error_status = str(status).isdigit() and int(str(status)) >= 400
                if has_error_code or level in {"ERROR", "WARN"} or is_error_status:
                    evidence_row = row
                    break
            if evidence_row is None:
                evidence_row = onset_logs[0]

        symptoms: List[str] = [
            f"error spike ({incident.get('total_errors', 0)} errors / {incident.get('total_logs', 0)} logs)"
        ]
        if "TIMEOUT" in dominant_code.upper():
            symptoms.append("latency spike")
        symptoms.append(f"dominant error {dominant_code}")
        symptoms_line = " + ".join(dict.fromkeys(symptoms))

        cause_lines: List[str] = []
        if related:
            for rel_idx, rel in enumerate(related[:2]):
                pr = rel["pr"]
                files = pr.get("files", [])
                file_hint = files[0].get("path", "unknown-file") if files and isinstance(files[0], dict) else "unknown-file"
                number = pr.get("number", "unknown")
                title = pr.get("title", "Untitled PR")
                confidence = rel.get("confidence", "Low")
                if rel_idx == 0:
                    cause_lines.append(
                        f"Primary suspect: PR #{number} ({confidence} confidence) changed {file_hint} ({title})."
                    )
                else:
                    cause_lines.append(
                        f"Secondary correlation: PR #{number} ({confidence} confidence, less directly related) changed {file_hint} ({title})."
                    )
        elif likely.get("reason"):
            cause_lines.append(str(likely.get("reason")))
        else:
            cause_lines.append(
                f"Likely fault domain points to {likely.get('service', 'unknown')} -> {likely.get('upstream_target', 'unknown')}."
            )

        if evidence_row:
            row = evidence_row
            cause_lines.append(
                f"Log shows {row.get('error_code') or 'error'} with message: {row.get('message', 'n/a')}."
            )
        elif top_errors:
            top = top_errors[0]
            cause_lines.append(
                f"Top error {top.get('error', 'unknown')} occurred {top.get('count', 0)} times (first seen {top.get('first_seen', 'unknown')})."
            )
        else:
            cause_lines.append("Error trend shows elevated failures in the incident window.")

        action_lines: List[str] = []
        if related:
            candidates = ", ".join(f"#{rel['pr'].get('number', 'unknown')}" for rel in related[:2])
            action_lines.append(f"Rollback or hotfix candidate PRs ({candidates}).")
        hints = FIX_HINTS.get(dominant_code, [])
        if hints:
            action_lines.extend(hints[:2])
        action_lines.append("Monitor error rate for 10 minutes after mitigation.")
        deduped_actions: List[str] = []
        for action in action_lines:
            if action not in deduped_actions:
                deduped_actions.append(action)
        action_lines = deduped_actions[:3]

        evidence_lines: List[str] = []
        if evidence_row:
            row = evidence_row
            evidence_lines.append(
                f"Log: {row.get('time')} {row.get('service')} {row.get('error_code') or row.get('status')} msg=\"{row.get('message', 'n/a')}\""
            )
        elif top_errors:
            top = top_errors[0]
            evidence_lines.append(
                f"Log: {top.get('error', 'unknown')} count={top.get('count', 0)} first_seen={top.get('first_seen', 'unknown')}"
            )
        if related:
            for rel in related[:2]:
                pr = rel["pr"]
                files = pr.get("files", [])
                file_hint = files[0].get("path", "unknown-file") if files and isinstance(files[0], dict) else "unknown-file"
                confidence = rel.get("confidence", "Low")
                relevance_label = rel.get("relevance_label", "PR relevance")
                evidence_lines.append(
                    f"PR ({relevance_label}, {confidence} confidence): #{pr.get('number', 'unknown')} {pr.get('title', 'Untitled PR')} (key file {file_hint})"
                )
        else:
            evidence_lines.append("PR: no high-confidence related PR match found")

        lines.append(f"Incident {idx}")
        lines.append("")
        lines.append("Incident Summary")
        lines.append("----------------")
        lines.append(f"Service: {service}")
        lines.append(f"Symptoms: {symptoms_line}")
        lines.append("")
        lines.append("Structured Output (JSON)")
        lines.append("------------------------")
        lines.append("```json")
        lines.append(json.dumps({"root_cause_category": root_cause_category}, indent=2))
        lines.append("```")
        lines.append("")
        lines.append("Likely Causes")
        lines.append("-------------")
        for cause in cause_lines[:3]:
            lines.append(f"- {cause}")
        lines.append("")
        lines.append("Recommended Actions")
        lines.append("-------------------")
        for action in action_lines:
            lines.append(f"- {action}")
        lines.append("")
        lines.append("Evidence")
        lines.append("--------")
        for evidence in evidence_lines[:3]:
            lines.append(f"- {evidence}")
        lines.append("")

    return "\n".join(lines)


def build_prompt(report: Dict[str, Any], github_context: Dict[str, Any], log_context: Dict[str, Any]) -> str:
    taxonomy_json = json.dumps(ROOT_CAUSE_CATEGORIES, indent=2)
    return (
        "You are an SRE incident analyst. Write a concise plain-text incident brief for on-call handoff.\n"
        "Requirements:\n"
        "- For each incident, output this exact section order and titles:\n"
        "  Incident Summary\n"
        "  ----------------\n"
        "  Service: ...\n"
        "  Symptoms: ...\n"
        "\n"
        "  Structured Output (JSON)\n"
        "  ------------------------\n"
        "  ```json\n"
        "  {\"root_cause_category\": \"<CATEGORY_FROM_TAXONOMY>\"}\n"
        "  ```\n"
        "\n"
        "  Likely Causes\n"
        "  -------------\n"
        "  - Primary suspect: PR #... (High/Medium/Low confidence) ...\n"
        "  - Secondary correlation: PR #... (High/Medium/Low confidence, less directly related) ...\n"
        "  - ...\n"
        "\n"
        "  Recommended Actions\n"
        "  -------------------\n"
        "  - ...\n"
        "  - ...\n"
        "\n"
        "  Evidence\n"
        "  --------\n"
        "  - Log: ...\n"
        "  - PR: ...\n"
        "- root_cause_category MUST be one of the allowed taxonomy values exactly.\n"
        "- Allowed taxonomy values:\n"
        f"{taxonomy_json}\n"
        "- Keep each incident block concise and factual.\n"
        "- Include concrete timestamps, top error signatures, and counts where useful.\n"
        "- Correlate incidents to relevant recent PR changes with explicit ranking/confidence labels.\n"
        "- Cite PR numbers and file paths when possible.\n"
        "- Use nearby log snippets as evidence and avoid unsupported speculation.\n"
        "- Keep it factual and avoid speculation beyond the provided data.\n\n"
        "Incident report JSON:\n"
        f"{json.dumps(report, indent=2)}\n\n"
        "Recent GitHub PR context JSON (mock endpoint output, only if enabled):\n"
        f"{json.dumps(github_context, indent=2)}\n\n"
        "Nearby ADX log snippets around incident onset/peak:\n"
        f"{json.dumps(log_context, indent=2)}"
    )


def openai_summary(
    report: Dict[str, Any],
    github_context: Dict[str, Any],
    log_context: Dict[str, Any],
    model: str,
    api_key: str,
    api_base: str,
    timeout_s: int,
) -> str:
    url = api_base.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You write production incident briefs for engineers.",
            },
            {
                "role": "user",
                "content": build_prompt(report, github_context, log_context),
            },
        ],
        "temperature": 0.2,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8")
    parsed = json.loads(body)
    choices = parsed.get("choices", [])
    if not choices:
        raise RuntimeError("OpenAI response did not include choices.")
    message = choices[0].get("message", {})
    content = message.get("content")
    if not content:
        raise RuntimeError("OpenAI response did not include message content.")
    if isinstance(content, str):
        return content.strip() + "\n"
    if isinstance(content, list):
        text_parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                text_parts.append(part.get("text", ""))
        if text_parts:
            return "\n".join(text_parts).strip() + "\n"
    raise RuntimeError("Unsupported content format in OpenAI response.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate incident_summary.md from incident_report.json.")
    parser.add_argument(
        "--report-json",
        type=Path,
        default=Path("data/generated/incident_report.json"),
        help="Input report JSON path.",
    )
    parser.add_argument(
        "--adx-log",
        type=Path,
        default=Path("data/generated/adx_logs.jsonl"),
        help="ADX log file used to extract nearby context snippets.",
    )
    parser.add_argument(
        "--out-md",
        type=Path,
        default=Path("data/generated/incident_summary.md"),
        help="Output markdown summary path.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config.yaml"),
        help="Config YAML path controlling github context access.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        help="OpenAI model name.",
    )
    parser.add_argument(
        "--api-base",
        type=str,
        default=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        help="OpenAI API base URL.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="API timeout in seconds.",
    )
    parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Fallback to local templated summary if API call fails.",
    )
    parser.add_argument(
        "--max-context-logs-per-incident",
        type=int,
        default=18,
        help="Upper-bound budget for per-incident context snippets sent to the LLM.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    config_path = args.config if args.config.is_absolute() else (repo_root / args.config)

    report = load_report(args.report_json if args.report_json.is_absolute() else (repo_root / args.report_json))
    adx_log_path = args.adx_log if args.adx_log.is_absolute() else (repo_root / args.adx_log)
    adx_rows = load_adx_rows(adx_log_path)
    log_context = build_log_context(
        report=report,
        adx_rows=adx_rows,
        max_logs_per_incident=max(4, args.max_context_logs_per_incident),
    )
    config = load_config(config_path)
    github_context = load_github_context(config, repo_root=repo_root, timeout_default=args.timeout_seconds)

    api_key = os.getenv("OPENAI_API_KEY", "")
    used_openai = False

    if api_key:
        try:
            summary = openai_summary(
                report=report,
                github_context=github_context,
                log_context=log_context,
                model=args.model,
                api_key=api_key,
                api_base=args.api_base,
                timeout_s=args.timeout_seconds,
            )
            used_openai = True
        except (urllib.error.URLError, urllib.error.HTTPError, RuntimeError, json.JSONDecodeError) as exc:
            if not args.allow_fallback:
                raise RuntimeError(
                    "OpenAI summary generation failed. "
                    "Use --allow-fallback to generate a local template summary instead."
                ) from exc
            summary = local_summary(report, github_context, log_context)
    else:
        summary = local_summary(report, github_context, log_context)

    out_path = args.out_md if args.out_md.is_absolute() else (repo_root / args.out_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(summary, encoding="utf-8")

    engine = "openai" if used_openai else "local-template"
    github_msg = (
        f"enabled={github_context.get('enabled')} prs={len(github_context.get('pull_requests', []))}"
        f"{' error=' + str(github_context.get('error')) if github_context.get('error') else ''}"
    )
    print(f"Wrote incident summary to {out_path} using {engine}")
    print(f"GitHub context: {github_msg}")


if __name__ == "__main__":
    main()
