#!/usr/bin/env python3
"""Root-cause taxonomy definitions shared across analysis, summary, and evaluation."""

from __future__ import annotations

from typing import Dict, List


ROOT_CAUSE_CATEGORIES: List[str] = [
    "auth_configuration_drift",
    "datastore_capacity_saturation",
    "dependency_latency_regression",
    "resilience_fallback_misconfiguration",
    "external_provider_throttling",
    "resilience_policy_exhaustion",
    "upstream_availability_degradation",
    "client_network_interruption",
    "request_validation_regression",
    "external_business_rule_rejection",
    "data_consistency_regression",
    "unknown",
]

ROOT_CAUSE_CATEGORY_SET = set(ROOT_CAUSE_CATEGORIES)

ERROR_CODE_TO_CATEGORY: Dict[str, str] = {
    "TOKEN_EXPIRED": "auth_configuration_drift",
    "DB_TIMEOUT": "datastore_capacity_saturation",
    "UPSTREAM_TIMEOUT": "dependency_latency_regression",
    "CACHE_FALLBACK_FAIL": "resilience_fallback_misconfiguration",
    "PAYMENT_429": "external_provider_throttling",
    "RETRY_BUDGET_EXCEEDED": "resilience_policy_exhaustion",
    "UPSTREAM_503": "upstream_availability_degradation",
    "CLIENT_ABORT": "client_network_interruption",
    "ORDER_INPUT_INVALID": "request_validation_regression",
    "CARD_DECLINED": "external_business_rule_rejection",
    "SKU_NOT_FOUND": "data_consistency_regression",
}


def normalize_root_cause_category(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    return normalized or None


def is_valid_root_cause_category(value: str | None) -> bool:
    normalized = normalize_root_cause_category(value)
    return bool(normalized and normalized in ROOT_CAUSE_CATEGORY_SET)


def category_for_error_code(error_code: str | None, default: str = "unknown") -> str:
    if not error_code:
        return default
    return ERROR_CODE_TO_CATEGORY.get(str(error_code).upper(), default)
