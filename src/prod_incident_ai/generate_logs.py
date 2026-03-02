#!/usr/bin/env python3
"""Generate ADX-style production logs with mixed normal and incident errors.

Output:
  - data/generated/adx_logs.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from uuid import uuid4


SERVICES = ["api-gateway", "auth-service", "order-service", "payment-service", "inventory-service"]
ROUTES = ["/health", "/login", "/orders", "/orders/{id}", "/checkout", "/inventory/{sku}"]
METHODS = ["GET", "POST", "PUT"]
REGIONS = ["eastus", "centralus"]
VERSIONS = ["2026.3.1", "2026.3.2", "2026.3.3"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "okhttp/4.12.0",
    "curl/8.6.0",
]
UPSTREAM_BY_SERVICE = {
    "api-gateway": "edge-lb",
    "auth-service": "identity-provider",
    "order-service": "postgres-orders",
    "payment-service": "payment-provider",
    "inventory-service": "redis-cache",
}

TENANT_ID = "26f4b3ef-7f17-4f9e-bf77-cad0ad7b5d77"
SUBSCRIPTION_ID = "9a4f2740-2de5-48ee-9de3-8f59a3e83e71"
RESOURCE_ID = (
    f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/rg-prod-observability/"
    "providers/microsoft.insights/components/prod-incident-ai"
)

ERROR_PROFILES: Dict[str, Dict[str, Any]] = {
    "DB_TIMEOUT": {
        "type": "Npgsql.NpgsqlException",
        "message_templates": [
            "Exception while reading from stream: timeout expired after 1500 ms.",
            "Timeout during ExecuteReaderAsync; pooled connection did not respond within 1500 ms.",
        ],
        "logger": "Order.Infrastructure.Repositories.OrderReadRepository",
        "status_code": 504,
        "stack_frames": [
            "Npgsql.NpgsqlConnector.<ReadMessageLong>d__215.MoveNext()",
            "Npgsql.NpgsqlCommand.ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)",
            "Order.Infrastructure.Repositories.OrderReadRepository.GetOrdersAsync(Guid accountId, CancellationToken ct)",
            "Order.API.Controllers.OrdersController.Get(CancellationToken ct)",
        ],
    },
    "UPSTREAM_TIMEOUT": {
        "type": "System.TimeoutException",
        "message_templates": [
            "Upstream request to order-service exceeded configured timeout of 2.0 s.",
            "Gateway timeout while proxying request to order-service.",
        ],
        "logger": "Gateway.Proxy.Forwarder",
        "status_code": 504,
        "stack_frames": [
            "Yarp.ReverseProxy.Forwarder.HttpForwarder.SendAsync(HttpContext context, string destinationPrefix)",
            "Gateway.Proxy.Forwarder.ForwardAsync(HttpContext context, CancellationToken ct)",
            "Gateway.Middleware.RequestPipeline.Invoke(HttpContext context)",
        ],
    },
    "CACHE_FALLBACK_FAIL": {
        "type": "System.Data.SqlClient.SqlException",
        "message_templates": [
            "Cache miss fallback failed; SQL command timed out (CommandTimeout=2).",
            "Redis miss triggered DB fallback; backend read timed out.",
        ],
        "logger": "Inventory.Application.Query.InventoryQueryHandler",
        "status_code": 500,
        "stack_frames": [
            "System.Data.SqlClient.SqlCommand.ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)",
            "Inventory.Infrastructure.Repositories.InventoryRepository.GetBySkuAsync(string sku, CancellationToken ct)",
            "Inventory.Application.Query.InventoryQueryHandler.Handle(GetInventoryQuery request, CancellationToken ct)",
        ],
    },
    "PAYMENT_429": {
        "type": "System.Net.Http.HttpRequestException",
        "message_templates": [
            "POST https://api.paymentprovider.com/v1/charges returned 429 Too Many Requests.",
            "Payment provider responded 429; retry-after header=2.",
        ],
        "logger": "Payment.Infrastructure.Provider.PaymentProviderClient",
        "status_code": 502,
        "stack_frames": [
            "System.Net.Http.HttpResponseMessage.EnsureSuccessStatusCode()",
            "Payment.Infrastructure.Provider.PaymentProviderClient.CreateChargeAsync(PaymentRequest req, CancellationToken ct)",
            "Payment.Application.ChargeService.AuthorizeAsync(Order order, CancellationToken ct)",
        ],
    },
    "RETRY_BUDGET_EXCEEDED": {
        "type": "Polly.CircuitBreaker.BrokenCircuitException",
        "message_templates": [
            "Retry budget exhausted for operation CreateCharge after 3 attempts.",
            "Circuit is open for payment provider dependency; fast-fail triggered.",
        ],
        "logger": "Payment.Application.Resilience.PolicyExecutor",
        "status_code": 503,
        "stack_frames": [
            "Polly.CircuitBreaker.AsyncCircuitBreakerEngine.ImplementationAsync[TResult](Func`3 action, Context context, CancellationToken cancellationToken)",
            "Payment.Application.Resilience.PolicyExecutor.ExecuteAsync[T](Func`1 action, CancellationToken ct)",
            "Payment.API.Controllers.CheckoutController.Post(CheckoutRequest request, CancellationToken ct)",
        ],
    },
    "UPSTREAM_503": {
        "type": "System.Net.Http.HttpRequestException",
        "message_templates": [
            "Received 503 from payment-service while proxying /checkout.",
            "Upstream service unavailable: payment-service returned 503.",
        ],
        "logger": "Gateway.Proxy.Forwarder",
        "status_code": 503,
        "stack_frames": [
            "Yarp.ReverseProxy.Forwarder.HttpForwarder.SendAsync(HttpContext context, string destinationPrefix)",
            "Gateway.Proxy.Forwarder.ForwardAsync(HttpContext context, CancellationToken ct)",
            "Gateway.Middleware.RequestPipeline.Invoke(HttpContext context)",
        ],
    },
    "CLIENT_ABORT": {
        "type": "Microsoft.AspNetCore.Connections.ConnectionResetException",
        "message_templates": [
            "The client reset the request stream before response completion.",
            "An existing connection was forcibly closed by the remote host.",
        ],
        "logger": "Microsoft.AspNetCore.Server.Kestrel",
        "status_code": 499,
        "stack_frames": [
            "Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http.Http1OutputProducer.WriteDataToPipeAsync(ReadOnlySpan`1 data, CancellationToken cancellationToken)",
            "Gateway.API.Middleware.ResponseWriter.WriteAsync(HttpContext context)",
        ],
    },
    "TOKEN_EXPIRED": {
        "type": "System.UnauthorizedAccessException",
        "message_templates": [
            "JWT validation failed: token exp claim is in the past.",
            "Bearer token rejected by auth-service due to expiration.",
        ],
        "logger": "Auth.API.Middleware.JwtValidationMiddleware",
        "status_code": 401,
        "stack_frames": [
            "Microsoft.IdentityModel.Tokens.Validators.ValidateLifetime(DateTime? notBefore, DateTime? expires, SecurityToken securityToken, TokenValidationParameters validationParameters)",
            "Auth.API.Middleware.JwtValidationMiddleware.Invoke(HttpContext context)",
        ],
    },
    "ORDER_INPUT_INVALID": {
        "type": "FluentValidation.ValidationException",
        "message_templates": [
            "Validation failed for CheckoutRequest: LineItems must contain at least one item.",
            "Request body rejected: field shippingAddress.postalCode did not match expected format.",
        ],
        "logger": "Order.API.Validation.CheckoutValidator",
        "status_code": 400,
        "stack_frames": [
            "FluentValidation.AbstractValidator`1.ValidateAndThrow(T instance)",
            "Order.API.Validation.CheckoutValidator.Validate(CheckoutRequest request)",
            "Order.API.Controllers.CheckoutController.Post(CheckoutRequest request, CancellationToken ct)",
        ],
    },
    "CARD_DECLINED": {
        "type": "PaymentProvider.CardDeclinedException",
        "message_templates": [
            "Provider declined card with code do_not_honor.",
            "Card authorization failed: insufficient_funds.",
        ],
        "logger": "Payment.Infrastructure.Provider.PaymentProviderClient",
        "status_code": 402,
        "stack_frames": [
            "PaymentProvider.Sdk.Charges.CreateAsync(ChargeRequest request, CancellationToken ct)",
            "Payment.Infrastructure.Provider.PaymentProviderClient.CreateChargeAsync(PaymentRequest req, CancellationToken ct)",
            "Payment.Application.ChargeService.AuthorizeAsync(Order order, CancellationToken ct)",
        ],
    },
    "SKU_NOT_FOUND": {
        "type": "Inventory.Domain.SkuNotFoundException",
        "message_templates": [
            "SKU SKU-48291 was not found in active catalog.",
            "Requested SKU does not exist or is disabled.",
        ],
        "logger": "Inventory.Application.Query.InventoryQueryHandler",
        "status_code": 404,
        "stack_frames": [
            "Inventory.Infrastructure.Repositories.InventoryRepository.GetBySkuAsync(string sku, CancellationToken ct)",
            "Inventory.Application.Query.InventoryQueryHandler.Handle(GetInventoryQuery request, CancellationToken ct)",
            "Inventory.API.Controllers.InventoryController.Get(string sku, CancellationToken ct)",
        ],
    },
}

BACKGROUND_ERROR_CODES = ["CLIENT_ABORT", "TOKEN_EXPIRED", "ORDER_INPUT_INVALID", "CARD_DECLINED", "SKU_NOT_FOUND"]

DEFAULT_SCENARIOS = {
    "scenarios": [
        {
            "incident_id": "INC-20260301-003",
            "name": "Auth token validation failure spike",
            "root_cause": "Clock skew and token validation strictness change caused widespread JWT expiry rejection",
            "primary_service": "auth-service",
            "start_offset_min": 6,
            "duration_min": 12,
            "affected_services": ["auth-service", "api-gateway"],
            "error_catalog": [
                {
                    "code": "TOKEN_EXPIRED",
                    "signature": "auth.jwt: token expired in auth middleware",
                    "weight": 0.60,
                },
                {
                    "code": "UPSTREAM_TIMEOUT",
                    "signature": "api_gateway.auth: auth-service token check timed out",
                    "weight": 0.25,
                },
                {
                    "code": "CLIENT_ABORT",
                    "signature": "gateway.request: client aborted during auth flow",
                    "weight": 0.15,
                },
            ],
        },
        {
            "incident_id": "INC-20260301-001",
            "name": "Database timeout spike in order flow",
            "root_cause": "Connection pool saturation after traffic surge",
            "primary_service": "order-service",
            "start_offset_min": 22,
            "duration_min": 31,
            "affected_services": ["api-gateway", "order-service", "inventory-service"],
            "error_catalog": [
                {
                    "code": "DB_TIMEOUT",
                    "signature": "order_repo.query: timeout after 1500ms",
                    "weight": 0.62,
                },
                {
                    "code": "UPSTREAM_TIMEOUT",
                    "signature": "api_gateway.proxy: upstream order-service timed out",
                    "weight": 0.23,
                },
                {
                    "code": "CACHE_FALLBACK_FAIL",
                    "signature": "inventory_cache.get: fallback db call timeout",
                    "weight": 0.15,
                },
            ],
        },
        {
            "incident_id": "INC-20260301-002",
            "name": "Payment provider rate limit",
            "root_cause": "Burst traffic exceeded external provider quota",
            "primary_service": "payment-service",
            "start_offset_min": 74,
            "duration_min": 26,
            "affected_services": ["payment-service", "api-gateway"],
            "error_catalog": [
                {
                    "code": "PAYMENT_429",
                    "signature": "payments.client: provider responded 429",
                    "weight": 0.58,
                },
                {
                    "code": "RETRY_BUDGET_EXCEEDED",
                    "signature": "payments.retry: retry budget exhausted",
                    "weight": 0.30,
                },
                {
                    "code": "UPSTREAM_503",
                    "signature": "api_gateway.proxy: upstream payment-service unavailable",
                    "weight": 0.12,
                },
            ],
        },
        {
            "incident_id": "INC-20260301-004",
            "name": "Inventory catalog mismatch after rollout",
            "root_cause": "Catalog sync filter removed active SKUs while cache still served stale references",
            "primary_service": "inventory-service",
            "start_offset_min": 104,
            "duration_min": 12,
            "affected_services": ["inventory-service", "order-service", "api-gateway"],
            "error_catalog": [
                {
                    "code": "SKU_NOT_FOUND",
                    "signature": "inventory.repo: sku missing after catalog refresh",
                    "weight": 0.58,
                },
                {
                    "code": "CACHE_FALLBACK_FAIL",
                    "signature": "inventory_cache.get: db fallback timed out for sku lookup",
                    "weight": 0.27,
                },
                {
                    "code": "ORDER_INPUT_INVALID",
                    "signature": "order.validator: sku validation mismatch",
                    "weight": 0.15,
                },
            ],
        },
    ]
}


@dataclass
class Scenario:
    incident_id: str
    name: str
    root_cause: str
    primary_service: str
    start_offset_min: int
    duration_min: int
    error_catalog: List[Dict[str, Any]]
    affected_services: List[str]


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def weighted_choice(items: List[Tuple[Any, float]]) -> Any:
    values = [item[0] for item in items]
    weights = [item[1] for item in items]
    return random.choices(values, weights=weights, k=1)[0]


def trace_id_hex() -> str:
    return uuid4().hex


def span_id_hex() -> str:
    return uuid4().hex[:16]


def build_stack_trace(exc_type: str, message: str, stack_frames: List[str]) -> str:
    rows = [f"{exc_type}: {message}"]
    for i, frame in enumerate(stack_frames):
        source = frame.split(".")[0].replace("`1", "").replace("`3", "")
        rows.append(f"   at {frame} in /src/{source}.cs:line {40 + i * 17}")
    return "\n".join(rows)


def load_scenarios(path: Path) -> List[Scenario]:
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8"))
    else:
        raw = DEFAULT_SCENARIOS

    scenarios = []
    for row in raw["scenarios"]:
        scenarios.append(
            Scenario(
                incident_id=row["incident_id"],
                name=row["name"],
                root_cause=row["root_cause"],
                primary_service=row["primary_service"],
                start_offset_min=row["start_offset_min"],
                duration_min=row["duration_min"],
                error_catalog=row["error_catalog"],
                affected_services=row["affected_services"],
            )
        )
    return scenarios


def phase_multiplier(elapsed_s: int, duration_s: int) -> float:
    phase = elapsed_s / max(duration_s, 1)
    if phase < 0.2:
        return 0.45
    if phase < 0.5:
        return 1.0
    if phase < 0.8:
        return 1.7
    return 0.8


def choose_normal_message(service: str, method: str, route: str, status: int, duration_ms: int) -> str:
    if service == "api-gateway":
        options = [
            f"{method} {route} responded {status} in {duration_ms} ms",
            f"Request forwarded to upstream successfully route={route} status={status} latency_ms={duration_ms}",
            f"Ingress request completed route={route} status={status} total_ms={duration_ms}",
        ]
        return random.choice(options)
    if service == "order-service":
        options = [
            f"Handled {method} {route}; db_roundtrips=2 duration_ms={duration_ms}",
            f"Order query pipeline completed route={route} rows=12 duration_ms={duration_ms}",
            f"Order command executed route={route} tx_state=committed duration_ms={duration_ms}",
        ]
        return random.choice(options)
    if service == "payment-service":
        options = [
            f"Provider pre-check completed for {method} {route} status={status} duration_ms={duration_ms}",
            f"Payment intent validated route={route} provider_status=ok duration_ms={duration_ms}",
            f"Checkout authorization path healthy route={route} status={status} duration_ms={duration_ms}",
        ]
        return random.choice(options)
    if service == "inventory-service":
        options = [
            f"Inventory query served for route={route} status={status} duration_ms={duration_ms}",
            f"Stock read completed route={route} cache=hit duration_ms={duration_ms}",
            f"Inventory projection updated route={route} status={status} duration_ms={duration_ms}",
        ]
        return random.choice(options)
    options = [
        f"Request processed route={route} status={status} duration_ms={duration_ms}",
        f"Middleware pipeline completed route={route} status={status} duration_ms={duration_ms}",
    ]
    return random.choice(options)


def build_runtime_metadata() -> Dict[str, Any]:
    return {
        "cpuUsagePct": round(random.uniform(18.0, 91.0), 2),
        "memoryRssMb": random.randint(220, 1900),
        "threadCount": random.randint(18, 160),
        "gcPauseMs": round(random.uniform(0.1, 18.0), 2),
    }


def choose_warn_message(service: str, route: str) -> str:
    samples = {
        "api-gateway": f"Upstream latency above SLO for route {route}; p95>1200ms",
        "auth-service": "Token introspection latency spike detected; fallback cache used",
        "order-service": "Connection pool usage above 85%; waiting for free slot",
        "payment-service": "Provider call nearing retry threshold; attempt=2",
        "inventory-service": "Redis timeout observed; fallback to primary datastore",
    }
    return samples[service]


def make_error_payload(code: str, signature: str, service: str, trace_id: str, route: str) -> Dict[str, Any]:
    profile = ERROR_PROFILES[code]
    exception_message = random.choice(profile["message_templates"])
    stack = build_stack_trace(profile["type"], exception_message, profile["stack_frames"])
    logger = profile["logger"]
    status_code = profile["status_code"]

    if code == "DB_TIMEOUT":
        message = f"fail: {logger}[0]\n      Executing DbCommand failed ({status_code}) for trace={trace_id} route={route}"
    elif code == "PAYMENT_429":
        message = f"error: {logger}[0]\n      Provider call failed with 429 during checkout trace={trace_id}"
    elif code == "UPSTREAM_TIMEOUT":
        message = f"warn: {logger}[102]\n      Upstream timeout while proxying route={route} trace={trace_id}"
    elif code == "UPSTREAM_503":
        message = f"fail: {logger}[503]\n      Upstream payment-service unavailable route={route} trace={trace_id}"
    elif code == "RETRY_BUDGET_EXCEEDED":
        message = f"error: {logger}[14]\n      Retry policy exhausted for provider operation trace={trace_id}"
    else:
        message = f"error: {logger}[0]\n      Unhandled exception in {service} for route={route} trace={trace_id}"

    return {
        "code": code,
        "signature": signature,
        "type": profile["type"],
        "exception_message": exception_message,
        "stack_trace": stack,
        "logger": logger,
        "status_code": status_code,
        "message": message,
    }


def build_base_event(ts: datetime, service: str, level: str) -> Dict[str, Any]:
    route = random.choice(ROUTES)
    method = random.choice(METHODS if route not in ["/health", "/login"] else ["GET", "POST"])
    duration_ms = random.randint(12, 900) if level == "INFO" else random.randint(120, 2200)
    status_code = 200 if level == "INFO" else (200 if random.random() < 0.45 else 206)
    trace_id = trace_id_hex()
    span_id = span_id_hex()
    parent_span_id = span_id_hex()

    return {
        "timestamp": ts,
        "service": service,
        "region": random.choice(REGIONS),
        "env": "prod",
        "version": random.choice(VERSIONS),
        "deployment_ring": weighted_choice([("stable", 0.9), ("canary", 0.1)]),
        "cluster": "aks-prod-central",
        "pod": f"{service}-{random.choice(['7c4f7f7b9f', '66f9d78d7b', '5b4cfd8cdb'])}-{random.randint(10000, 99999)}",
        "namespace": "prod",
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "request_id": uuid4().hex[:16],
        "route": route,
        "method": method,
        "duration_ms": duration_ms,
        "status_code": status_code,
        "client_ip": f"10.{random.randint(20, 31)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "user_agent": random.choice(USER_AGENTS),
        "account_id": f"acct_{uuid4().hex[:10]}",
        "session_id": uuid4().hex[:20],
        "runtime": build_runtime_metadata(),
        "level": level,
        "message": choose_normal_message(service, method, route, status_code, duration_ms),
        "error": None,
        "incident_id": None,
    }


def to_adx_record(event: Dict[str, Any]) -> Dict[str, Any]:
    severity = {"INFO": 1, "WARN": 2, "ERROR": 3}[event["level"]]
    properties: Dict[str, Any] = {
        "env": event["env"],
        "region": event["region"],
        "cluster": event["cluster"],
        "namespace": event["namespace"],
        "serviceVersion": event["version"],
        "traceId": event["trace_id"],
        "spanId": event["span_id"],
        "requestId": event["request_id"],
        "httpMethod": event["method"],
        "httpRoute": event["route"],
        "httpStatusCode": event["status_code"],
        "durationMs": event["duration_ms"],
        "clientIp": event["client_ip"],
        "userAgent": event["user_agent"],
        "accountId": event["account_id"],
        "sessionId": event["session_id"],
        "deploymentRing": event["deployment_ring"],
        "upstreamTarget": UPSTREAM_BY_SERVICE[event["service"]],
        "cpuUsagePct": event["runtime"]["cpuUsagePct"],
        "memoryRssMb": event["runtime"]["memoryRssMb"],
        "threadCount": event["runtime"]["threadCount"],
        "gcPauseMs": event["runtime"]["gcPauseMs"],
    }
    if event["incident_id"]:
        properties["incidentId"] = event["incident_id"]
    if event["error"]:
        properties["errorCode"] = event["error"]["code"]
        properties["errorSignature"] = event["error"]["signature"]
    elif event["level"] == "WARN":
        properties["warningCategory"] = "latency_or_dependency"

    row = {
        "TimeGenerated": iso(event["timestamp"]),
        "TenantId": TENANT_ID,
        "SourceSystem": "Azure",
        "Type": "AppTraces",
        "_ResourceId": RESOURCE_ID,
        "Category": "Application",
        "Level": event["level"],
        "SeverityLevel": severity,
        "AppRoleName": event["service"],
        "AppRoleInstance": event["pod"],
        "OperationName": f"{event['method']} {event['route']}",
        "OperationId": event["trace_id"],
        "ParentId": event["parent_span_id"],
        "Message": event["message"],
        "ResultType": str(event["status_code"]),
        "DurationMs": event["duration_ms"],
        "Properties": properties,
    }
    if event["error"]:
        row["ExceptionType"] = event["error"]["type"]
        row["ExceptionMessage"] = event["error"]["exception_message"]
        row["StackTrace"] = event["error"]["stack_trace"]
    return row


def generate_adx_rows(
    scenarios: List[Scenario],
    start_time: datetime,
    duration_minutes: int,
    baseline_rps: float,
    incident_error_ratio: float,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    windows: List[Tuple[Scenario, datetime, datetime]] = []

    for scenario in scenarios:
        start_ts = start_time + timedelta(minutes=scenario.start_offset_min)
        end_ts = start_ts + timedelta(minutes=scenario.duration_min)
        windows.append((scenario, start_ts, end_ts))

    for second in range(duration_minutes * 60):
        ts = start_time + timedelta(seconds=second)
        count = max(1, int(random.gauss(baseline_rps, baseline_rps * 0.22)))

        for _ in range(count):
            service = random.choice(SERVICES)
            level = weighted_choice([("INFO", 0.90), ("WARN", 0.08), ("ERROR", 0.02)])
            event = build_base_event(ts, service, level)

            if level == "WARN":
                event["message"] = choose_warn_message(service, event["route"])

            if level == "ERROR":
                bg_code = random.choice(BACKGROUND_ERROR_CODES)
                payload = make_error_payload(
                    code=bg_code,
                    signature=ERROR_PROFILES[bg_code]["logger"],
                    service=service,
                    trace_id=event["trace_id"],
                    route=event["route"],
                )
                event["error"] = payload
                event["message"] = payload["message"]
                event["status_code"] = payload["status_code"]

            for scenario, start_ts, end_ts in windows:
                if not (start_ts <= ts <= end_ts):
                    continue
                if service not in scenario.affected_services:
                    continue

                elapsed = int((ts - start_ts).total_seconds())
                span = int((end_ts - start_ts).total_seconds())
                ratio = min(0.97, incident_error_ratio * phase_multiplier(elapsed, span))
                if random.random() < ratio:
                    err_def = weighted_choice([(item, item["weight"]) for item in scenario.error_catalog])
                    payload = make_error_payload(
                        code=err_def["code"],
                        signature=err_def["signature"],
                        service=service,
                        trace_id=event["trace_id"],
                        route=event["route"],
                    )
                    event["level"] = "ERROR"
                    event["error"] = payload
                    event["message"] = payload["message"]
                    event["status_code"] = payload["status_code"]
                    event["duration_ms"] = random.randint(700, 5000)
                    event["incident_id"] = scenario.incident_id
                break

            events.append(event)

    events.sort(key=lambda row: row["timestamp"])
    return [to_adx_record(event) for event in events]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate ADX-style production incident logs.")
    parser.add_argument(
        "--scenario-file",
        type=Path,
        default=Path("data/scenarios/default_scenarios.json"),
        help="Path to scenario JSON. Falls back to built-in defaults if missing.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/generated"),
        help="Output directory.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=7,
        help="Deterministic random seed.",
    )
    parser.add_argument(
        "--start-time",
        type=str,
        default="2026-03-01T12:00:00Z",
        help="Start time in ISO8601.",
    )
    parser.add_argument(
        "--duration-minutes",
        type=int,
        default=120,
        help="Total timeline length in minutes.",
    )
    parser.add_argument(
        "--baseline-rps",
        type=float,
        default=4.0,
        help="Average records per second.",
    )
    parser.add_argument(
        "--incident-error-ratio",
        type=float,
        default=0.32,
        help="Base chance of incident failures on affected services.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    if args.start_time.endswith("Z"):
        start_time = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    else:
        start_time = datetime.fromisoformat(args.start_time)

    scenarios = load_scenarios(args.scenario_file)
    rows = generate_adx_rows(
        scenarios=scenarios,
        start_time=start_time,
        duration_minutes=args.duration_minutes,
        baseline_rps=args.baseline_rps,
        incident_error_ratio=args.incident_error_ratio,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    adx_path = args.out_dir / "adx_logs.jsonl"
    with adx_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")

    level_counts = Counter(row["Level"] for row in rows)
    print(f"Wrote {len(rows)} ADX-style rows to {adx_path}")
    print(f"Levels: INFO={level_counts.get('INFO', 0)} WARN={level_counts.get('WARN', 0)} ERROR={level_counts.get('ERROR', 0)}")


if __name__ == "__main__":
    main()
