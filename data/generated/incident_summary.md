Incident 1

Incident Summary
----------------
Service: auth-service
Symptoms: error spike (366 errors / 2513 logs) + dominant error TOKEN_EXPIRED

Likely Causes
-------------
1. Recent PR #1960 changed services/auth/middleware/jwt_validation.py (Harden JWT expiration and JWKS cache refresh).
2. Recent PR #1951 changed services/payment/provider/client.go (Tune payment provider client concurrency and retry backoff).
3. Log shows TOKEN_EXPIRED with message: error: Auth.API.Middleware.JwtValidationMiddleware[0].

Recommended Actions
-------------------
- Rollback or hotfix candidate PRs (#1960, #1951).
- Reintroduce reasonable JWT clock skew tolerance and validate token-expiry guardrails.
- Review JWKS cache TTL and refresh behavior to avoid sudden auth rejection spikes.

Evidence
--------
- Log: 2026-03-01T12:06:01Z auth-service TOKEN_EXPIRED msg="error: Auth.API.Middleware.JwtValidationMiddleware[0]"
- PR: #1960 Harden JWT expiration and JWKS cache refresh (key file services/auth/middleware/jwt_validation.py)
- PR: #1951 Tune payment provider client concurrency and retry backoff (key file services/payment/provider/client.go)

Incident 2

Incident Summary
----------------
Service: order-service
Symptoms: error spike (1441 errors / 6553 logs) + latency spike + dominant error DB_TIMEOUT

Likely Causes
-------------
1. Recent PR #1935 changed services/gateway/proxy/timeout_policy.ts (Gateway timeout budget normalization).
2. Recent PR #1942 changed services/order/repositories/order_read_repository.py (Reduce order DB command timeout and add retry policy).
3. Log shows DB_TIMEOUT with message: fail: Order.Infrastructure.Repositories.OrderReadRepository[0].

Recommended Actions
-------------------
- Rollback or hotfix candidate PRs (#1935, #1942).
- Review recent DB timeout/retry changes in order-service data access layer.
- Increase command timeout carefully and add jittered backoff on retries.

Evidence
--------
- Log: 2026-03-01T12:22:04Z inventory-service DB_TIMEOUT msg="fail: Order.Infrastructure.Repositories.OrderReadRepository[0]"
- PR: #1935 Gateway timeout budget normalization (key file services/gateway/proxy/timeout_policy.ts)
- PR: #1942 Reduce order DB command timeout and add retry policy (key file services/order/repositories/order_read_repository.py)

Incident 3

Incident Summary
----------------
Service: payment-service
Symptoms: error spike (862 errors / 5476 logs) + dominant error PAYMENT_429

Likely Causes
-------------
1. Recent PR #1935 changed services/gateway/proxy/timeout_policy.ts (Gateway timeout budget normalization).
2. Recent PR #1951 changed services/payment/provider/client.go (Tune payment provider client concurrency and retry backoff).
3. Log shows PAYMENT_429 with message: error: Payment.Infrastructure.Provider.PaymentProviderClient[0].

Recommended Actions
-------------------
- Rollback or hotfix candidate PRs (#1935, #1951).
- Throttle outbound payment requests and honor provider retry-after semantics.
- Add circuit-breaker guardrails to avoid retry storms during provider limits.

Evidence
--------
- Log: 2026-03-01T13:14:01Z api-gateway PAYMENT_429 msg="error: Payment.Infrastructure.Provider.PaymentProviderClient[0]"
- PR: #1935 Gateway timeout budget normalization (key file services/gateway/proxy/timeout_policy.ts)
- PR: #1951 Tune payment provider client concurrency and retry backoff (key file services/payment/provider/client.go)

Incident 4

Incident Summary
----------------
Service: inventory-service
Symptoms: error spike (588 errors / 2498 logs) + dominant error SKU_NOT_FOUND

Likely Causes
-------------
1. Recent PR #1964 changed services/inventory/catalog/sync.py (Catalog sync filter update for active SKU projection).
2. Recent PR #1935 changed services/gateway/proxy/timeout_policy.ts (Gateway timeout budget normalization).
3. Log shows SKU_NOT_FOUND with message: error: Inventory.Application.Query.InventoryQueryHandler[0].

Recommended Actions
-------------------
- Rollback or hotfix candidate PRs (#1964, #1935).
- Audit recent catalog sync filters and restore expected SKU inclusion criteria.
- Validate inventory projection/caching consistency after catalog rollout.

Evidence
--------
- Log: 2026-03-01T13:44:04Z inventory-service SKU_NOT_FOUND msg="error: Inventory.Application.Query.InventoryQueryHandler[0]"
- PR: #1964 Catalog sync filter update for active SKU projection (key file services/inventory/catalog/sync.py)
- PR: #1935 Gateway timeout budget normalization (key file services/gateway/proxy/timeout_policy.ts)
