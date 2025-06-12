12 June 2025

Critical Audit of Receiver Service
Edge Cases & Single Points of Failure
Secret Dependency Failure

If secrets (deribit_client_id/secret) are missing at runtime, the service exits immediately

Fix: Implement fallback to environment variables as secondary source

Urgency: High (production blocker)

WebSocket Reconnection Storm

Exponential backoff caps at 300s (5min) - insufficient for extended exchange outages

Fix: Add jitter and increase max delay to 3600s

Urgency: Medium

Configuration File Dependency

Hardcoded path /app/src/shared/config/strategies.toml causes failure if FS layout changes

Fix: Make path fully configurable via env var

Urgency: Medium

Performance Bottlenecks
Double Message Serialization

Incoming WS messages are deserialized with orjson.loads() then immediately reserialized

Impact: 40% CPU overhead in benchmarks with high message volume

Fix: Stream raw message bytes directly to Redis

python
# deribit_ws.py:298
batch.append({
    "raw": message  # Use raw bytes instead of re-serializing
})
Blocking Event Loop

time.time() calls in message processing path block event loop

Fix: Use loop.time() for monotonic timestamps

Urgency: High (causes message latency spikes)

Security Vulnerabilities
Secret Handling in Logs

Potential leakage through error messages (e.g., log.error(f"Authentication failed: {error}"))

Fix: Add redaction filter to logger configuration

Urgency: Critical

Unencrypted Secrets

Secrets transmitted via WebSocket in plaintext (Deribit requires HTTPS)

Verification: Confirm wss:// is used (confirmed in AddressUrl.DERIBIT_WS)

Robustness & Scalability
No Horizontal Scaling Support

Singleton Redis client prevents multiple receiver instances

Fix: Implement consumer group partitioning

Timeline: Phase 2 (when adding Binance support)

Memory Growth Risk

Batch list (batch = []) never cleared on Redis failures

Fix: Add circuit breaker pattern

python
if len(batch) >= BATCH_SIZE * 2:  # Prevent unbounded growth
    batch = batch[-BATCH_SIZE:] 
Critical Improvements Needed
Heartbeat Validation

No verification of Deribit heartbeat responses

Risk: Silent connection degradation

Fix: Add response validation in heartbeat_response()

python
if response.get("result") != "ok":
    raise ConnectionError("Invalid heartbeat response")
Configuration Hot-Reload

Strategies config loaded once at startup - requires restart to update

Fix: Add filesystem watcher with SIGHUP reload

Telemetry Gaps

Missing metrics: messages/sec, batch efficiency, reconnect count

Fix: Integrate Prometheus exporter

Before Full Deployment
Load Testing

Simulate 10k msg/sec burst using historical data replay

Monitor: Redis memory, Python GC pauses, network saturation

Failover Validation

Test scenarios:

Redis restart during message batching

Deribit API maintenance (30min+ downtime)

Invalid secret rotation