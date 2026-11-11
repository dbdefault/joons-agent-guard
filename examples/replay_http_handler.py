"""A minimal POST /payments handler with idempotency via ReplayGuard.

What this shows
---------------
* request_digest() fingerprints (method, path, body) so retries carrying the
  same Idempotency-Key and the same body are recognized as the same request.
* claim()/complete(): a retried request returns the stored response;
  the same key with a different body returns 409 (conflict);
  a request claimed but never completed (simulated crash) stays 'pending' and
  its retries return 409 'in progress'.
* No real server is needed: the handler function is called directly with
  (method, path, headers, body), like a web framework would.

The DB lives in a tempfile.TemporaryDirectory; the script asserts the expected
statuses and exits 0.

Run:  PYTHONPATH=src python examples/replay_http_handler.py
"""
import json
import os
import tempfile

from joons_agent_guard import ReplayGuard, request_digest


def handle_payment(guard, method, path, headers, body, crash_before_complete=False):
    """Fake HTTP handler: returns (status_code, json-serializable body)."""
    # principal comes from a trusted transport layer, never from client input.
    principal = headers["X-Principal"]
    request_id = headers["Idempotency-Key"]
    digest = request_digest(method, path, body)

    outcome = guard.claim(principal, request_id, digest)

    if outcome == "conflict":
        return 409, {"error": "idempotency_key_reused_with_different_body"}
    if outcome == "pending":
        return 409, {"error": "request_in_progress"}
    if outcome == "completed":
        code, stored = guard.response(principal, request_id, digest)
        return code, {**stored, "replayed": True}

    # outcome == "new": this caller runs the effect exactly once.
    if crash_before_complete:
        raise RuntimeError("worker crashed between claim and complete")
    amount = json.loads(body.decode("utf-8"))["amount_cents"]
    code, response = 201, {"status": "created", "payment_id": "pay-" + digest[:12],
                           "amount_cents": amount}
    if not guard.complete(principal, request_id, digest, code, response):
        # Lost a race with another worker that completed the same request.
        code, stored = guard.response(principal, request_id, digest)
        return code, {**stored, "replayed": True}
    return code, response


def send(guard, label, body, key, crash=False):
    headers = {"X-Principal": "service-checkout", "Idempotency-Key": key}
    print(f"{label}: POST /payments  Idempotency-Key={key}  body={body.decode()}")
    if crash:
        try:
            handle_payment(guard, "POST", "/payments", headers, body, crash_before_complete=True)
        except RuntimeError as exc:
            print(f"  -> worker died: {exc} (request left 'pending')")
        return
    status, payload = handle_payment(guard, "POST", "/payments", headers, body)
    print(f"  -> {status} {json.dumps(payload, sort_keys=True)}")
    return status, payload


def main():
    with tempfile.TemporaryDirectory() as tmp:
        guard = ReplayGuard(os.path.join(tmp, "guard.sqlite3"))
        try:
            body_a = json.dumps({"amount_cents": 1500}).encode("utf-8")
            body_a_retry = json.dumps({"amount_cents": 1500}).encode("utf-8")  # same content, new bytes
            body_b = json.dumps({"amount_cents": 9999}).encode("utf-8")

            # 1) first request: new -> 201 created
            status, payload = send(guard, "1) first request           ", body_a, "idem-001")
            assert status == 201 and payload["status"] == "created"

            # 2) retried request, same key and same body -> stored response
            status, payload = send(guard, "2) retry (same key+body)   ", body_a_retry, "idem-001")
            assert status == 201 and payload["replayed"] and payload["payment_id"] == "pay-" + request_digest("POST", "/payments", body_a)[:12]

            # 3) same key, different body -> 409 conflict
            status, payload = send(guard, "3) same key, other body    ", body_b, "idem-001")
            assert status == 409 and payload["error"] == "idempotency_key_reused_with_different_body"

            # 4) second key, but the worker crashes after claim() -> stays pending
            send(guard, "4) crash after claim       ", body_a, "idem-002", crash=True)

            # 5) retry of the pending request -> 409 in progress
            status, payload = send(guard, "5) retry of pending request", body_a, "idem-002")
            assert status == 409 and payload["error"] == "request_in_progress"
        finally:
            guard.close()
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
