# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # PoC — Privacy gate trước Bronze
#
# Spike này chứng minh bốn invariant của D5 trong architecture brief:
#
# 1. cùng PII + cùng tenant + cùng key version → cùng token để join;
# 2. cùng PII ở tenant khác → token khác, không link chéo tenant;
# 3. rotate key → token mới có version rõ ràng;
# 4. thiếu key → fail closed, không trả payload thô.
#
# Chỉ dùng Python standard library và dữ liệu tổng hợp.

# %%
from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass


EMAIL = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
PHONE = re.compile(r"(?<!\d)(?:\+?84|0)(?:[ .-]?\d){9}(?!\d)")


@dataclass(frozen=True)
class AuditToken:
    pii_type: str
    token: str
    key_version: str


class PrivacyGateUnavailable(RuntimeError):
    """Fail-closed signal: caller must send the encrypted event to DLQ."""


class TenantTokenizer:
    def __init__(self, keys: dict[str, bytes], active_version: str) -> None:
        self._keys = keys
        self.active_version = active_version

    @staticmethod
    def _normalise(pii_type: str, value: str) -> str:
        if pii_type == "email":
            return value.strip().casefold()
        return re.sub(r"\D", "", value)

    def _token(self, tenant_id: str, pii_type: str, value: str) -> str:
        key = self._keys.get(self.active_version)
        if key is None:
            raise PrivacyGateUnavailable(
                f"KMS key {self.active_version!r} unavailable; do not write Bronze"
            )
        canonical = self._normalise(pii_type, value)
        message = f"{tenant_id}\x1f{pii_type}\x1f{canonical}".encode()
        digest = hmac.new(key, message, hashlib.sha256).hexdigest()[:24]
        return f"tok_{self.active_version}_{pii_type}_{digest}"

    def redact(self, tenant_id: str, text: str) -> tuple[str, list[AuditToken]]:
        audit: list[AuditToken] = []

        def replace(pattern: re.Pattern[str], pii_type: str, value: str) -> str:
            token = self._token(tenant_id, pii_type, value)
            audit.append(AuditToken(pii_type, token, self.active_version))
            return token

        redacted = EMAIL.sub(lambda m: replace(EMAIL, "email", m.group()), text)
        redacted = PHONE.sub(lambda m: replace(PHONE, "phone", m.group()), redacted)
        return redacted, audit


# %% [markdown]
# ## Chạy canary tổng hợp

# %%
KEYS = {
    "v1": b"synthetic-demo-key-v1-not-for-production",
    "v2": b"synthetic-demo-key-v2-not-for-production",
}
sample = "Contact alice@example.com or +84 912 345 678 about request req_42."

gate_v1 = TenantTokenizer(KEYS, "v1")
redacted_a1, audit_a1 = gate_v1.redact("tenant-A", sample)
redacted_a2, audit_a2 = gate_v1.redact("tenant-A", sample)
redacted_b, audit_b = gate_v1.redact("tenant-B", sample)

print("Source SHA-256:", hashlib.sha256(sample.encode()).hexdigest()[:16])
print("Bronze payload:", redacted_a1)
print("Audit (không chứa plaintext):", audit_a1)


# %% [markdown]
# ## Assertions — acceptance gate

# %%
assert redacted_a1 == redacted_a2, "same tenant must get deterministic tokens"
assert audit_a1 == audit_a2
assert audit_a1[0].token != audit_b[0].token, "tokens must not link across tenants"
assert EMAIL.search(redacted_a1) is None
assert PHONE.search(redacted_a1) is None
assert "alice@example.com" not in repr(audit_a1), "audit must not retain plaintext"

gate_v2 = TenantTokenizer(KEYS, "v2")
redacted_v2, audit_v2 = gate_v2.redact("tenant-A", sample)
audit_v1_token = audit_a1[0].token
assert audit_v1_token
assert audit_v1_token != audit_v2[0].token, "rotation must produce a new token"
assert "tok_v2_" in redacted_v2

unavailable = TenantTokenizer({}, "v3")
try:
    unavailable.redact("tenant-A", sample)
    raise AssertionError("privacy gate unexpectedly failed open")
except PrivacyGateUnavailable as exc:
    print("Fail-closed check:", exc)

print("\n[PASS] deterministic within tenant")
print("[PASS] unlinkable across tenants")
print("[PASS] key rotation is versioned")
print("[PASS] missing key fails closed")
