import hashlib
import re

PII_PATTERNS = [
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
]


def redact_pii(raw_text: str) -> str:
    """Cheap regex redaction. Safe to run inside the request handler."""
    cleaned = raw_text
    for pattern in PII_PATTERNS:
        cleaned = pattern.sub("[REDACTED]", cleaned)
    return cleaned


def compute_fingerprint(cleaned_text: str, blob_iterations: int = 120_000) -> str:
    """CPU-heavy hashing (~40-60ms). Must run in a PROCESS pool, never in the event loop.
    Module-level function so worker processes can import it."""
    digest = cleaned_text.encode("utf-8")
    for _ in range(blob_iterations):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


def scrub_and_fingerprint(raw_text: str, blob_iterations: int = 120_000) -> tuple[str, str]:
    cleaned = redact_pii(raw_text)
    return cleaned, compute_fingerprint(cleaned, blob_iterations)