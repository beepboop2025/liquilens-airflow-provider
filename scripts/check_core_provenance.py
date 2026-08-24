"""Verify pip recorded the exact LiquiLens core release wheel hash."""

from __future__ import annotations

import json
from importlib.metadata import distribution

EXPECTED = "f0162affab57307c8e20acf91dcefc33840f91e8cf9969a8d5ec8d8df860cd24"
dist = distribution("liquilens-evidence")
if dist.version != "0.14.0":
    raise SystemExit(f"unexpected liquilens-evidence version: {dist.version}")
direct_url_text = dist.read_text("direct_url.json")
if direct_url_text is None:
    raise SystemExit("liquilens-evidence has no direct_url.json provenance")
direct_url = json.loads(direct_url_text)
hash_value = direct_url.get("archive_info", {}).get("hash", "")
hashes = direct_url.get("archive_info", {}).get("hashes", {})
if hash_value != f"sha256={EXPECTED}" and hashes.get("sha256") != EXPECTED:
    raise SystemExit("liquilens-evidence wheel hash provenance differs")
print(json.dumps({"package": dist.metadata["Name"], "version": dist.version, "sha256": EXPECTED}))
