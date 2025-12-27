import os
from pathlib import Path

root = Path("out/pq_drop")
if not root.exists():
    print("out/pq_drop does not exist. Run: python -m reconworks publish-pq --config config.toml")
    raise SystemExit(1)

print("Found:", root)
for p in sorted(root.rglob("*.csv"))[:10]:
    print(" -", p)
