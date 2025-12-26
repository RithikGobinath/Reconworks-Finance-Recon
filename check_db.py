import sqlite3

conn = sqlite3.connect("out/sqlite/reconworks.db")

tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]

print("tables:", tables)
print("stg_transactions_raw:", conn.execute("SELECT COUNT(*) FROM stg_transactions_raw").fetchone()[0])
print("stg_vendor_payments_raw:", conn.execute("SELECT COUNT(*) FROM stg_vendor_payments_raw").fetchone()[0])
print("ingest_files:", conn.execute("SELECT COUNT(*) FROM ingest_files").fetchone()[0])

conn.close()
