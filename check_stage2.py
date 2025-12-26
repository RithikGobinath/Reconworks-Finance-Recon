import sqlite3

conn = sqlite3.connect("out/sqlite/reconworks.db")
b = conn.execute(
    "SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1"
).fetchone()[0]

conn.execute("DELETE FROM stg_transactions_mapped WHERE batch_id=?", (b,))
conn.execute("DELETE FROM stg_vendor_payments_mapped WHERE batch_id=?", (b,))
conn.execute("DELETE FROM mapping_runs WHERE batch_id=?", (b,))

conn.commit()
conn.close()

print("✅ Cleaned mapped rows for latest batch:", b)
