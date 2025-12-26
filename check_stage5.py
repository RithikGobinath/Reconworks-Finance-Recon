import sqlite3, pandas as pd

conn = sqlite3.connect("out/sqlite/reconworks.db")
b = conn.execute(
    "SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1"
).fetchone()[0]

print("latest batch:", b)

print("dim_vendor:", conn.execute("SELECT COUNT(1) FROM dim_vendor").fetchone()[0])
print("fact_transactions (latest):", conn.execute("SELECT COUNT(1) FROM fact_transactions WHERE batch_id=?", (b,)).fetchone()[0])
print("fact_vendor_payments (latest):", conn.execute("SELECT COUNT(1) FROM fact_vendor_payments WHERE batch_id=?", (b,)).fetchone()[0])

df = pd.read_sql_query(
    "SELECT txn_id, vendor_canonical, vendor_id, date, amount_cents, month, is_weekend "
    "FROM fact_transactions WHERE batch_id=? ORDER BY source_row_number",
    conn, params=(b,)
)
print("\ntransactions fact preview:")
print(df.to_string(index=False))

conn.close()
