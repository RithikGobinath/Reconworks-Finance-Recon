import sqlite3, pandas as pd

conn = sqlite3.connect("out/sqlite/reconworks.db")
b = conn.execute(
    "SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1"
).fetchone()[0]

df = pd.read_sql_query(
    "SELECT vendor_raw, vendor_clean, vendor_canonical, vendor_norm_method, vendor_norm_confidence "
    "FROM norm_transactions WHERE batch_id=? ORDER BY source_row_number",
    conn,
    params=(b,)
)

print("latest batch:", b)
print(df.to_string(index=False))
conn.close()
