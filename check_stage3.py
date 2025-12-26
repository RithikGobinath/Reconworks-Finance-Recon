import sqlite3
import pandas as pd

conn = sqlite3.connect("out/sqlite/reconworks.db")

tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]
print("tables:", tables)

b = conn.execute(
    "SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1"
).fetchone()[0]
print("latest batch:", b)

def count(tbl):
    return conn.execute(f"SELECT COUNT(1) FROM {tbl} WHERE batch_id=?", (b,)).fetchone()[0]

print("raw tx:", count("stg_transactions_raw"))
print("mapped tx:", count("stg_transactions_mapped"))
print("clean tx:", count("clean_transactions"))

print("\nclean preview:")
df = pd.read_sql_query(
    "SELECT vendor_raw, date, amount_cents, clean_status, clean_notes "
    "FROM clean_transactions WHERE batch_id=? ORDER BY source_row_number",
    conn,
    params=(b,)
)
print(df.to_string(index=False))

conn.close()
