import sqlite3
import pandas as pd

conn = sqlite3.connect("out/sqlite/reconworks.db")
b = conn.execute(
    "SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1"
).fetchone()[0]

print("latest batch:", b)
print("qa_flags (latest):", conn.execute("SELECT COUNT(1) FROM qa_flags WHERE batch_id=?", (b,)).fetchone()[0])

df = pd.read_sql_query(
    "SELECT severity, flag_code, record_type, vendor_canonical, date, amount_cents, message "
    "FROM qa_flags WHERE batch_id=? ORDER BY severity, flag_code",
    conn,
    params=(b,)
)

print("\nflags preview:")
print(df.to_string(index=False) if not df.empty else "(no flags)")

conn.close()
