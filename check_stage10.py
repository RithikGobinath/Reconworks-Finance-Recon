import sqlite3
import pandas as pd

conn = sqlite3.connect("out/sqlite/reconworks.db")
b = conn.execute("SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1").fetchone()[0]
print("latest batch:", b)

tables = [r[0] for r in conn.execute("select name from sqlite_master where type='table' order by name").fetchall()]
print("tables:", tables)

def cnt(t):
    try:
        return conn.execute(f"SELECT COUNT(1) FROM {t} WHERE batch_id=?", (b,)).fetchone()[0]
    except Exception:
        return None

print("\ncounts:")
for t in ["qa_flags","matches","exceptions","rpt_spend_by_month_vendor","rpt_match_rate_by_month","rpt_exceptions_by_code","rpt_top_vendors"]:
    print(f"  {t}: {cnt(t)}")

print("\nmatch preview:")
df = pd.read_sql_query("SELECT txn_id, pay_id, match_score, match_type FROM matches WHERE batch_id=? ORDER BY match_score DESC", conn, params=(b,))
print(df.to_string(index=False))

print("\nexceptions preview:")
df = pd.read_sql_query("SELECT severity, exception_code, record_type, record_id, message FROM exceptions WHERE batch_id=? ORDER BY severity, exception_code", conn, params=(b,))
print(df.to_string(index=False) if not df.empty else "(none)")

conn.close()
