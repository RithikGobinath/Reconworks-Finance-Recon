from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None

def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]

def add_columns_text(conn: sqlite3.Connection, table: str, cols: Iterable[str]) -> None:
    existing = set(get_columns(conn, table))
    for c in cols:
        if c not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT;')
    conn.commit()

def create_ingest_files_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_files (
            batch_id TEXT,
            source_name TEXT,
            source_file TEXT,
            file_modified_at TEXT,
            file_size_bytes INTEGER,
            row_count INTEGER,
            original_columns_json TEXT,
            sanitized_columns_json TEXT,
            ingested_at_utc TEXT
        );
        """
    )
    conn.commit()

def insert_ingest_file(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO ingest_files ({cols}) VALUES ({placeholders});", values)
    conn.commit()


def create_mapping_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mapping_runs (
            mapped_at_utc TEXT,
            batch_id TEXT,
            source_name TEXT,
            input_table TEXT,
            output_table TEXT,
            vendor_col TEXT,
            date_col TEXT,
            amount_col TEXT,
            row_count INTEGER,
            notes TEXT
        );
        """
    )
    conn.commit()

def insert_mapping_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO mapping_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()



def create_cleaning_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cleaning_runs (
            cleaned_at_utc TEXT,
            batch_id TEXT,
            source_name TEXT,
            input_table TEXT,
            output_table TEXT,
            row_count INTEGER,
            error_count INTEGER
        );
        """
    )
    conn.commit()

def insert_cleaning_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO cleaning_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()



def create_normalization_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS normalization_runs ("
        " normalized_at_utc TEXT,"
        " batch_id TEXT,"
        " source_name TEXT,"
        " input_table TEXT,"
        " output_table TEXT,"
        " alias_file TEXT,"
        " row_count INTEGER,"
        " alias_match_count INTEGER,"
        " no_match_count INTEGER"
        ");"
    )
    conn.commit()

def insert_normalization_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO normalization_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()

def latest_batch_id(conn: sqlite3.Connection) -> Optional[str]:
    cur = conn.execute("SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1;")
    row = cur.fetchone()
    return row[0] if row else None


def create_modeling_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS modeling_runs ("
        " modeled_at_utc TEXT,"
        " batch_id TEXT,"
        " source_name TEXT,"
        " input_table TEXT,"
        " output_table TEXT,"
        " row_count INTEGER,"
        " distinct_vendor_count INTEGER"
        ");"
    )
    conn.commit()

def insert_modeling_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO modeling_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()


def create_qa_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS qa_runs (
            batch_id TEXT,
            qa_at_utc TEXT,
            policy_rules_path TEXT
        );
        """
    )
    conn.commit()

def create_qa_flags_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS qa_flags (
            batch_id TEXT,
            record_type TEXT,
            record_id TEXT,
            flag_code TEXT,
            severity TEXT,
            message TEXT,
            vendor_canonical TEXT,
            vendor_id TEXT,
            date TEXT,
            amount_cents INTEGER,
            source_file TEXT,
            source_row_number INTEGER,
            row_hash TEXT,
            created_at_utc TEXT
        );
        """
    )
    conn.commit()

def delete_where_batch(conn: sqlite3.Connection, table: str, batch_id: str) -> None:
    conn.execute(f"DELETE FROM {table} WHERE batch_id=?", (batch_id,))
    conn.commit()


def create_matching_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS matching_runs ("
        " matched_at_utc TEXT,"
        " batch_id TEXT,"
        " date_window_days INTEGER,"
        " amount_tolerance_cents INTEGER,"
        " min_score REAL,"
        " match_count INTEGER,"
        " unmatched_tx_count INTEGER,"
        " unmatched_pay_count INTEGER"
        ");"
    )
    conn.commit()

def create_match_candidates_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS match_candidates ("
        " batch_id TEXT,"
        " txn_id TEXT,"
        " pay_id TEXT,"
        " vendor_sim REAL,"
        " date_diff_days INTEGER,"
        " amount_diff_cents INTEGER,"
        " score REAL"
        ");"
    )
    conn.commit()

def create_matches_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS matches ("
        " batch_id TEXT,"
        " txn_id TEXT,"
        " pay_id TEXT,"
        " match_score REAL,"
        " match_type TEXT,"
        " vendor_sim REAL,"
        " date_diff_days INTEGER,"
        " amount_diff_cents INTEGER,"
        " matched_at_utc TEXT"
        ");"
    )
    conn.commit()

def insert_matching_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO matching_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()

def create_exceptions_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS exception_runs ("
        " created_at_utc TEXT,"
        " batch_id TEXT,"
        " exception_count INTEGER"
        ");"
    )
    conn.commit()

def create_exceptions_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS exceptions ("
        " batch_id TEXT,"
        " exception_id TEXT,"
        " record_type TEXT,"
        " record_id TEXT,"
        " related_record_id TEXT,"
        " exception_code TEXT,"
        " severity TEXT,"
        " message TEXT,"
        " recommended_action TEXT,"
        " vendor_canonical TEXT,"
        " vendor_id TEXT,"
        " date TEXT,"
        " amount_cents INTEGER,"
        " created_at_utc TEXT"
        ");"
    )
    conn.commit()

def insert_exception_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO exception_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()

def create_reporting_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS report_runs ("
        " created_at_utc TEXT,"
        " batch_id TEXT"
        ");"
    )
    conn.commit()

def insert_report_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO report_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()

def create_excel_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS excel_runs ("
        " created_at_utc TEXT,"
        " batch_id TEXT,"
        " output_path TEXT"
        ");"
    )
    conn.commit()

def insert_excel_run(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    keys = list(row.keys())
    placeholders = ",".join(["?"] * len(keys))
    cols = ",".join([f'"{k}"' for k in keys])
    values = [row[k] for k in keys]
    conn.execute(f"INSERT INTO excel_runs ({cols}) VALUES ({placeholders});", values)
    conn.commit()
