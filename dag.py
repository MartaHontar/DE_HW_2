from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


MYSQL_CONN_ID: str = "mysql_call_centre"

_HERE = Path(__file__).resolve().parent

TELEPHONY_JSON_DIR: str = os.getenv(
    "TELEPHONY_JSON_DIR",
    str(_HERE.parent / "telephony_json"),
)

DUCKDB_PATH: str = os.getenv(
    "DUCKDB_PATH",
    str(_HERE.parent / "include" / "call_centre.duckdb"),
)

WATERMARK_VAR: str = "call_centre_etl.last_loaded_call_time"
EPOCH_SENTINEL: str = "1970-01-01T00:00:00"

log = logging.getLogger(__name__)


def _on_failure_callback(context: dict) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log.error(
        "ALERT: Task failed | dag=%s | task=%s | run_id=%s",
        dag_id, task_id, run_id,
    )


default_args: dict = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _on_failure_callback,
}


def _get_watermark() -> datetime:
    raw: str = Variable.get(WATERMARK_VAR, default=EPOCH_SENTINEL)
    return datetime.fromisoformat(raw)


def _set_watermark(dt: datetime) -> None:
    Variable.set(WATERMARK_VAR, dt.isoformat())
    log.info("Watermark updated → %s", dt.isoformat())


def _json_path(call_id: int) -> Path:
    return Path(TELEPHONY_JSON_DIR) / f"call_{call_id:04d}.json"


def _ensure_duckdb_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS call_enriched (
            call_id           INTEGER   PRIMARY KEY,
            employee_id       INTEGER   NOT NULL,
            full_name         VARCHAR   NOT NULL,
            team              VARCHAR   NOT NULL,
            role              VARCHAR   NOT NULL,
            call_time         TIMESTAMP NOT NULL,
            direction         VARCHAR   NOT NULL,
            status            VARCHAR   NOT NULL,
            duration_sec      INTEGER,
            short_description VARCHAR,
            loaded_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)


def detect_new_calls(**context: Any) -> list[int]:
    watermark: datetime = _get_watermark()
    log.info("Current watermark: %s", watermark.isoformat())

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    rows = hook.get_records(
        """
        SELECT call_id, call_time
        FROM calls
        WHERE call_time > %s
        ORDER BY call_time ASC
        """,
        parameters=(watermark,),
    )

    if not rows:
        log.info(
            "OBSERVABILITY | detect_new_calls | new_calls=0 | watermark=%s",
            watermark.isoformat(),
        )
        context["ti"].xcom_push(key="new_call_ids", value=[])
        context["ti"].xcom_push(key="max_call_time", value=watermark.isoformat())
        return []

    new_ids: list[int] = [int(r[0]) for r in rows]
    max_call_time: datetime = max(r[1] for r in rows)
    if isinstance(max_call_time, str):
        max_call_time = datetime.fromisoformat(max_call_time)

    if len(new_ids) != len(set(new_ids)):
        duplicates = [cid for cid in new_ids if new_ids.count(cid) > 1]
        log.warning(
            "DATA QUALITY | duplicate call_ids detected: %s",
            sorted(set(duplicates)),
        )

    log.info(
        "OBSERVABILITY | detect_new_calls | new_calls=%d | id_range=%d–%d | max_call_time=%s",
        len(new_ids), min(new_ids), max(new_ids), max_call_time.isoformat(),
    )

    context["ti"].xcom_push(key="new_call_ids", value=new_ids)
    context["ti"].xcom_push(key="max_call_time", value=max_call_time.isoformat())
    return new_ids


def load_telephony_details(**context: Any) -> list[dict]:
    ti = context["ti"]
    new_call_ids: list[int] = ti.xcom_pull(
        task_ids="detect_new_calls", key="new_call_ids"
    ) or []

    if not new_call_ids:
        log.info("OBSERVABILITY | load_telephony_details | total=0")
        ti.xcom_push(key="telephony_records", value=[])
        return []

    validated: list[dict] = []
    skipped_missing = 0
    skipped_parse_err = 0
    skipped_schema = 0
    skipped_quality = 0

    for call_id in new_call_ids:
        path = _json_path(call_id)

        if not path.exists():
            skipped_missing += 1
            continue

        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload: dict = json.load(fh)
        except (json.JSONDecodeError, OSError):
            skipped_parse_err += 1
            continue

        raw_cid = payload.get("call_id")
        raw_dur = payload.get("duration_sec")
        raw_desc = str(payload.get("short_description", "")).strip()

        if raw_cid is None or raw_dur is None or not raw_desc:
            skipped_schema += 1
            continue

        try:
            file_call_id = int(raw_cid)
            duration_sec = int(raw_dur)
        except (TypeError, ValueError):
            skipped_schema += 1
            continue

        if file_call_id != call_id:
            skipped_quality += 1
            continue

        if duration_sec < 0:
            skipped_quality += 1
            continue

        validated.append({
            "call_id": call_id,
            "duration_sec": duration_sec,
            "short_description": raw_desc[:500],
        })

    log.info(
        "OBSERVABILITY | load_telephony_details | total_input=%d | valid=%d",
        len(new_call_ids), len(validated),
    )

    ti.xcom_push(key="telephony_records", value=validated)
    return validated


def transform_and_load_duckdb(**context: Any) -> int:
    ti = context["ti"]

    telephony_records: list[dict] = ti.xcom_pull(
        task_ids="load_telephony_details", key="telephony_records"
    ) or []
    max_call_time_str: str = ti.xcom_pull(
        task_ids="detect_new_calls", key="max_call_time"
    )

    if not telephony_records:
        log.info("OBSERVABILITY | transform_and_load_duckdb | records=0")
        return 0

    call_ids = [r["call_id"] for r in telephony_records]
    telephony_by_id: dict[int, dict] = {r["call_id"]: r for r in telephony_records}

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    valid_employee_ids: set[int] = {
        int(r[0]) for r in hook.get_records("SELECT employee_id FROM employees")
    }

    placeholders = ", ".join(["%s"] * len(call_ids))
    mysql_rows = hook.get_records(
        f"""
        SELECT
            c.call_id,
            c.employee_id,
            e.full_name,
            e.team,
            e.role,
            c.call_time,
            c.direction,
            c.status
        FROM calls c
        JOIN employees e ON e.employee_id = c.employee_id
        WHERE c.call_id IN ({placeholders})
        ORDER BY c.call_time ASC
        """,
        parameters=tuple(call_ids),
    )

    if not mysql_rows:
        return 0

    enriched_rows: list[tuple] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for row in mysql_rows:
        cid = int(row[0])
        employee_id = int(row[1])

        tel = telephony_by_id.get(cid)
        if tel is None:
            continue

        if employee_id not in valid_employee_ids:
            continue

        if tel["duration_sec"] < 0:
            continue

        call_time = row[5]
        if isinstance(call_time, str):
            call_time = datetime.fromisoformat(call_time)

        enriched_rows.append((
            cid,
            employee_id,
            str(row[2]),
            str(row[3]),
            str(row[4]),
            call_time,
            str(row[6]),
            str(row[7]),
            tel["duration_sec"],
            tel["short_description"],
            now,
        ))

    if not enriched_rows:
        return 0

    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(DUCKDB_PATH) as duck:
        _ensure_duckdb_schema(duck)

        duck.executemany(
            """
            INSERT OR REPLACE INTO call_enriched
                (call_id, employee_id, full_name, team, role,
                 call_time, direction, status,
                 duration_sec, short_description, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            enriched_rows,
        )

    new_watermark = datetime.fromisoformat(max_call_time_str)
    _set_watermark(new_watermark)

    return len(enriched_rows)


with DAG(
    dag_id="call_centre_hourly_etl",
    description="Hourly watermark-driven ETL: MySQL → JSON enrichment → DuckDB.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=True,
    max_active_runs=1,
    tags=["call_centre", "etl", "duckdb"],
) as dag:

    task_detect = PythonOperator(
        task_id="detect_new_calls",
        python_callable=detect_new_calls,
    )

    task_load_json = PythonOperator(
        task_id="load_telephony_details",
        python_callable=load_telephony_details,
    )

    task_duckdb = PythonOperator(
        task_id="transform_and_load_duckdb",
        python_callable=transform_and_load_duckdb,
    )

    task_detect >> task_load_json >> task_duckdb