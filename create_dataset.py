import os
import time
import threading
import traceback
from datetime import datetime

import pandas as pd
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, TableMapEvent
from pymysqlreplication.event import QueryEvent

# -----------------------
# CONFIG
# -----------------------
MYSQL = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "",  # sesuaikan
}

BINLOG_FILE = "mariadb-bin.000024"  # binlog spesifik
BINLOG_LOG_FILE = os.path.join(os.getcwd(), "binlog_logs", "binlog_10cols.csv")
os.makedirs(os.path.dirname(BINLOG_LOG_FILE), exist_ok=True)

LISTENER_COLUMNS = [
    "timestamp",
    "event_type",
    "query_type",
    "is_ddl",
    "schema_table",
    "event_size",
    "affected_rows",
    "affected_columns",
    "query_length"
]

# -----------------------
# INIT CSV
# -----------------------
def ensure_csv_with_header(path, cols):
    if not os.path.isfile(path):
        df = pd.DataFrame(columns=cols)
        df.to_csv(path, index=False)

# -----------------------
# BINLOG LISTENER
# -----------------------
def binlog_listener(stop_event):
    ensure_csv_with_header(BINLOG_LOG_FILE, LISTENER_COLUMNS)
    stream = None
    table_map = {}

    try:
        stream = BinLogStreamReader(
            connection_settings=MYSQL,
            server_id=1002,
            blocking=True,
            resume_stream=False,
            log_file=BINLOG_FILE,
            log_pos=4,
            only_events=[
                WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent,
                TableMapEvent, QueryEvent
            ]
        )
        print(f"[Listener] started — reading from: {BINLOG_FILE}")

        for binlogevent in stream:
            if stop_event.is_set():
                break

            ts_raw = getattr(binlogevent, "timestamp", None)
            event_time = datetime.fromtimestamp(ts_raw) if ts_raw else datetime.now()
            ts_str = event_time.strftime("%Y-%m-%d %H:%M:%S")
            event_size = getattr(binlogevent, "event_size", 0)

            if isinstance(binlogevent, TableMapEvent):
                schema = binlogevent.schema.decode() if isinstance(binlogevent.schema, bytes) else binlogevent.schema
                table = binlogevent.table.decode() if isinstance(binlogevent.table, bytes) else binlogevent.table
                table_map[binlogevent.table_id] = {"schema": schema, "table": table}
                continue

            if isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                tinfo = table_map.get(binlogevent.table_id, {})
                schema = tinfo.get("schema", "")
                table = tinfo.get("table", "")
                query_type = (
                    "INSERT" if isinstance(binlogevent, WriteRowsEvent)
                    else "UPDATE" if isinstance(binlogevent, UpdateRowsEvent)
                    else "DELETE"
                )

                affected_rows = len(binlogevent.rows)
                try:
                    first_row = binlogevent.rows[0]
                    vals = first_row.get("values") or first_row.get("after_values") or first_row.get("before_values") or {}
                    affected_columns = len(vals) if isinstance(vals, dict) else 0
                except Exception:
                    affected_columns = 0

                row = {
                    "timestamp": ts_str,
                    "event_type": binlogevent.__class__.__name__,
                    "query_type": query_type,
                    "is_ddl": 0,
                    "schema_table": f"{schema}.{table}" if table else schema,
                    "event_size": event_size,
                    "affected_rows": affected_rows,
                    "affected_columns": affected_columns,
                    "query_length": 0
                }

            elif isinstance(binlogevent, QueryEvent):
                schema = binlogevent.schema.decode() if isinstance(binlogevent.schema, bytes) else binlogevent.schema
                sql = binlogevent.query.decode() if isinstance(binlogevent.query, bytes) else str(binlogevent.query)
                qfirst = sql.strip().split()[0].upper() if sql.strip() else "QUERY"
                is_ddl = 1 if qfirst in ["CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"] else 0

                row = {
                    "timestamp": ts_str,
                    "event_type": "QueryEvent",
                    "query_type": qfirst,
                    "is_ddl": is_ddl,
                    "schema_table": schema,
                    "event_size": event_size,
                    "affected_rows": 0,
                    "affected_columns": 0,
                    "query_length": len(sql)
                }

            else:
                continue

            pd.DataFrame([row]).to_csv(BINLOG_LOG_FILE, mode='a', header=False, index=False)

    except Exception as e:
        print("[Listener] Error:", e)
        traceback.print_exc()
    finally:
        if stream:
            stream.close()
        print("[Listener] stopped.")

# -----------------------
# MAIN ENTRY
# -----------------------
def main():
    stop_event = threading.Event()
    t = threading.Thread(target=binlog_listener, args=(stop_event,), daemon=True)
    t.start()

    print("Binlog listener running. Press Ctrl+C to stop.")
    try:
        while t.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping listener...")
        stop_event.set()
        t.join(timeout=5)
        print("Exited cleanly.")

if __name__ == "__main__":
    main()

