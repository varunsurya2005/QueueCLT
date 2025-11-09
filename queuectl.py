#!/usr/bin/env python3
import argparse
import sqlite3
import os
import json
import time
import subprocess
import random
import sys
from datetime import datetime, timezone
from multiprocessing import Process

DB_PATH = "queue.db"

# ----------------------- Helpers -----------------------
def utcnow_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def new_worker_id():
    return f"w-{random.randint(10000,99999)}"

# ----------------------- DB Init -----------------------
def init_db():
    conn = connect()
    with conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs(
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            next_run_at TEXT NOT NULL,
            last_error TEXT,
            worker_id TEXT
        );

        CREATE TABLE IF NOT EXISTS dlq(
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            failed_at TEXT NOT NULL,
            last_error TEXT
        );

        CREATE TABLE IF NOT EXISTS config(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workers(
            id TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            last_heartbeat TEXT NOT NULL,
            desired_state TEXT NOT NULL
        );
        """)
        conn.execute("INSERT OR IGNORE INTO config VALUES('max_retries','3')")
        conn.execute("INSERT OR IGNORE INTO config VALUES('backoff_base','2')")
        conn.execute("INSERT OR IGNORE INTO config VALUES('poll_interval_sec','1')")
        conn.execute("INSERT OR IGNORE INTO config VALUES('timeout_seconds','10')")
    print(f"Database initialized at {os.path.abspath(DB_PATH)}")

# ----------------------- Config -----------------------
def get_config(conn, key, default=None):
    row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default

def set_config(key, value):
    conn = connect()
    with conn:
        conn.execute(
            "INSERT INTO config(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
    conn.close()

# ----------------------- Enqueue -----------------------
def enqueue_job(json_or_file):
    if json_or_file.startswith("@"):
        with open(json_or_file[1:], "r", encoding="utf-8") as f:
            payload = json.load(f)
    else:
        payload = json.loads(json_or_file)

    if "id" not in payload or "command" not in payload:
        raise SystemExit("Error: Job must include 'id' and 'command'.")

    now = utcnow_iso()
    run_at = payload.get("run_at", now)  # new feature: scheduling

    record = {
        "id": payload["id"],
        "command": payload["command"],
        "state": "pending",
        "attempts": 0,
        "max_retries": payload.get("max_retries", 3),
        "created_at": now,
        "updated_at": now,
        "next_run_at": run_at,
        "last_error": None,
        "worker_id": None,
    }

    conn = connect()
    try:
        with conn:
            conn.execute("""INSERT INTO jobs
                (id,command,state,attempts,max_retries,created_at,updated_at,next_run_at,last_error,worker_id)
                VALUES(:id,:command,:state,:attempts,:max_retries,:created_at,:updated_at,:next_run_at,:last_error,:worker_id)
            """, record)
        print(f"Enqueued job '{record['id']}' (scheduled at {run_at})")
    except sqlite3.IntegrityError:
        print(f"Job '{record['id']}' already exists.")
    finally:
        conn.close()

# ----------------------- Worker Management -----------------------
def worker_start(count=1):
    """Start N detached background workers that won't block the terminal."""
    for _ in range(count):
        cmd = [sys.executable, __file__, "worker", "_run"]
        subprocess.Popen(cmd, creationflags=subprocess.DETACHED_PROCESS if os.name == "nt" else 0)
    print(f"Started {count} background worker(s)")

def worker_stop():
    conn = connect()
    with conn:
        conn.execute("UPDATE workers SET desired_state='stopping' WHERE desired_state='running'")
    conn.close()
    print("Signaled all workers to stop gracefully")

def worker_main():
    wid = new_worker_id()
    pid = os.getpid()
    conn = connect()
    with conn:
        conn.execute("""
            INSERT OR REPLACE INTO workers(id,pid,started_at,last_heartbeat,desired_state)
            VALUES(?,?,?,?,?)
        """, (wid, pid, utcnow_iso(), utcnow_iso(), "running"))
    conn.close()

    print(f"Worker {wid} (PID {pid}) started")
    try:
        while True:
            conn = connect()
            desired = conn.execute("SELECT desired_state FROM workers WHERE id=?", (wid,)).fetchone()
            if not desired or desired["desired_state"] == "stopping":
                print(f"Worker {wid} stopping gracefully...")
                conn.execute("DELETE FROM workers WHERE id=?", (wid,))
                conn.close()
                break

            conn.execute("UPDATE workers SET last_heartbeat=? WHERE id=?", (utcnow_iso(), wid))
            conn.close()

            try_process_one(wid)
            time.sleep(float(get_config(connect(), "poll_interval_sec", 1)))
    except KeyboardInterrupt:
        conn = connect()
        conn.execute("DELETE FROM workers WHERE id=?", (wid,))
        conn.close()
        print(f"Worker {wid} interrupted manually")

def try_process_one(worker_id):
    conn = connect()
    job = None
    now = utcnow_iso()
    with conn:
        row = conn.execute("""
            SELECT id FROM jobs WHERE state='pending' AND next_run_at<=? ORDER BY created_at LIMIT 1
        """, (now,)).fetchone()
        if row:
            j_id = row["id"]
            updated = conn.execute("""
                UPDATE jobs SET state='processing', worker_id=?, updated_at=? WHERE id=? AND state='pending'
            """, (worker_id, now, j_id))
            if updated.rowcount == 1:
                job = conn.execute("SELECT * FROM jobs WHERE id=?", (j_id,)).fetchone()

    if not job:
        conn.close()
        return False

    print(f"Processing job {job['id']}: {job['command']}")
    timeout_seconds = int(get_config(connect(), "timeout_seconds", 10))  # new config
    success, output = run_command(job["command"], timeout_seconds)
    with conn:
        if success:
            conn.execute("UPDATE jobs SET state='completed', updated_at=?, last_error=NULL WHERE id=?",
                         (utcnow_iso(), job["id"]))
            print(f"Job {job['id']} completed successfully")
        else:
            attempts = job["attempts"] + 1
            max_retries = job["max_retries"]
            base = int(get_config(conn, "backoff_base", 2))
            if attempts < max_retries:
                delay = base ** attempts
                next_run = datetime.now(timezone.utc).timestamp() + delay
                next_run_iso = datetime.fromtimestamp(next_run, timezone.utc).isoformat().replace("+00:00", "Z")
                conn.execute("""
                    UPDATE jobs SET state='pending', attempts=?, next_run_at=?, updated_at=?, last_error=? WHERE id=?
                """, (attempts, next_run_iso, utcnow_iso(), output, job["id"]))
                print(f"Retrying job {job['id']} in {delay} seconds")
            else:
                conn.execute("""
                    INSERT OR REPLACE INTO dlq(id,command,attempts,max_retries,failed_at,last_error)
                    VALUES (?,?,?,?,?,?)
                """, (job["id"], job["command"], attempts, max_retries, utcnow_iso(), output))
                conn.execute("""
                    UPDATE jobs SET state='dead', attempts=?, updated_at=?, last_error=? WHERE id=?
                """, (attempts, utcnow_iso(), output, job["id"]))
                print(f"Job {job['id']} moved to DLQ after {attempts} attempts ({output})")
    conn.close()
    return True

def run_command(cmd, timeout_seconds=10):
    """Execute a command with timeout support."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout_seconds)
        if result.returncode == 0:
            return True, result.stdout.strip() or "OK"
        return False, result.stderr.strip() or f"Exit code {result.returncode}"
    except subprocess.TimeoutExpired:
        return False, f"Job exceeded {timeout_seconds}s timeout"
    except Exception as e:
        return False, str(e)

# ----------------------- DLQ -----------------------
def dlq_list():
    conn = connect()
    rows = conn.execute("SELECT * FROM dlq ORDER BY failed_at DESC").fetchall()
    if not rows:
        print("Dead Letter Queue is empty")
    else:
        print("id\tattempts\tfailed_at\tlast_error")
        for r in rows:
            print(f"{r['id']}\t{r['attempts']}\t{r['failed_at']}\t{r['last_error'][:50]}")
    conn.close()

def dlq_retry(job_id):
    conn = connect()
    with conn:
        row = conn.execute("SELECT * FROM dlq WHERE id=?", (job_id,)).fetchone()
        if not row:
            print(f"DLQ job '{job_id}' not found.")
            return
        now = utcnow_iso()
        conn.execute("""
            INSERT OR REPLACE INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at,next_run_at,last_error,worker_id)
            VALUES(?,?,?,?,?,?,?,?,?,NULL)
        """, (row["id"], row["command"], "pending", 0, row["max_retries"], now, now, now, None))
        conn.execute("DELETE FROM dlq WHERE id=?", (job_id,))
    conn.close()
    print(f"Re-enqueued DLQ job '{job_id}'")

# ----------------------- Status -----------------------
def status():
    conn = connect()
    print("=== QueueCTL Status ===")
    print(f"Database: {os.path.abspath(DB_PATH)}\n")
    counts = dict(conn.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state").fetchall())
    dlq_count = conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
    print("Jobs:")
    for s in ["pending", "processing", "completed", "dead"]:
        print(f"  {s:<10} {counts.get(s, 0)}")
    print(f"  dlq        {dlq_count}\n")
    print("Workers:")
    workers = conn.execute("SELECT * FROM workers").fetchall()
    if not workers:
        print("  (no active workers)")
    else:
        for w in workers:
            print(f"  {w['id']}\tPID={w['pid']}\tstate={w['desired_state']}")
    conn.close()

# ----------------------- CLI -----------------------
def main():
    parser = argparse.ArgumentParser(prog="queuectl", description="CLI Job Queue System")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("init", help="Initialize the database")
    p_enq = sub.add_parser("enqueue", help="Add a job")
    p_enq.add_argument("payload", help="JSON or @file.json")

    p_worker = sub.add_parser("worker", help="Manage workers")
    wsub = p_worker.add_subparsers(dest="wcmd")
    p_start = wsub.add_parser("start", help="Start workers")
    p_start.add_argument("--count", type=int, default=1)
    wsub.add_parser("stop", help="Stop workers gracefully")
    wsub.add_parser("_run", help=argparse.SUPPRESS)

    sub.add_parser("list", help="List jobs")
    sub.add_parser("status", help="Show system status")

    p_dlq = sub.add_parser("dlq", help="Manage Dead Letter Queue")
    dlqsub = p_dlq.add_subparsers(dest="dlq_cmd")
    dlqsub.add_parser("list", help="List DLQ jobs")
    p_retry = dlqsub.add_parser("retry", help="Retry a DLQ job")
    p_retry.add_argument("job_id")

    p_cfg = sub.add_parser("config", help="Manage configuration")
    cfgsub = p_cfg.add_subparsers(dest="cfg_cmd")
    cfgsub.add_parser("get", help="Show all config values")
    p_set = cfgsub.add_parser("set", help="Set a config key")
    p_set.add_argument("key", choices=["max-retries","backoff-base","poll-interval-sec","timeout-seconds"])
    p_set.add_argument("value")

    args = parser.parse_args()
    if args.cmd == "init": init_db()
    elif args.cmd == "enqueue": enqueue_job(args.payload)
    elif args.cmd == "worker":
        if args.wcmd == "start": worker_start(args.count)
        elif args.wcmd == "stop": worker_stop()
        elif args.wcmd == "_run": worker_main()
        else: print("Usage: queuectl worker [start|stop]")
    elif args.cmd == "dlq":
        if args.dlq_cmd == "list": dlq_list()
        elif args.dlq_cmd == "retry": dlq_retry(args.job_id)
        else: print("Usage: queuectl dlq [list|retry]")
    elif args.cmd == "config":
        if args.cfg_cmd == "get":
            conn = connect()
            rows = conn.execute("SELECT * FROM config").fetchall()
            for r in rows: print(f"{r['key']} = {r['value']}")
            conn.close()
        elif args.cfg_cmd == "set":
            mapping = {
                "max-retries":"max_retries",
                "backoff-base":"backoff_base",
                "poll-interval-sec":"poll_interval_sec",
                "timeout-seconds":"timeout_seconds"
            }
            set_config(mapping[args.key], args.value)
            print(f"Set {args.key} = {args.value}")
        else:
            print("Usage: queuectl config [get|set]")
    elif args.cmd == "list":
        conn = connect()
        rows = conn.execute("SELECT id,command,state,attempts,next_run_at FROM jobs ORDER BY created_at").fetchall()
        if not rows: print("No jobs found")
        else:
            for r in rows:
                print(f"{r['id']}\t{r['state']}\t{r['command']}\t(run_at: {r['next_run_at']})")
        conn.close()
    elif args.cmd == "status": status()
    else: parser.print_help()

if __name__ == "__main__":
    main()
