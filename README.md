<p align="center">
  <h1 align="center">QueueCLT</h1>
  <p align="center">A lightweight, production-style background job queue system built entirely with Python</p>
  <p align="center">
    <img src="https://img.shields.io/badge/python-3.8%2B-blue" alt="Python Version">
    <img src="https://img.shields.io/badge/platform-Windows%20%7C%20macOS%20%7C%20Linux-success" alt="Platform">
    <img src="https://img.shields.io/badge/database-SQLite-lightgrey" alt="Database">
    
  </p>
</p>

---

# queuectl — CLI-Based Background Job Queue System

`queuectl` is a command-line tool that manages background jobs using worker processes.  
It supports **automatic retries**, **exponential backoff**, **timeouts**, and **scheduled jobs (`run_at`)**, all persisted in a **SQLite database**.  

It’s built entirely using Python’s standard library (no third-party dependencies), designed as a **lightweight, production-inspired system** that’s easy to learn, test, and extend.

---

## 1 Setup Instructions — How to Run Locally

### Requirements
- Python **3.8+**
- Works on **Windows, macOS, and Linux**
- No external dependencies required

### Installation Steps
```bash
# Clone this repository
git clone https://github.com/<your-username>/queuectl.git
cd queuectl

# Initialize the system
python queuectl.py init
```

**Expected Output:**
```
Database initialized at C:\Users\<you>\Desktop\queuectl\queue.db
```

That’s it! Your system is ready to enqueue and run jobs.

---

## 2️ Usage Examples — CLI Commands with Example Outputs

### Enqueue a Job
Create a job file `job1.json`:
```json
{
  "id": "job1",
  "command": "echo Hello from QueueCTL"
}
```

Then enqueue it:
```bash
python queuectl.py enqueue "@job1.json"
```

**Output:**
```
Enqueued job 'job1' (scheduled at 2025-11-09T10:00:00Z)
```

---

### Start Worker Processes
Start two background workers:
```bash
python queuectl.py worker start --count 2
```

Check status:
```bash
python queuectl.py status
```

**Example Output:**
```
=== QueueCTL Status ===
Jobs:
  pending     1
  processing  1
  completed   0
  dead        0
  dlq         0

Workers:
  w-47291  PID=12860  state=running
  w-61340  PID=13028  state=running
```

---

### List All Jobs
```bash
python queuectl.py list
```

**Example Output:**
```
job1       completed   echo Hello from QueueCTL
failjob    dead        exit 1
slowjob    dead        ping 127.0.0.1 -n 15 >nul
futurejob  pending     echo This job runs in future (run_at: 2099-01-01T00:00:00Z)
```

---

### Dead Letter Queue (DLQ)
Failed jobs after maximum retries move to DLQ.

View DLQ:
```bash
python queuectl.py dlq list
```

**Output:**
```
id        attempts   failed_at               last_error
failjob   3          2025-11-09T11:10:00Z    Exit code 1
slowjob   3          2025-11-09T11:12:00Z    Job exceeded 10s timeout
```

Retry DLQ job:
```bash
python queuectl.py dlq retry failjob
```

**Output:**
```
Re-enqueued DLQ job 'failjob'
```

---

### Manage Configuration
Show configuration:
```bash
python queuectl.py config get
```
**Output:**
```
max_retries = 3
backoff_base = 2
poll_interval_sec = 1
timeout_seconds = 10
```

Update configuration:
```bash
python queuectl.py config set timeout-seconds 15
```

Stop workers gracefully:
```bash
python queuectl.py worker stop
```

**Output:**
```
Signaled all workers to stop gracefully
```

---

## 3️ Architecture Overview — Job Lifecycle, Persistence, Worker Logic

### Core Components
| Component | Description |
|------------|-------------|
| **queuectl.py** | Main CLI tool — entry point for all commands |
| **SQLite DB (`queue.db`)** | Persistent job and config storage |
| **Worker Processes** | Poll and execute jobs asynchronously |
| **DLQ (Dead Letter Queue)** | Stores permanently failed jobs |
| **Config Manager** | Holds global retry/timeout/backoff settings |

---

### Job Lifecycle
| State | Description |
|:------|:-------------|
| `pending` | Waiting to be picked up by a worker |
| `processing` | Currently being executed |
| `completed` | Successfully finished |
| `failed` | Temporarily failed (retryable) |
| `dead` | Permanently failed and moved to DLQ |

---

### How It Works Internally
1. Jobs are stored in `queue.db` with timestamps and metadata.  
2. Worker processes poll for `pending` jobs based on `poll_interval_sec`.  
3. Each job runs in a subprocess using the system shell.  
4. Failed jobs automatically retry with **exponential backoff** (`backoff_base ^ attempt`).  
5. After `max_retries`, the job is moved to the **DLQ**.  
6. Job and worker states persist across restarts.  
7. All logic is implemented using **only Python’s standard library**.

---

## 4️ Assumptions & Trade-offs

| Category | Decision | Reason |
|-----------|-----------|--------|
| **Database** | SQLite | Lightweight, no external setup, persistent |
| **Workers** | Spawned processes | Isolated execution, mimics production |
| **Commands** | System shell subprocess | Simple and platform-independent |
| **Timeout** | Default 10s | Prevents infinite job hangs |
| **Scheduling** | `run_at` timestamps | Enables delayed job execution |
| **Dependencies** | None (pure standard library) | Simplicity, portability |
| **Trade-off** | No distributed queue (single DB) | Simplicity over scalability |

---

## 5️ Testing Instructions — How to Verify Functionality

###  Basic Flow
```bash
python queuectl.py init
python queuectl.py enqueue "@job1.json"
python queuectl.py worker start --count 2
python queuectl.py status
python queuectl.py list
```
Expected: `job1` completes successfully.

---

### Retry + DLQ Test
```bash
python queuectl.py enqueue "@failjob.json"
python queuectl.py dlq list
```
Expected: `failjob` appears in DLQ after 3 attempts.

---

### Timeout Test
```bash
python queuectl.py enqueue "@slowjob.json"
```
Expected: `slowjob` fails with “Job exceeded 10s timeout”.

---

### Scheduled Job Test
```bash
python queuectl.py enqueue "@futurejob.json"
```
Expected: `futurejob` stays pending until the scheduled time.

---

###  Config Update Test
```bash
python queuectl.py config set timeout-seconds 5
python queuectl.py config get
```

---

###  Graceful Stop Test
```bash
python queuectl.py worker stop
```
Expected: Workers stop after finishing current jobs.

---

##  Future Scope
-  Job prioritization (high / medium / low)  
-  Web dashboard for live monitoring  
-  File-based job logs (`logs/job_<id>.log`)  
-  Pause / resume jobs  
- cd path/to/queuectl
 Docker container support  

---

##  Common Mistakes to Avoid
| Mistake | Explanation |
|----------|--------------|
| Forgetting `python queuectl.py init` | Database not created |
| Missing `"id"` or `"command"` keys | Job won’t enqueue |
| Editing `queue.db` manually | May corrupt data |
| Not quoting JSON paths | PowerShell syntax issue |
| Expecting instant worker stop | They finish current jobs first |

---

## Author
**Developed by:** Varun Surya  
**Contact:** varunsurya_cherukuri@srmap.edu.in 

---
