# 🤖 Airflow AI Agent
### Intelligent Apache Airflow Workflow Supervisor

An AI agent that autonomously monitors Airflow DAG failures, classifies root causes,
checks dependencies, monitors system resources, and triggers intelligent retries —
all powered by a local LLM (Ollama) with full rule-based fallback.

---

## 🧠 How the Agent Works

```
Airflow DAG Fails at 2:00 AM
         ↓
[1] Log Monitor detects failure (file scan or REST API poll)
         ↓
[2] Error Classifier (LLM or rules) identifies root cause:
    • API_FAILURE        → external API down
    • DATA_NOT_AVAILABLE → stored proc returned 0 rows
    • FILE_NOT_AVAILABLE → required input file missing
    • TIMEOUT            → DB/network under high load
    • INFRA              → worker/server issue
         ↓
[3] Dependency Checker polls the specific dependency:
    • API_FAILURE        → polls API endpoint every 60s
    • DATA_NOT_AVAILABLE → runs stored proc check every 2 min
    • FILE_NOT_AVAILABLE → watches file path every 60s
    • TIMEOUT/INFRA      → monitors CPU/memory every 30s
         ↓
[4] Resource Monitor checks system health:
    • CPU < 80% AND Memory < 85% → safe to retry
         ↓
[5] Retry Engine triggers DAG:
    • Clears failed tasks via Airflow REST API
    • OR triggers a fresh DAG run
         ↓
[6] Learning System updates historical patterns:
    • Records avg wait time per DAG + error type
    • Uses this to optimize future wait intervals
```

---

## 📁 Project Structure

```
airflow_ai_agent/
├── main.py                          # FastAPI backend (14 endpoints)
├── dashboard/
│   └── app.py                       # Streamlit dashboard (9 pages)
├── agent/
│   ├── supervisor.py                # Main orchestration engine
│   ├── log_monitor.py               # Watches Airflow logs + REST API
│   ├── error_classifier.py          # LLM + rule-based error classification
│   ├── dependency_checker.py        # API / DB / file dependency probing
│   ├── resource_monitor.py          # CPU / memory / DB health
│   └── airflow_client.py            # Airflow REST API client
├── db/
│   └── models.py                    # SQLite DB (6 tables + demo data)
├── core/
│   └── llm.py                       # Ollama LLM factory
├── config/
│   └── settings.py                  # App configuration
├── .env                             # Environment variables
└── requirements.txt
```

---

## ✅ Prerequisites

### 1. Python 3.11
Download: https://www.python.org/downloads/
✅ Check **"Add Python to PATH"** during install

```powershell
python --version  # Must show 3.11.x
```

### 2. Git (Optional)
Download: https://git-scm.com/download/win

### 3. Ollama (Optional — but recommended for AI mode)
Download: https://ollama.com/download/windows

> The agent runs **without Ollama** using rule-based classification.
> Install Ollama for deeper, more accurate root cause analysis.

---

## 🚀 Installation

### Step 1: Extract and enter the folder
```powershell
cd C:\path\to\airflow_ai_agent
```

### Step 2: Create virtual environment
```powershell
python -m venv .venv
```

### Step 3: Activate it
```powershell
.venv\Scripts\Activate.ps1

# If you get an ExecutionPolicy error:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
# Then try again
```

### Step 4: Install packages
```powershell
pip install -r requirements.txt
# Takes 3-7 minutes
```

### Step 5: (Optional) Set up Ollama for AI mode
```powershell
# After installing Ollama from ollama.com:
ollama pull llama3.1:8b-instruct-q4_K_M   # 4.7 GB — one time download
# OR faster/smaller model:
ollama pull phi3:mini                       # 2.3 GB
```

---

## ▶️ Running the Application

### You need 2 terminal windows:

**Terminal 1 — Start the AI Agent (FastAPI)**
```powershell
cd C:\path\to\airflow_ai_agent
.venv\Scripts\Activate.ps1
uvicorn main:app --reload --port 8000
```
Expected output:
```
🚀 Starting Airflow AI Agent...
✅ Database ready
✅ Demo incidents seeded
🚀 Supervisor started in background thread
🔄 Monitor loop started — polling every 30s
```

**Terminal 2 — Start the Dashboard (Streamlit)**
```powershell
cd C:\path\to\airflow_ai_agent
.venv\Scripts\Activate.ps1
streamlit run dashboard/app.py
```

### Open in Browser:
| Page | URL |
|---|---|
| 🖥 Dashboard | http://localhost:8501 |
| 🔧 API Docs (Swagger) | http://localhost:8000/docs |
| 🔧 API Health | http://localhost:8000/health |

---

## 🎯 Using Each Dashboard Page

### 🏠 Overview
Main dashboard with:
- KPI metrics (total incidents, resolved, active)
- Live CPU/Memory charts
- Recent incident table
- Error category pie chart

### 🚨 Incidents
All failures the agent has handled. Filter by status or DAG ID.
Color-coded by status (green=resolved, red=failed, blue=retrying)

### 🔍 Incident Detail
Deep dive into a single incident:
- AI root cause analysis
- Dependency check history (every probe attempt)
- Retry decisions with CPU/memory readings

### 🧪 Simulate Failure ← **Start here for demo**
Create a fake DAG failure and watch the agent react.

1. Choose a DAG name and error type
2. Click "Simulate Failure + Start Recovery"
3. The agent immediately:
   - Classifies the error
   - Starts checking dependencies in the background
   - Makes retry decisions when conditions are met
4. Navigate to **Incident Detail** to watch real-time progress

### 📊 System Resources
Live CPU/Memory gauges + historical trend charts.
Shows current resource thresholds used for retry decisions.

### 🧠 Learning Patterns
The agent's accumulated knowledge:
- Average wait time per DAG/error type
- Confidence scores (more data = higher confidence)
- Updated automatically after each successful recovery

### 📋 Agent Actions
Full audit trail of every action: classification, dependency checks, retry triggers.

---

## 🔌 Connecting to Real Apache Airflow

Edit `.env` to point to your Airflow instance:
```env
AIRFLOW_BASE_URL=http://your-airflow-server:8080
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=yourpassword
AIRFLOW_LOG_DIR=C:\path\to\airflow\logs
```

The agent uses Airflow's stable REST API (v1) — available in Airflow 2.0+.

### What the agent can do with real Airflow:
```python
# Detect failures
airflow.get_failed_dag_runs()           # Scan all failed runs

# Read task logs
airflow.get_task_log(dag_id, run_id, task_id)  # Get raw log text

# Trigger recovery
airflow.clear_task(dag_id, run_id, task_id)    # Clear for retry
airflow.trigger_dag(dag_id)                     # Fresh run
```

---

## 🔌 Configuring Real Dependency Checks

In `agent/supervisor.py`, replace `_simulate_dep_check` with real checks:

```python
# API check
result = self.checker.check_api("https://your-api.com/health")

# Database stored procedure check
result = self.checker.check_stored_procedure(
    db_url="postgresql://user:pass@host/db",
    proc_name="sp_get_daily_data",
    params=["2024-01-15"],
    min_rows=1
)

# File availability check
result = self.checker.check_file("/data/input/daily_extract.csv", min_size_bytes=1000)

# File freshness check (must be modified within last 60 minutes)
result = self.checker.check_file_age("/data/input/daily_extract.csv", max_age_minutes=60)

# Custom DB query
result = self.checker.check_db_query(
    db_url="...",
    query="SELECT COUNT(*) FROM daily_sales WHERE sale_date = CURDATE()",
    min_rows=1
)
```

---

## 📊 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | System health + Ollama + Airflow status |
| GET | `/api/status` | Agent status summary |
| GET | `/api/incidents` | All incidents (filter by status, dag_id) |
| GET | `/api/incidents/{id}` | Single incident with checks + decisions |
| POST | `/api/simulate` | Simulate a DAG failure |
| POST | `/api/classify` | Classify raw log text |
| POST | `/api/check-resources` | Take resource snapshot |
| GET | `/api/metrics/latest` | Current CPU/memory |
| GET | `/api/metrics/history` | CPU/memory history |
| GET | `/api/dependency-checks` | All dependency probe results |
| GET | `/api/actions` | Agent action audit log |
| GET | `/api/patterns` | Learned patterns |

---

## ⚙️ Configuration Reference (`.env`)

```env
# LLM model (change to phi3:mini for speed)
LLM_MODEL=llama3.1:8b-instruct-q4_K_M

# How often to scan for new failures (seconds)
MONITOR_INTERVAL_SECONDS=30

# How often to check dependencies after a failure (seconds)
DEPENDENCY_CHECK_INTERVAL_SECONDS=60

# Max retries before giving up and alerting
MAX_RETRIES=5

# Max total wait time before giving up (minutes)
MAX_WAIT_MINUTES=120

# Resource thresholds for retry safety
CPU_THRESHOLD_PERCENT=80
MEMORY_THRESHOLD_PERCENT=85
```

---

## ❗ Troubleshooting

| Problem | Solution |
|---|---|
| "uvicorn not found" | Activate venv: `.venv\Scripts\Activate.ps1` |
| Dashboard shows "API Offline" | Start backend first in Terminal 1 |
| Ollama not available | Agent still works in rule-based mode |
| "Module not found" | Re-run: `pip install -r requirements.txt` |
| Port in use | Use `--port 8001` and update `API = "http://localhost:8001"` in dashboard |

---

## 🛣️ Roadmap — What to Add Next

1. **Email/Slack alerting** — notify on-call when max retries reached
2. **Grafana integration** — expose Prometheus metrics endpoint
3. **DAG dependency map** — visualize which DAGs depend on which data sources
4. **Webhook triggers** — let external systems notify the agent directly
5. **Multi-Airflow support** — monitor multiple Airflow clusters
6. **Predictive failure** — predict failures before they happen using time-series patterns
7. **LangGraph workflows** — replace threading with proper stateful LangGraph graphs

---

Built for: Apache Airflow 2.x | AMD Ryzen 5 5600H | RTX 3050 | Windows 11
