"""
Log Monitor
-----------
Continuously scans Airflow log directories for new failures.
Supports both real Airflow log file watching and the Airflow REST API.
"""

import os
import re
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Callable
from loguru import logger

# Patterns that indicate a task has failed
FAILURE_INDICATORS = [
    r"Task exited with return code",
    r"Task failed with exception",
    r"Marking task as FAILED",
    r"State of this instance.*FAILED",
    r"\[ERROR\]",
    r"Traceback \(most recent call last\)",
    r"ERROR -",
    r"CRITICAL -",
]

class LogMonitor:
    def __init__(self, log_dir: str, airflow_client=None):
        self.log_dir = Path(log_dir)
        self.airflow_client = airflow_client
        self._seen_files: set[str] = set()
        self._seen_runs: set[str] = set()
        self.log_dir.mkdir(parents=True, exist_ok=True)

    # ── Real Log File Monitoring ──────────────────────────────────

    def scan_log_directory(self) -> list[dict]:
        """
        Scan the Airflow log directory for newly failed task logs.
        Airflow log structure: {log_dir}/{dag_id}/{task_id}/{execution_date}/{try_num}.log
        """
        failures = []
        for log_file in self.log_dir.rglob("*.log"):
            if str(log_file) in self._seen_files:
                continue

            # Only process recent files (last 24 hours)
            mtime = log_file.stat().st_mtime
            if time.time() - mtime > 86400:
                self._seen_files.add(str(log_file))
                continue

            try:
                content = log_file.read_text(encoding="utf-8", errors="replace")
                if self._is_failure_log(content):
                    parts = self._parse_path(log_file)
                    failures.append({
                        "dag_id":       parts.get("dag_id", "unknown"),
                        "task_id":      parts.get("task_id", "unknown"),
                        "run_id":       parts.get("run_id", "unknown"),
                        "log_path":     str(log_file),
                        "log_content":  content,
                        "detected_at":  datetime.utcnow().isoformat(),
                        "source":       "file",
                    })
                    self._seen_files.add(str(log_file))
            except Exception as e:
                logger.error(f"Error reading {log_file}: {e}")

        return failures

    def _is_failure_log(self, content: str) -> bool:
        for pattern in FAILURE_INDICATORS:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False

    def _parse_path(self, log_file: Path) -> dict:
        """Extract dag_id / task_id / execution_date from Airflow log path"""
        parts = log_file.parts
        try:
            # Find position of the log_dir in path
            base_parts = self.log_dir.parts
            relative = parts[len(base_parts):]
            return {
                "dag_id":  relative[0] if len(relative) > 0 else "unknown",
                "task_id": relative[1] if len(relative) > 1 else "unknown",
                "run_id":  relative[2] if len(relative) > 2 else "unknown",
            }
        except Exception:
            return {}

    # ── Airflow REST API Monitoring ───────────────────────────────

    def scan_airflow_api(self) -> list[dict]:
        """Scan failed DAG runs via Airflow REST API"""
        if not self.airflow_client:
            return []

        failures = []
        try:
            failed_runs = self.airflow_client.get_failed_dag_runs()
            for run in failed_runs:
                key = f"{run.get('dag_id')}_{run.get('dag_run_id')}"
                if key in self._seen_runs:
                    continue
                self._seen_runs.add(key)

                dag_id  = run.get("dag_id")
                run_id  = run.get("dag_run_id")

                # Get failed tasks
                tasks = self.airflow_client.get_task_instances(dag_id, run_id)
                failed_tasks = [t for t in tasks if t.get("state") == "failed"]

                for task in failed_tasks:
                    task_id = task.get("task_id")
                    log_content = self.airflow_client.get_task_log(dag_id, run_id, task_id)
                    failures.append({
                        "dag_id":      dag_id,
                        "task_id":     task_id,
                        "run_id":      run_id,
                        "log_content": log_content,
                        "detected_at": datetime.utcnow().isoformat(),
                        "source":      "airflow_api",
                    })
        except Exception as e:
            logger.error(f"API scan error: {e}")

        return failures

    # ── Combined Scan ─────────────────────────────────────────────

    def scan(self) -> list[dict]:
        """Scan all sources for failures"""
        failures = []
        # File-based logs
        failures.extend(self.scan_log_directory())
        # REST API
        failures.extend(self.scan_airflow_api())
        if failures:
            logger.info(f"🔍 Detected {len(failures)} new failure(s)")
        return failures

    # ── Demo / Simulation ─────────────────────────────────────────

    def create_demo_log(self, dag_id: str, task_id: str, error_type: str = "API_FAILURE") -> str:
        """
        Create a realistic simulated Airflow log file for testing.
        Use this when you don't have a real Airflow instance.
        """
        log_templates = {
            "API_FAILURE": """
[2024-01-15 02:00:15,123] {{taskinstance.py:1703}} INFO - Dependencies all met; queuing instance...
[2024-01-15 02:00:15,234] {{taskinstance.py:1322}} INFO - Starting attempt 1 of 3
[2024-01-15 02:00:15,456] {{taskinstance.py:1347}} INFO - Executing <Task(PythonOperator): fetch_api_data>
[2024-01-15 02:00:18,789] {{logging_mixin.py:137}} INFO - Sending request to Sales API...
[2024-01-15 02:00:48,901] {{logging_mixin.py:137}} ERROR - requests.exceptions.HTTPError: 503 Server Error: Service Unavailable
[2024-01-15 02:00:48,902] {{logging_mixin.py:137}} ERROR - Max retries exceeded with url: /api/v1/sales/daily
[2024-01-15 02:00:48,903] {{taskinstance.py:1703}} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/usr/local/lib/python3.9/site-packages/requests/adapters.py", line 519, in send
    raise ConnectionError(e, request=request)
requests.exceptions.ConnectionError: HTTPSConnectionPool(host='sales-api.internal', port=443): Max retries exceeded
[2024-01-15 02:00:48,950] {{taskinstance.py:1322}} INFO - Marking task as FAILED.
[2024-01-15 02:00:48,951] {{local_task_job.py:102}} INFO - Task exited with return code 1
""",
            "DATA_NOT_AVAILABLE": """
[2024-01-15 02:05:10,123] {{taskinstance.py:1703}} INFO - Starting attempt 1 of 3
[2024-01-15 02:05:10,234] {{taskinstance.py:1347}} INFO - Executing <Task(PythonOperator): run_stored_proc>
[2024-01-15 02:05:11,456] {{logging_mixin.py:137}} INFO - Calling stored procedure: sp_get_inventory_data
[2024-01-15 02:05:11,890] {{logging_mixin.py:137}} WARNING - Stored procedure returned 0 rows
[2024-01-15 02:05:11,891] {{logging_mixin.py:137}} ERROR - DataValidationError: Expected > 0 rows from sp_get_inventory_data
[2024-01-15 02:05:11,892] {{logging_mixin.py:137}} ERROR - Empty result set — source data not yet populated
[2024-01-15 02:05:11,900] {{taskinstance.py:1322}} ERROR - Task failed with exception
DataValidationError: Stored procedure returned empty result set
[2024-01-15 02:05:11,950] {{taskinstance.py:1322}} INFO - Marking task as FAILED.
""",
            "FILE_NOT_AVAILABLE": """
[2024-01-15 03:00:45,123] {{taskinstance.py:1347}} INFO - Executing <Task(PythonOperator): load_source_file>
[2024-01-15 03:00:45,456] {{logging_mixin.py:137}} INFO - Looking for file: /data/finance/daily_extract_20240115.csv
[2024-01-15 03:00:45,890] {{logging_mixin.py:137}} ERROR - FileNotFoundError: /data/finance/daily_extract_20240115.csv
[2024-01-15 03:00:45,891] {{logging_mixin.py:137}} ERROR - No such file or directory
[2024-01-15 03:00:45,900] {{taskinstance.py:1322}} ERROR - Task failed with exception
FileNotFoundError: [Errno 2] No such file or directory: '/data/finance/daily_extract_20240115.csv'
[2024-01-15 03:00:45,950] {{taskinstance.py:1322}} INFO - Marking task as FAILED.
""",
            "TIMEOUT": """
[2024-01-15 04:12:30,123] {{taskinstance.py:1347}} INFO - Executing <Task(PythonOperator): transform_data>
[2024-01-15 04:12:30,456] {{logging_mixin.py:137}} INFO - Connecting to database for transform operation...
[2024-01-15 04:12:30,890] {{logging_mixin.py:137}} ERROR - sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server timed out
[2024-01-15 04:12:30,891] {{logging_mixin.py:137}} ERROR - QueuePool limit of size 5 overflow 10 reached, connection timed out
[2024-01-15 04:12:30,900] {{taskinstance.py:1322}} ERROR - Task failed with exception
sqlalchemy.exc.OperationalError: connection timed out, server is under high load
[2024-01-15 04:12:30,950] {{taskinstance.py:1322}} INFO - Marking task as FAILED.
""",
        }

        content = log_templates.get(error_type, log_templates["API_FAILURE"])
        log_dir = self.log_dir / dag_id / task_id / "2024-01-15T02:00:00"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "1.log"
        log_path.write_text(content)
        logger.info(f"📝 Created demo log: {log_path}")
        return str(log_path)
