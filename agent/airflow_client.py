"""
Airflow REST API Client
-----------------------
Wraps Airflow's stable REST API (v1) for DAG management.
Works with Airflow 2.x and above.
"""

import time
from typing import Optional
import httpx
from loguru import logger
from config.settings import get_settings

settings = get_settings()

class AirflowClient:
    def __init__(self):
        self.base_url = settings.AIRFLOW_BASE_URL.rstrip("/")
        self.auth = (settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD)
        self.api_base = f"{self.base_url}/api/v1"
        self._connected = None

    def _get(self, path: str) -> Optional[dict]:
        try:
            r = httpx.get(
                f"{self.api_base}{path}",
                auth=self.auth, timeout=15
            )
            if r.status_code == 200:
                return r.json()
            logger.warning(f"Airflow GET {path} → {r.status_code}")
            return None
        except Exception as e:
            logger.error(f"Airflow GET {path} failed: {e}")
            return None

    def _post(self, path: str, json_body: dict) -> Optional[dict]:
        try:
            r = httpx.post(
                f"{self.api_base}{path}",
                auth=self.auth,
                json=json_body,
                timeout=30
            )
            if r.status_code in (200, 201):
                return r.json()
            logger.warning(f"Airflow POST {path} → {r.status_code}: {r.text}")
            return None
        except Exception as e:
            logger.error(f"Airflow POST {path} failed: {e}")
            return None

    def _patch(self, path: str, json_body: dict) -> Optional[dict]:
        try:
            r = httpx.patch(
                f"{self.api_base}{path}",
                auth=self.auth,
                json=json_body,
                timeout=30
            )
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            logger.error(f"Airflow PATCH {path} failed: {e}")
            return None

    def is_connected(self) -> bool:
        result = self._get("/health")
        self._connected = result is not None
        return self._connected

    def get_dags(self) -> list:
        data = self._get("/dags?limit=100")
        return data.get("dags", []) if data else []

    def get_dag_runs(self, dag_id: str, limit: int = 10) -> list:
        data = self._get(f"/dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date")
        return data.get("dag_runs", []) if data else []

    def get_failed_dag_runs(self, dag_id: str = None) -> list:
        """Get all recently failed DAG runs"""
        if dag_id:
            runs = self.get_dag_runs(dag_id, limit=20)
        else:
            # Get runs across all DAGs
            data = self._get("/dags/~/dagRuns?limit=50&state=failed")
            runs = data.get("dag_runs", []) if data else []
        return [r for r in runs if r.get("state") == "failed"]

    def get_task_instances(self, dag_id: str, run_id: str) -> list:
        data = self._get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        return data.get("task_instances", []) if data else []

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        """Fetch raw log content for a task instance"""
        try:
            r = httpx.get(
                f"{self.api_base}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
                auth=self.auth,
                headers={"Accept": "text/plain"},
                timeout=30
            )
            return r.text if r.status_code == 200 else ""
        except Exception as e:
            logger.error(f"Failed to fetch log: {e}")
            return ""

    def trigger_dag(self, dag_id: str, conf: dict = None) -> Optional[dict]:
        """Trigger a new DAG run"""
        from datetime import datetime, timezone
        body = {
            "execution_date": datetime.now(timezone.utc).isoformat(),
            "conf": conf or {}
        }
        result = self._post(f"/dags/{dag_id}/dagRuns", body)
        if result:
            logger.info(f"✅ Triggered DAG: {dag_id} → run_id={result.get('dag_run_id')}")
        return result

    def clear_task(self, dag_id: str, run_id: str, task_id: str) -> bool:
        """Clear a failed task so it can be retried"""
        body = {
            "dry_run": False,
            "task_ids": [task_id],
            "only_failed": True,
            "reset_dag_runs": False,
            "include_subdags": False,
            "include_parentdag": False,
        }
        result = self._post(f"/dags/{dag_id}/clearTaskInstances", body)
        return result is not None

    def set_task_state(self, dag_id: str, run_id: str, task_id: str, state: str) -> bool:
        body = {"dry_run": False, "new_state": state}
        result = self._patch(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}",
            body
        )
        return result is not None

    def unpause_dag(self, dag_id: str) -> bool:
        result = self._patch(f"/dags/{dag_id}", {"is_paused": False})
        return result is not None

    def get_health(self) -> dict:
        data = self._get("/health")
        return data or {"status": "unreachable"}

    def simulate_failure(self, dag_id: str, task_id: str,
                          error_type: str = "API_FAILURE") -> dict:
        """
        Creates a simulated failure incident (for demo/testing without real Airflow).
        In production this is replaced by real Airflow log reading.
        """
        from datetime import datetime
        errors = {
            "API_FAILURE":        "HTTPError: 503 Service Unavailable — endpoint not responding",
            "DATA_NOT_AVAILABLE": "StoredProcedureError: sp_get_data returned 0 rows — source not ready",
            "FILE_NOT_AVAILABLE": "FileNotFoundError: required input file not found",
            "TIMEOUT":            "OperationalError: connection timed out — database under high load",
            "INFRA":              "AirflowException: Worker node unreachable — infrastructure issue",
        }
        return {
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": f"simulated__{datetime.utcnow().isoformat()}",
            "state": "failed",
            "error_message": errors.get(error_type, errors["API_FAILURE"]),
            "execution_date": datetime.utcnow().isoformat(),
            "simulated": True,
        }
