"""
AI Supervisor — Main Orchestration Engine
------------------------------------------
The brain of the system. Coordinates:
  1. Log monitoring
  2. Error classification
  3. Dependency checking
  4. Resource monitoring
  5. Intelligent retry decisions
  6. DAG recovery
  7. Learning from history

This runs as a continuous background loop.
"""

import time
import json
import threading
from datetime import datetime, timedelta
from typing import Optional, Callable
from loguru import logger

from config.settings import get_settings
from db.models import (
    SessionLocal, DAGFailureIncident, DependencyCheck,
    RetryDecision, AgentAction, SystemMetricSnapshot, LearningPattern
)
from agent.log_monitor import LogMonitor
from agent.error_classifier import ErrorClassifier
from agent.dependency_checker import DependencyChecker
from agent.resource_monitor import ResourceMonitor
from agent.airflow_client import AirflowClient
from core.llm import get_llm

settings = get_settings()

# ── Retry Strategy per Error Category ────────────────────────────

RETRY_STRATEGY = {
    "API_FAILURE": {
        "check_type": "API",
        "initial_wait_seconds": 60,
        "backoff_multiplier": 1.5,
        "max_checks": 30,
    },
    "DATA_NOT_AVAILABLE": {
        "check_type": "DATABASE",
        "initial_wait_seconds": 120,
        "backoff_multiplier": 1.2,
        "max_checks": 20,
    },
    "FILE_NOT_AVAILABLE": {
        "check_type": "FILE",
        "initial_wait_seconds": 60,
        "backoff_multiplier": 1.0,  # Fixed interval for file watch
        "max_checks": 30,
    },
    "TIMEOUT": {
        "check_type": "SYSTEM",
        "initial_wait_seconds": 120,
        "backoff_multiplier": 1.5,
        "max_checks": 15,
    },
    "INFRA": {
        "check_type": "SYSTEM",
        "initial_wait_seconds": 300,
        "backoff_multiplier": 2.0,
        "max_checks": 10,
    },
    "DB_ERROR": {
        "check_type": "DATABASE",
        "initial_wait_seconds": 90,
        "backoff_multiplier": 1.5,
        "max_checks": 20,
    },
    "UNKNOWN": {
        "check_type": "SYSTEM",
        "initial_wait_seconds": 300,
        "backoff_multiplier": 1.0,
        "max_checks": 5,
    },
}

class AISupervisor:
    def __init__(self):
        self.settings = settings
        self.llm = get_llm()
        self.airflow = AirflowClient()
        self.classifier = ErrorClassifier(llm=self.llm)
        self.checker = DependencyChecker()
        self.resource_monitor = ResourceMonitor(
            cpu_threshold=settings.CPU_THRESHOLD_PERCENT,
            mem_threshold=settings.MEMORY_THRESHOLD_PERCENT,
        )
        self.log_monitor = LogMonitor(
            log_dir=settings.AIRFLOW_LOG_DIR,
            airflow_client=self.airflow if self.airflow.is_connected() else None
        )
        self._running = False
        self._active_incidents: dict[str, int] = {}   # dag+task → incident_id
        self._event_callbacks: list[Callable] = []     # UI can subscribe to events
        logger.info("🤖 AI Supervisor initialized")
        if self.llm:
            logger.info(f"✅ LLM ready: {settings.LLM_MODEL}")
        else:
            logger.info("📋 LLM offline — rule-based mode active")

    # ── Event System (for real-time UI updates) ───────────────────

    def on_event(self, callback: Callable):
        """Register a callback to receive agent events"""
        self._event_callbacks.append(callback)

    def _emit(self, event_type: str, data: dict):
        """Emit an event to all registered callbacks"""
        event = {"type": event_type, "data": data, "ts": datetime.utcnow().isoformat()}
        for cb in self._event_callbacks:
            try:
                cb(event)
            except Exception:
                pass

    # ── Main Loop ─────────────────────────────────────────────────

    def start(self):
        """Start the supervisor loop in a background thread"""
        self._running = True
        thread = threading.Thread(target=self._run_loop, daemon=True)
        thread.start()
        logger.info("🚀 Supervisor started in background thread")
        return thread

    def stop(self):
        self._running = False
        logger.info("🛑 Supervisor stopping...")

    def _run_loop(self):
        """Main monitoring loop"""
        logger.info(f"🔄 Monitor loop started — polling every {settings.MONITOR_INTERVAL_SECONDS}s")
        self._take_metric_snapshot()

        while self._running:
            try:
                # Scan for new failures
                failures = self.log_monitor.scan()
                for failure in failures:
                    self._handle_failure(failure)

                # Take periodic resource snapshot
                self._take_metric_snapshot()

            except Exception as e:
                logger.error(f"Supervisor loop error: {e}")

            time.sleep(settings.MONITOR_INTERVAL_SECONDS)

    # ── Failure Handling Pipeline ─────────────────────────────────

    def _handle_failure(self, failure: dict):
        """Full pipeline: classify → check deps → decide → retry"""
        dag_id  = failure["dag_id"]
        task_id = failure["task_id"]
        run_id  = failure["run_id"]

        logger.info(f"🚨 Handling failure: dag={dag_id} task={task_id}")
        self._emit("failure_detected", {"dag_id": dag_id, "task_id": task_id})

        # Step 1: Classify the error
        classification = self.classifier.classify(
            failure.get("log_content", ""),
            dag_id, task_id
        )
        logger.info(f"🏷️ Classified: {classification.category} (confidence={classification.confidence})")
        self._emit("error_classified", {
            "dag_id": dag_id, "category": classification.category,
            "confidence": classification.confidence
        })

        # Step 2: Create incident record
        incident_id = self._create_incident(failure, classification)
        self._log_action("CLASSIFY", dag_id, {
            "category": classification.category,
            "confidence": classification.confidence,
            "incident_id": incident_id
        })

        # Step 3: Start the retry/wait loop in a dedicated thread
        worker = threading.Thread(
            target=self._recovery_loop,
            args=(incident_id, dag_id, task_id, run_id, failure, classification),
            daemon=True
        )
        worker.start()

    def _recovery_loop(self, incident_id: int, dag_id: str, task_id: str,
                        run_id: str, failure: dict, classification):
        """Per-incident recovery thread — checks deps, waits, retries"""
        strategy = RETRY_STRATEGY.get(classification.category, RETRY_STRATEGY["UNKNOWN"])
        wait_seconds = strategy["initial_wait_seconds"]
        max_checks   = strategy["max_checks"]
        check_count  = 0
        retry_count  = 0
        total_waited = 0  # minutes

        self._update_incident_status(incident_id, "WAITING_FOR_DATA")

        while check_count < max_checks and retry_count < settings.MAX_RETRIES:
            check_count += 1
            logger.info(f"⏳ [{dag_id}] Check {check_count}/{max_checks} — waiting {wait_seconds}s")
            time.sleep(wait_seconds)
            total_waited += wait_seconds / 60

            # Step A: Check dependencies
            dep_ready = self._check_dependencies(incident_id, dag_id, classification)
            self._emit("dependency_check", {
                "dag_id": dag_id, "ready": dep_ready,
                "check": check_count, "max": max_checks
            })

            if not dep_ready:
                # Apply backoff
                wait_seconds = min(
                    wait_seconds * strategy["backoff_multiplier"],
                    settings.DEPENDENCY_CHECK_INTERVAL_SECONDS * 5
                )
                continue

            # Step B: Check resources
            self._update_incident_status(incident_id, "WAITING_FOR_RESOURCES")
            resource_snap = self.resource_monitor.snapshot(settings.DATABASE_URL)
            self._emit("resource_check", {
                "dag_id": dag_id,
                "cpu": resource_snap.cpu_percent,
                "mem": resource_snap.mem_percent,
                "safe": resource_snap.is_safe_to_retry
            })

            if not resource_snap.is_safe_to_retry:
                logger.info(f"⚠️ [{dag_id}] Resources not safe: {resource_snap.reason}")
                time.sleep(60)  # Short wait then recheck
                continue

            # Step C: Make retry decision
            retry_count += 1
            decision = self._make_retry_decision(
                incident_id, dag_id, retry_count, resource_snap, classification
            )

            if decision == "ESCALATE":
                self._update_incident_status(incident_id, "FAILED")
                self._emit("escalation", {"dag_id": dag_id, "incident_id": incident_id})
                break

            if decision == "RETRY":
                # Step D: Trigger DAG retry
                self._update_incident_status(incident_id, "RETRYING")
                success = self._trigger_retry(dag_id, task_id, run_id)
                self._emit("retry_triggered", {
                    "dag_id": dag_id, "retry": retry_count, "success": success
                })

                if success:
                    # Wait and check outcome
                    time.sleep(120)  # Give Airflow 2 min to process
                    if self._check_dag_succeeded(dag_id, run_id):
                        self._update_incident_status(incident_id, "RESOLVED")
                        self._finalize_incident(incident_id, "RETRIED_SUCCESS", retry_count, total_waited)
                        self._learn_from_success(dag_id, classification, wait_seconds / 60, retry_count)
                        self._emit("resolved", {"dag_id": dag_id, "retries": retry_count})
                        logger.info(f"✅ [{dag_id}] Resolved after {retry_count} retries!")
                        return
                    # DAG still failed — continue loop
                else:
                    logger.warning(f"⚠️ [{dag_id}] Retry trigger failed (Airflow unreachable?)")

            wait_seconds = max(wait_seconds, 60)

        # Max retries reached
        self._update_incident_status(incident_id, "FAILED")
        self._finalize_incident(incident_id, "MAX_RETRIES", retry_count, total_waited)
        self._emit("max_retries", {"dag_id": dag_id, "incident_id": incident_id})
        logger.error(f"❌ [{dag_id}] Max retries reached — escalating")

    # ── Dependency Checking ───────────────────────────────────────

    def _check_dependencies(self, incident_id: int,
                             dag_id: str, classification) -> bool:
        """Check the right dependency type based on error category"""
        db = SessionLocal()
        try:
            cat = classification.category
            check_result = None

            if cat == "API_FAILURE":
                # In production: load API URL from DAG config / metadata
                # For demo: simulate becoming available over time
                check_result = self._simulate_dep_check("API", dag_id)

            elif cat in ("DATA_NOT_AVAILABLE", "DB_ERROR"):
                check_result = self._simulate_dep_check("DATABASE", dag_id)

            elif cat == "FILE_NOT_AVAILABLE":
                check_result = self._simulate_dep_check("FILE", dag_id)

            elif cat in ("TIMEOUT", "INFRA"):
                resource = self.resource_monitor.snapshot()
                check_result = {
                    "check_type": "SYSTEM",
                    "target":     "system_resources",
                    "available":  resource.is_safe_to_retry,
                    "detail":     resource.reason,
                    "response_ms": None
                }

            if check_result:
                db.add(DependencyCheck(
                    incident_id=incident_id,
                    dag_id=dag_id,
                    check_type=check_result["check_type"],
                    check_target=check_result["target"],
                    check_result="AVAILABLE" if check_result["available"] else "UNAVAILABLE",
                    response_time_ms=check_result.get("response_ms"),
                    details=check_result.get("detail", "")
                ))
                db.commit()
                return check_result["available"]
            return False
        finally:
            db.close()

    def _simulate_dep_check(self, check_type: str, dag_id: str) -> dict:
        """
        Simulated dependency check for demo purposes.
        In production, replace with real API/DB/file checks.
        After ~2 minutes, dependencies become 'available'.
        """
        import random
        # For demo: 30% chance of being ready each check
        available = random.random() > 0.7
        return {
            "check_type": check_type,
            "target":     f"dependency_for_{dag_id}",
            "available":  available,
            "detail":     "Available" if available else "Not yet ready",
            "response_ms": random.uniform(100, 500)
        }

    # ── Retry Decision ────────────────────────────────────────────

    def _make_retry_decision(self, incident_id: int, dag_id: str,
                              retry_num: int, resource_snap,
                              classification) -> str:
        """Decide: RETRY / WAIT / ESCALATE / ABORT"""
        if retry_num > settings.MAX_RETRIES:
            decision = "ESCALATE"
            reason = f"Exceeded max retries ({settings.MAX_RETRIES})"
        elif not resource_snap.is_safe_to_retry:
            decision = "WAIT"
            reason = f"Resources not safe: {resource_snap.reason}"
        else:
            decision = "RETRY"
            reason = f"Dependencies satisfied, resources OK. Retry #{retry_num}"

        db = SessionLocal()
        try:
            db.add(RetryDecision(
                incident_id=incident_id,
                dag_id=dag_id,
                decision=decision,
                reason=reason,
                cpu_at_decision=resource_snap.cpu_percent,
                mem_at_decision=resource_snap.mem_percent,
                retry_number=retry_num,
            ))
            db.commit()
        finally:
            db.close()

        logger.info(f"📋 [{dag_id}] Decision: {decision} — {reason}")
        return decision

    # ── Airflow DAG Triggering ────────────────────────────────────

    def _trigger_retry(self, dag_id: str, task_id: str, run_id: str) -> bool:
        """Trigger the actual Airflow retry"""
        try:
            if self.airflow.is_connected():
                # Real Airflow: clear the failed task to allow retry
                cleared = self.airflow.clear_task(dag_id, run_id, task_id)
                if cleared:
                    logger.info(f"✅ Task cleared for retry: {dag_id}/{task_id}")
                    return True
                # Fallback: trigger a fresh run
                result = self.airflow.trigger_dag(dag_id)
                return result is not None
            else:
                # Simulated (no real Airflow)
                logger.info(f"🔄 [SIMULATED] Would trigger retry: {dag_id}/{task_id}")
                self._log_action("RETRY_TRIGGER", dag_id, {
                    "task_id": task_id, "run_id": run_id, "simulated": True
                })
                return True
        except Exception as e:
            logger.error(f"Retry trigger failed: {e}")
            return False

    def _check_dag_succeeded(self, dag_id: str, run_id: str) -> bool:
        """Check if the DAG run completed successfully"""
        if self.airflow.is_connected():
            runs = self.airflow.get_dag_runs(dag_id, limit=3)
            for run in runs:
                if run.get("dag_run_id") == run_id:
                    return run.get("state") == "success"
        # Simulated: 70% success rate for demo
        import random
        return random.random() > 0.3

    # ── Learning ──────────────────────────────────────────────────

    def _learn_from_success(self, dag_id: str, classification,
                             wait_minutes: float, retry_count: int):
        """Update learning patterns based on successful recovery"""
        db = SessionLocal()
        try:
            cat = classification.category
            # Update or create "avg wait minutes" pattern
            pattern = db.query(LearningPattern).filter(
                LearningPattern.dag_id == dag_id,
                LearningPattern.error_category == cat,
                LearningPattern.pattern_type == "AVG_WAIT_MINUTES"
            ).first()

            if pattern:
                # Running average
                old_avg = float(pattern.pattern_value)
                n = pattern.sample_count
                new_avg = (old_avg * n + wait_minutes) / (n + 1)
                pattern.pattern_value = str(round(new_avg, 1))
                pattern.sample_count = n + 1
                pattern.confidence = min(0.95, 0.5 + n * 0.05)
                pattern.updated_at = datetime.utcnow()
            else:
                db.add(LearningPattern(
                    dag_id=dag_id,
                    error_category=cat,
                    pattern_type="AVG_WAIT_MINUTES",
                    pattern_value=str(round(wait_minutes, 1)),
                    sample_count=1,
                    confidence=0.5
                ))
            db.commit()
            logger.info(f"📚 Updated learning pattern for {dag_id}/{cat}")
        except Exception as e:
            logger.error(f"Learning update error: {e}")
            db.rollback()
        finally:
            db.close()

    def get_learned_wait_time(self, dag_id: str, error_category: str) -> Optional[float]:
        """Get the historically optimal wait time for this DAG + error type"""
        db = SessionLocal()
        try:
            pattern = db.query(LearningPattern).filter(
                LearningPattern.dag_id == dag_id,
                LearningPattern.error_category == error_category,
                LearningPattern.pattern_type == "AVG_WAIT_MINUTES",
                LearningPattern.confidence >= 0.6
            ).first()
            return float(pattern.pattern_value) if pattern else None
        finally:
            db.close()

    # ── DB Helpers ────────────────────────────────────────────────

    def _create_incident(self, failure: dict, classification) -> int:
        db = SessionLocal()
        try:
            incident = DAGFailureIncident(
                dag_id=failure["dag_id"],
                task_id=failure.get("task_id", ""),
                run_id=failure.get("run_id", ""),
                error_category=classification.category,
                error_message=failure.get("log_content", "")[-500:],
                root_cause=classification.root_cause,
                log_snippet="\n".join(classification.key_evidence[:10]),
                status="INVESTIGATING"
            )
            db.add(incident)
            db.commit()
            db.refresh(incident)
            return incident.id
        finally:
            db.close()

    def _update_incident_status(self, incident_id: int, status: str):
        db = SessionLocal()
        try:
            inc = db.query(DAGFailureIncident).filter(
                DAGFailureIncident.id == incident_id
            ).first()
            if inc:
                inc.status = status
                db.commit()
        finally:
            db.close()

    def _finalize_incident(self, incident_id: int, resolution: str,
                            retry_count: int, total_wait_mins: float):
        db = SessionLocal()
        try:
            inc = db.query(DAGFailureIncident).filter(
                DAGFailureIncident.id == incident_id
            ).first()
            if inc:
                inc.resolution = resolution
                inc.retry_count = retry_count
                inc.total_wait_mins = round(total_wait_mins, 1)
                inc.resolved_at = datetime.utcnow() if resolution == "RETRIED_SUCCESS" else None
                db.commit()
        finally:
            db.close()

    def _log_action(self, action_type: str, dag_id: str, details: dict,
                    success: bool = True, error_msg: str = None, duration_ms: int = None):
        db = SessionLocal()
        try:
            db.add(AgentAction(
                action_type=action_type,
                dag_id=dag_id,
                details=details,
                success=success,
                error_msg=error_msg,
                duration_ms=duration_ms
            ))
            db.commit()
        finally:
            db.close()

    def _take_metric_snapshot(self):
        db = SessionLocal()
        try:
            snap = self.resource_monitor.snapshot()
            db.add(SystemMetricSnapshot(
                cpu_percent=snap.cpu_percent,
                mem_percent=snap.mem_percent,
                disk_percent=snap.disk_percent,
                load_avg_1m=snap.load_avg_1m,
                load_avg_5m=snap.load_avg_5m,
            ))
            db.commit()
        finally:
            db.close()

    # ── Manual Triggers (for API / Dashboard) ─────────────────────

    def simulate_failure_and_recover(self, dag_id: str, task_id: str,
                                      error_type: str = "API_FAILURE") -> dict:
        """
        Manually trigger a simulated failure + recovery cycle.
        Used by the dashboard and API for demo/testing.
        """
        failure = self.airflow.simulate_failure(dag_id, task_id, error_type)
        # Add a demo log
        self.log_monitor.create_demo_log(dag_id, task_id, error_type)

        # Run classification synchronously for the API response
        classification = self.classifier.classify(
            failure.get("error_message", ""), dag_id, task_id
        )

        incident_id = self._create_incident(failure, classification)
        self._log_action("SIMULATE", dag_id, {
            "error_type": error_type, "incident_id": incident_id
        })

        # Kick off recovery in background
        worker = threading.Thread(
            target=self._recovery_loop,
            args=(incident_id, dag_id, task_id,
                  failure["run_id"], failure, classification),
            daemon=True
        )
        worker.start()

        return {
            "incident_id": incident_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "error_type": error_type,
            "classification": classification.model_dump(),
            "status": "recovery_started",
            "message": f"Recovery loop started for incident #{incident_id}"
        }

    def get_status_summary(self) -> dict:
        """Return a summary dict for the dashboard health panel"""
        db = SessionLocal()
        try:
            total     = db.query(DAGFailureIncident).count()
            resolved  = db.query(DAGFailureIncident).filter(DAGFailureIncident.status == "RESOLVED").count()
            active    = db.query(DAGFailureIncident).filter(
                DAGFailureIncident.status.in_(["INVESTIGATING","WAITING_FOR_DATA","WAITING_FOR_RESOURCES","RETRYING"])
            ).count()
            snap = self.resource_monitor.snapshot()
            return {
                "supervisor_running": self._running,
                "llm_mode": "llm" if self.llm else "rules",
                "airflow_connected": self.airflow.is_connected(),
                "total_incidents": total,
                "resolved_incidents": resolved,
                "active_incidents": active,
                "system": snap.model_dump()
            }
        finally:
            db.close()
