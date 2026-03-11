"""
Airflow AI Agent — FastAPI Backend
-----------------------------------
Run: uvicorn main:app --reload --port 8000
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from loguru import logger
from typing import Optional

from config.settings import get_settings
from db.models import (
    create_tables, seed_demo_incidents, get_db,
    DAGFailureIncident, DependencyCheck, RetryDecision,
    AgentAction, SystemMetricSnapshot, LearningPattern
)
from core.llm import check_ollama_status
from agent.supervisor import AISupervisor

settings = get_settings()
supervisor = AISupervisor()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting Airflow AI Agent...")
    os.makedirs(settings.AIRFLOW_LOG_DIR, exist_ok=True)
    create_tables()
    seed_demo_incidents()
    supervisor.start()   # Start background monitoring loop
    logger.info("✅ Agent started and monitoring")
    yield
    supervisor.stop()
    logger.info("Shutdown complete")

app = FastAPI(
    title="Airflow AI Agent",
    description="Intelligent Airflow workflow supervisor with local LLM",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Health & Status ───────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    return {
        "status": "ok",
        "ollama": check_ollama_status(),
        "airflow_connected": supervisor.airflow.is_connected(),
        "supervisor": supervisor.get_status_summary()
    }

@app.get("/api/status", tags=["System"])
def agent_status():
    return supervisor.get_status_summary()

@app.get("/api/metrics/latest", tags=["System"])
def latest_metrics(db: Session = Depends(get_db)):
    snap = db.query(SystemMetricSnapshot).order_by(
        SystemMetricSnapshot.recorded_at.desc()
    ).first()
    if not snap:
        return supervisor.resource_monitor.snapshot().model_dump()
    return {
        "cpu_percent":   snap.cpu_percent,
        "mem_percent":   snap.mem_percent,
        "disk_percent":  snap.disk_percent,
        "load_avg_1m":   snap.load_avg_1m,
        "load_avg_5m":   snap.load_avg_5m,
        "db_response_ms": snap.db_response_ms,
        "recorded_at":   str(snap.recorded_at)
    }

@app.get("/api/metrics/history", tags=["System"])
def metrics_history(limit: int = 50, db: Session = Depends(get_db)):
    snaps = db.query(SystemMetricSnapshot).order_by(
        SystemMetricSnapshot.recorded_at.desc()
    ).limit(limit).all()
    return [{
        "cpu": s.cpu_percent, "mem": s.mem_percent,
        "load": s.load_avg_1m, "ts": str(s.recorded_at)
    } for s in reversed(snaps)]

# ── Incidents ─────────────────────────────────────────────────────

@app.get("/api/incidents", tags=["Incidents"])
def list_incidents(
    status: Optional[str] = None,
    dag_id: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    q = db.query(DAGFailureIncident)
    if status:
        q = q.filter(DAGFailureIncident.status == status)
    if dag_id:
        q = q.filter(DAGFailureIncident.dag_id == dag_id)
    incidents = q.order_by(DAGFailureIncident.detected_at.desc()).limit(limit).all()
    return [_inc_dict(i) for i in incidents]

@app.get("/api/incidents/{incident_id}", tags=["Incidents"])
def get_incident(incident_id: int, db: Session = Depends(get_db)):
    inc = db.query(DAGFailureIncident).filter(DAGFailureIncident.id == incident_id).first()
    if not inc:
        raise HTTPException(404, f"Incident {incident_id} not found")
    dep_checks = db.query(DependencyCheck).filter(
        DependencyCheck.incident_id == incident_id
    ).all()
    decisions = db.query(RetryDecision).filter(
        RetryDecision.incident_id == incident_id
    ).all()
    return {
        **_inc_dict(inc),
        "dependency_checks": [{
            "check_type": d.check_type, "target": d.check_target,
            "result": d.check_result, "detail": d.details,
            "checked_at": str(d.checked_at)
        } for d in dep_checks],
        "retry_decisions": [{
            "decision": r.decision, "reason": r.reason,
            "retry_number": r.retry_number, "cpu": r.cpu_at_decision,
            "mem": r.mem_at_decision, "decided_at": str(r.decided_at)
        } for r in decisions],
    }

def _inc_dict(i: DAGFailureIncident) -> dict:
    return {
        "id": i.id, "dag_id": i.dag_id, "task_id": i.task_id,
        "run_id": i.run_id, "error_category": i.error_category,
        "error_message": i.error_message, "root_cause": i.root_cause,
        "status": i.status, "retry_count": i.retry_count,
        "total_wait_mins": i.total_wait_mins, "resolution": i.resolution,
        "detected_at": str(i.detected_at),
        "resolved_at": str(i.resolved_at) if i.resolved_at else None,
    }

# ── Agent Control ─────────────────────────────────────────────────

class SimulateRequest(BaseModel):
    dag_id:     str
    task_id:    str
    error_type: str = "API_FAILURE"

@app.post("/api/simulate", tags=["Agent"])
def simulate_failure(req: SimulateRequest):
    """Simulate a DAG failure and start the intelligent recovery process"""
    result = supervisor.simulate_failure_and_recover(
        req.dag_id, req.task_id, req.error_type
    )
    return result

@app.post("/api/classify", tags=["Agent"])
def classify_log(log_text: str):
    """Classify an error from raw log text"""
    from agent.error_classifier import ErrorClassifier
    from core.llm import get_llm
    classifier = ErrorClassifier(llm=get_llm())
    result = classifier.classify(log_text)
    return result.model_dump()

@app.post("/api/check-resources", tags=["Agent"])
def check_resources():
    snap = supervisor.resource_monitor.snapshot(settings.DATABASE_URL)
    return snap.model_dump()

# ── Dependency Checks ──────────────────────────────────────────────

@app.get("/api/dependency-checks", tags=["Dependencies"])
def list_dep_checks(incident_id: Optional[int] = None,
                    limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(DependencyCheck)
    if incident_id:
        q = q.filter(DependencyCheck.incident_id == incident_id)
    checks = q.order_by(DependencyCheck.checked_at.desc()).limit(limit).all()
    return [{
        "id": c.id, "dag_id": c.dag_id, "check_type": c.check_type,
        "target": c.check_target, "result": c.check_result,
        "response_ms": c.response_time_ms, "detail": c.details,
        "checked_at": str(c.checked_at)
    } for c in checks]

# ── Agent Action Logs ──────────────────────────────────────────────

@app.get("/api/actions", tags=["Audit"])
def list_actions(limit: int = 100, db: Session = Depends(get_db)):
    actions = db.query(AgentAction).order_by(
        AgentAction.created_at.desc()
    ).limit(limit).all()
    return [{
        "id": a.id, "action_type": a.action_type, "dag_id": a.dag_id,
        "details": a.details, "success": a.success, "error_msg": a.error_msg,
        "duration_ms": a.duration_ms, "created_at": str(a.created_at)
    } for a in actions]

# ── Learning Patterns ──────────────────────────────────────────────

@app.get("/api/patterns", tags=["Learning"])
def list_patterns(db: Session = Depends(get_db)):
    patterns = db.query(LearningPattern).order_by(
        LearningPattern.confidence.desc()
    ).all()
    return [{
        "id": p.id, "dag_id": p.dag_id, "error_category": p.error_category,
        "pattern_type": p.pattern_type, "pattern_value": p.pattern_value,
        "sample_count": p.sample_count, "confidence": p.confidence,
        "updated_at": str(p.updated_at)
    } for p in patterns]
