"""
Database Models
---------------
Stores all agent state, incident history, dependency checks,
and retry decisions. This is the agent's "memory" for learning.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    DateTime, Text, JSON, Boolean
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from datetime import datetime
from config.settings import get_settings

settings = get_settings()

engine = create_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    pass

# ── Core Models ───────────────────────────────────────────────────

class DAGFailureIncident(Base):
    """Every detected DAG/task failure is recorded here"""
    __tablename__ = "dag_failure_incidents"

    id              = Column(Integer, primary_key=True, index=True)
    dag_id          = Column(String(200), nullable=False, index=True)
    task_id         = Column(String(200))
    run_id          = Column(String(300))
    error_category  = Column(String(100))   # API_FAILURE / DATA_NOT_AVAILABLE / DB_ERROR / TIMEOUT / INFRA
    error_message   = Column(Text)
    root_cause      = Column(Text)          # LLM-generated root cause
    log_snippet     = Column(Text)          # Relevant log lines
    detected_at     = Column(DateTime, default=datetime.utcnow)
    resolved_at     = Column(DateTime, nullable=True)
    resolution      = Column(String(100))   # RETRIED_SUCCESS / MAX_RETRIES / MANUAL
    retry_count     = Column(Integer, default=0)
    total_wait_mins = Column(Float, default=0)
    status          = Column(String(50), default="INVESTIGATING")
    # INVESTIGATING / WAITING_FOR_DATA / WAITING_FOR_RESOURCES / RETRYING / RESOLVED / FAILED

class DependencyCheck(Base):
    """Result of each dependency check run"""
    __tablename__ = "dependency_checks"

    id              = Column(Integer, primary_key=True, index=True)
    incident_id     = Column(Integer, index=True)
    dag_id          = Column(String(200))
    check_type      = Column(String(50))    # API / DATABASE / FILE / SYSTEM
    check_target    = Column(String(500))   # URL / proc name / file path / metric
    check_result    = Column(String(20))    # AVAILABLE / UNAVAILABLE / DEGRADED
    response_time_ms = Column(Float)
    details         = Column(Text)
    checked_at      = Column(DateTime, default=datetime.utcnow)

class RetryDecision(Base):
    """Every retry decision made by the agent"""
    __tablename__ = "retry_decisions"

    id              = Column(Integer, primary_key=True, index=True)
    incident_id     = Column(Integer, index=True)
    dag_id          = Column(String(200))
    decision        = Column(String(50))    # RETRY / WAIT / ESCALATE / ABORT
    reason          = Column(Text)          # Why the agent made this decision
    cpu_at_decision = Column(Float)
    mem_at_decision = Column(Float)
    retry_number    = Column(Integer)
    decided_at      = Column(DateTime, default=datetime.utcnow)
    triggered_at    = Column(DateTime, nullable=True)
    outcome         = Column(String(50))    # SUCCESS / FAILED / PENDING

class AgentAction(Base):
    """Full audit log of every action the agent takes"""
    __tablename__ = "agent_actions"

    id          = Column(Integer, primary_key=True, index=True)
    action_type = Column(String(100))   # LOG_SCAN / CLASSIFY / DEP_CHECK / RETRY_TRIGGER / ALERT
    dag_id      = Column(String(200))
    details     = Column(JSON)
    success     = Column(Boolean, default=True)
    error_msg   = Column(Text)
    duration_ms = Column(Integer)
    created_at  = Column(DateTime, default=datetime.utcnow)

class SystemMetricSnapshot(Base):
    """Periodic system resource snapshots for trend analysis"""
    __tablename__ = "system_metrics"

    id          = Column(Integer, primary_key=True, index=True)
    cpu_percent = Column(Float)
    mem_percent = Column(Float)
    disk_percent = Column(Float)
    load_avg_1m = Column(Float)
    load_avg_5m = Column(Float)
    db_response_ms = Column(Float)
    recorded_at = Column(DateTime, default=datetime.utcnow)

class LearningPattern(Base):
    """Patterns the agent learns from historical incidents"""
    __tablename__ = "learning_patterns"

    id              = Column(Integer, primary_key=True, index=True)
    dag_id          = Column(String(200), index=True)
    error_category  = Column(String(100))
    pattern_type    = Column(String(100))   # COMMON_FAILURE_TIME / AVG_WAIT / SUCCESS_RETRY_COUNT
    pattern_value   = Column(Text)          # JSON or string value
    sample_count    = Column(Integer, default=1)
    confidence      = Column(Float, default=0.5)
    updated_at      = Column(DateTime, default=datetime.utcnow)

# ── Utilities ─────────────────────────────────────────────────────

def create_tables():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def seed_demo_incidents():
    """Seed realistic demo incidents so the dashboard shows data"""
    db = SessionLocal()
    try:
        if db.query(DAGFailureIncident).count() > 0:
            return
        from datetime import datetime, timedelta

        incidents = [
            DAGFailureIncident(
                dag_id="daily_sales_pipeline",
                task_id="fetch_api_data",
                run_id="manual__2024-01-15T02:00:00",
                error_category="API_FAILURE",
                error_message="HTTPError: 503 Service Unavailable — Sales API endpoint not responding",
                root_cause="External Sales API experienced downtime from 2:00 AM to 3:15 AM. Data was not yet available at scheduled run time.",
                log_snippet="[2024-01-15 02:01:34] ERROR - requests.exceptions.HTTPError: 503 Server Error\n[2024-01-15 02:01:34] ERROR - Task failed with exception",
                detected_at=datetime.utcnow() - timedelta(hours=6),
                resolved_at=datetime.utcnow() - timedelta(hours=4, minutes=30),
                resolution="RETRIED_SUCCESS",
                retry_count=3,
                total_wait_mins=75,
                status="RESOLVED"
            ),
            DAGFailureIncident(
                dag_id="inventory_sync_dag",
                task_id="run_stored_proc",
                run_id="scheduled__2024-01-15T02:00:00",
                error_category="DATA_NOT_AVAILABLE",
                error_message="Stored procedure sp_get_inventory returned 0 rows — source data not yet populated",
                root_cause="Upstream inventory system completes its EOD process at approximately 3:30 AM. The DAG runs at 2:00 AM before source data is ready.",
                log_snippet="[2024-01-15 02:05:12] WARNING - Stored proc returned empty result set\n[2024-01-15 02:05:12] ERROR - DataValidationError: Expected > 0 rows",
                detected_at=datetime.utcnow() - timedelta(hours=5),
                resolved_at=datetime.utcnow() - timedelta(hours=3),
                resolution="RETRIED_SUCCESS",
                retry_count=2,
                total_wait_mins=90,
                status="RESOLVED"
            ),
            DAGFailureIncident(
                dag_id="finance_report_dag",
                task_id="load_source_file",
                run_id="scheduled__2024-01-15T03:00:00",
                error_category="FILE_NOT_AVAILABLE",
                error_message="FileNotFoundError: /data/finance/daily_extract_20240115.csv not found",
                root_cause="Finance team's batch export job runs at 3:45 AM. Required CSV file not present at DAG start time of 3:00 AM.",
                log_snippet="[2024-01-15 03:00:45] ERROR - FileNotFoundError: /data/finance/daily_extract_20240115.csv\n[2024-01-15 03:00:45] ERROR - No such file or directory",
                detected_at=datetime.utcnow() - timedelta(hours=4),
                resolved_at=datetime.utcnow() - timedelta(hours=2, minutes=15),
                resolution="RETRIED_SUCCESS",
                retry_count=1,
                total_wait_mins=45,
                status="RESOLVED"
            ),
            DAGFailureIncident(
                dag_id="customer_analytics_dag",
                task_id="transform_data",
                run_id="scheduled__2024-01-15T04:00:00",
                error_category="TIMEOUT",
                error_message="OperationalError: connection to server timed out — database under high load",
                root_cause="Database server was under heavy load from concurrent batch jobs. Connection pool exhausted causing timeout on transform task.",
                log_snippet="[2024-01-15 04:12:33] ERROR - sqlalchemy.exc.OperationalError: connection timed out\n[2024-01-15 04:12:33] ERROR - QueuePool limit overflow",
                detected_at=datetime.utcnow() - timedelta(hours=3),
                resolved_at=None,
                resolution=None,
                retry_count=2,
                total_wait_mins=30,
                status="WAITING_FOR_RESOURCES"
            ),
            DAGFailureIncident(
                dag_id="daily_sales_pipeline",
                task_id="fetch_api_data",
                run_id="scheduled__2024-01-14T02:00:00",
                error_category="API_FAILURE",
                error_message="ConnectionError: Max retries exceeded — Sales API connection refused",
                root_cause="Sales API server maintenance window. API returned connection refused errors for 55 minutes.",
                log_snippet="[2024-01-14 02:00:15] ERROR - requests.exceptions.ConnectionError\n[2024-01-14 02:00:15] ERROR - Max retries exceeded with url: /api/sales/daily",
                detected_at=datetime.utcnow() - timedelta(days=1, hours=6),
                resolved_at=datetime.utcnow() - timedelta(days=1, hours=4),
                resolution="RETRIED_SUCCESS",
                retry_count=4,
                total_wait_mins=55,
                status="RESOLVED"
            ),
        ]
        db.add_all(incidents)

        # Add some dependency checks
        dep_checks = [
            DependencyCheck(incident_id=1, dag_id="daily_sales_pipeline",
                check_type="API", check_target="https://sales-api.internal/health",
                check_result="UNAVAILABLE", response_time_ms=30000,
                details="503 Service Unavailable", checked_at=datetime.utcnow() - timedelta(hours=5, minutes=30)),
            DependencyCheck(incident_id=1, dag_id="daily_sales_pipeline",
                check_type="API", check_target="https://sales-api.internal/health",
                check_result="AVAILABLE", response_time_ms=245,
                details="200 OK", checked_at=datetime.utcnow() - timedelta(hours=4, minutes=45)),
            DependencyCheck(incident_id=2, dag_id="inventory_sync_dag",
                check_type="DATABASE", check_target="sp_get_inventory",
                check_result="UNAVAILABLE", response_time_ms=120,
                details="Returned 0 rows", checked_at=datetime.utcnow() - timedelta(hours=4, minutes=30)),
        ]
        db.add_all(dep_checks)

        # Add learning patterns
        patterns = [
            LearningPattern(dag_id="daily_sales_pipeline", error_category="API_FAILURE",
                pattern_type="COMMON_FAILURE_HOUR", pattern_value="2",
                sample_count=8, confidence=0.85),
            LearningPattern(dag_id="daily_sales_pipeline", error_category="API_FAILURE",
                pattern_type="AVG_WAIT_MINUTES", pattern_value="65",
                sample_count=8, confidence=0.78),
            LearningPattern(dag_id="inventory_sync_dag", error_category="DATA_NOT_AVAILABLE",
                pattern_type="AVG_WAIT_MINUTES", pattern_value="90",
                sample_count=5, confidence=0.90),
        ]
        db.add_all(patterns)
        db.commit()
        print("✅ Demo incidents seeded")
    except Exception as e:
        db.rollback()
        print(f"Seed error: {e}")
    finally:
        db.close()
