"""
Resource Monitor
----------------
Monitors system resources and database health.
The agent uses this to decide the optimal retry time.
"""

import time
import statistics
from typing import Optional
from pydantic import BaseModel
from loguru import logger

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available — using simulated metrics")

class ResourceSnapshot(BaseModel):
    cpu_percent:    float
    mem_percent:    float
    disk_percent:   float
    load_avg_1m:    float
    load_avg_5m:    float
    db_response_ms: Optional[float] = None
    is_safe_to_retry: bool = False
    reason:         str = ""
    timestamp:      str = ""

class ResourceMonitor:
    def __init__(self, cpu_threshold: float = 80.0, mem_threshold: float = 85.0):
        self.cpu_threshold = cpu_threshold
        self.mem_threshold = mem_threshold
        self._history: list[ResourceSnapshot] = []

    def snapshot(self, db_url: str = None) -> ResourceSnapshot:
        """Take a full system resource snapshot"""
        from datetime import datetime

        if PSUTIL_AVAILABLE:
            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory().percent
            disk = psutil.disk_usage("/").percent
            try:
                load = psutil.getloadavg()
                load_1m, load_5m = load[0], load[1]
            except AttributeError:
                # Windows doesn't have getloadavg
                load_1m = cpu / 100 * psutil.cpu_count()
                load_5m = load_1m
        else:
            # Simulated for demo
            import random
            cpu  = random.uniform(20, 75)
            mem  = random.uniform(40, 80)
            disk = random.uniform(30, 60)
            load_1m = random.uniform(0.5, 3.0)
            load_5m = random.uniform(0.5, 2.5)

        db_ms = self._check_db_response(db_url) if db_url else None

        # Determine if safe to retry
        reasons = []
        safe = True
        if cpu > self.cpu_threshold:
            safe = False
            reasons.append(f"CPU {cpu:.1f}% > {self.cpu_threshold}%")
        if mem > self.mem_threshold:
            safe = False
            reasons.append(f"Memory {mem:.1f}% > {self.mem_threshold}%")
        if db_ms and db_ms > 5000:
            safe = False
            reasons.append(f"DB response {db_ms:.0f}ms — too slow")

        reason = "All resources within limits" if safe else "Waiting: " + " | ".join(reasons)

        snap = ResourceSnapshot(
            cpu_percent=round(cpu, 1),
            mem_percent=round(mem, 1),
            disk_percent=round(disk, 1),
            load_avg_1m=round(load_1m, 2),
            load_avg_5m=round(load_5m, 2),
            db_response_ms=db_ms,
            is_safe_to_retry=safe,
            reason=reason,
            timestamp=datetime.utcnow().isoformat()
        )

        self._history.append(snap)
        if len(self._history) > 100:
            self._history = self._history[-100:]

        return snap

    def _check_db_response(self, db_url: str) -> Optional[float]:
        """Measure database response time in milliseconds"""
        try:
            from sqlalchemy import create_engine, text
            start = time.time()
            eng = create_engine(db_url, connect_args={"check_same_thread": False})
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
            return round((time.time() - start) * 1000, 2)
        except Exception as e:
            logger.warning(f"DB health check failed: {e}")
            return 9999.0

    def get_trend(self, last_n: int = 5) -> dict:
        """Get resource trend from recent snapshots"""
        recent = self._history[-last_n:]
        if not recent:
            return {"trend": "unknown"}

        cpu_vals = [s.cpu_percent for s in recent]
        mem_vals = [s.mem_percent for s in recent]

        return {
            "cpu_avg":   round(statistics.mean(cpu_vals), 1),
            "cpu_trend": "rising" if cpu_vals[-1] > cpu_vals[0] else "falling",
            "mem_avg":   round(statistics.mean(mem_vals), 1),
            "mem_trend": "rising" if mem_vals[-1] > mem_vals[0] else "falling",
            "snapshots": len(recent),
        }

    def wait_for_resources(self, timeout_seconds: int = 3600,
                            check_interval: int = 30,
                            db_url: str = None) -> bool:
        """
        Block until resources are safe. Returns True when safe, False on timeout.
        In the async supervisor, this is called in a background thread.
        """
        start = time.time()
        while time.time() - start < timeout_seconds:
            snap = self.snapshot(db_url)
            if snap.is_safe_to_retry:
                logger.info(f"✅ Resources safe: CPU={snap.cpu_percent}% MEM={snap.mem_percent}%")
                return True
            logger.info(f"⏳ Resources not safe: {snap.reason}. Waiting {check_interval}s...")
            time.sleep(check_interval)
        logger.warning("⚠️ Timed out waiting for resources to become available")
        return False
