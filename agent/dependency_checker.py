"""
Dependency Checker
------------------
Actively probes external dependencies after a failure:
  - API endpoint availability
  - Database stored procedure data readiness
  - File existence and size
  - Data freshness checks
"""

import os
import time
from typing import Optional
from pydantic import BaseModel
from loguru import logger

class DependencyCheckResult(BaseModel):
    check_type:      str     # API / DATABASE / FILE / SYSTEM
    target:          str     # URL / proc name / file path
    available:       bool
    response_time_ms: Optional[float] = None
    detail:          str = ""
    checked_at:      str = ""

class DependencyChecker:
    def __init__(self):
        pass

    # ── API Check ─────────────────────────────────────────────────

    def check_api(self, url: str, method: str = "GET",
                  expected_status: int = 200,
                  timeout: int = 15) -> DependencyCheckResult:
        from datetime import datetime
        import httpx
        start = time.time()
        try:
            r = httpx.request(method, url, timeout=timeout)
            elapsed = round((time.time() - start) * 1000, 2)
            available = r.status_code == expected_status
            return DependencyCheckResult(
                check_type="API", target=url,
                available=available,
                response_time_ms=elapsed,
                detail=f"HTTP {r.status_code}",
                checked_at=datetime.utcnow().isoformat()
            )
        except Exception as e:
            elapsed = round((time.time() - start) * 1000, 2)
            return DependencyCheckResult(
                check_type="API", target=url,
                available=False, response_time_ms=elapsed,
                detail=str(e)[:200],
                checked_at=datetime.utcnow().isoformat()
            )

    # ── Database / Stored Procedure Check ─────────────────────────

    def check_stored_procedure(self, db_url: str,
                                proc_name: str,
                                params: list = None,
                                min_rows: int = 1) -> DependencyCheckResult:
        from datetime import datetime
        from sqlalchemy import create_engine, text
        start = time.time()
        try:
            eng = create_engine(db_url, connect_args={"check_same_thread": False})
            param_str = ", ".join([str(p) for p in (params or [])])
            query = f"EXEC {proc_name} {param_str}" if "mssql" in db_url else \
                    f"CALL {proc_name}({param_str})"
            with eng.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
            elapsed = round((time.time() - start) * 1000, 2)
            row_count = len(rows)
            available = row_count >= min_rows
            return DependencyCheckResult(
                check_type="DATABASE", target=proc_name,
                available=available, response_time_ms=elapsed,
                detail=f"Returned {row_count} rows (need >= {min_rows})",
                checked_at=datetime.utcnow().isoformat()
            )
        except Exception as e:
            elapsed = round((time.time() - start) * 1000, 2)
            return DependencyCheckResult(
                check_type="DATABASE", target=proc_name,
                available=False, response_time_ms=elapsed,
                detail=str(e)[:200],
                checked_at=datetime.utcnow().isoformat()
            )

    def check_db_query(self, db_url: str, query: str,
                        min_rows: int = 1) -> DependencyCheckResult:
        """Run an arbitrary SELECT query and check if it returns enough rows"""
        from datetime import datetime
        from sqlalchemy import create_engine, text
        start = time.time()
        try:
            eng = create_engine(db_url, connect_args={"check_same_thread": False})
            with eng.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
            elapsed = round((time.time() - start) * 1000, 2)
            row_count = len(rows)
            available = row_count >= min_rows
            return DependencyCheckResult(
                check_type="DATABASE", target=query[:80],
                available=available, response_time_ms=elapsed,
                detail=f"Query returned {row_count} rows (need >= {min_rows})",
                checked_at=datetime.utcnow().isoformat()
            )
        except Exception as e:
            elapsed = round((time.time() - start) * 1000, 2)
            return DependencyCheckResult(
                check_type="DATABASE", target=query[:80],
                available=False, response_time_ms=elapsed,
                detail=str(e)[:200],
                checked_at=datetime.utcnow().isoformat()
            )

    # ── File Check ────────────────────────────────────────────────

    def check_file(self, file_path: str, min_size_bytes: int = 1) -> DependencyCheckResult:
        from datetime import datetime
        try:
            exists = os.path.exists(file_path)
            if exists:
                size = os.path.getsize(file_path)
                available = size >= min_size_bytes
                detail = f"File exists, size={size:,} bytes"
            else:
                available = False
                detail = "File does not exist"
            return DependencyCheckResult(
                check_type="FILE", target=file_path,
                available=available,
                detail=detail,
                checked_at=datetime.utcnow().isoformat()
            )
        except Exception as e:
            return DependencyCheckResult(
                check_type="FILE", target=file_path,
                available=False, detail=str(e)[:200],
                checked_at=datetime.utcnow().isoformat()
            )

    def check_file_age(self, file_path: str,
                        max_age_minutes: int = 60) -> DependencyCheckResult:
        """Check file exists AND was modified within the last N minutes (data freshness)"""
        from datetime import datetime
        result = self.check_file(file_path)
        if not result.available:
            return result
        try:
            mtime = os.path.getmtime(file_path)
            age_minutes = (time.time() - mtime) / 60
            is_fresh = age_minutes <= max_age_minutes
            result.available = is_fresh
            result.detail += f" | Age: {age_minutes:.1f} min (limit: {max_age_minutes} min)"
        except Exception as e:
            result.detail += f" | Age check failed: {e}"
        return result

    # ── Batch Check ───────────────────────────────────────────────

    def run_checks(self, checks: list[dict]) -> list[DependencyCheckResult]:
        """
        Run multiple dependency checks at once.
        checks = [
          {"type": "API",  "target": "https://api.example.com/health"},
          {"type": "FILE", "target": "/data/input.csv"},
          {"type": "DB",   "target": "sp_get_data", "db_url": "...", "min_rows": 1},
        ]
        """
        results = []
        for check in checks:
            t = check.get("type", "").upper()
            if t == "API":
                results.append(self.check_api(check["target"]))
            elif t == "FILE":
                results.append(self.check_file(
                    check["target"],
                    check.get("min_size_bytes", 1)
                ))
            elif t == "DB":
                if "proc_name" in check:
                    results.append(self.check_stored_procedure(
                        check["db_url"], check["proc_name"],
                        check.get("params", []), check.get("min_rows", 1)
                    ))
                else:
                    results.append(self.check_db_query(
                        check["db_url"], check.get("query", "SELECT 1"),
                        check.get("min_rows", 1)
                    ))
        return results

    def all_satisfied(self, results: list[DependencyCheckResult]) -> bool:
        return all(r.available for r in results)
