"""
Error Classifier Agent
-----------------------
Analyzes Airflow log content and classifies the root cause.
Uses LLM for deep analysis + rule-based fallback patterns.
"""

import re
import json
from typing import Optional
from pydantic import BaseModel
from loguru import logger

# ── Error Categories ─────────────────────────────────────────────

CATEGORIES = {
    "API_FAILURE":        "External API endpoint not responding or returning errors",
    "DATA_NOT_AVAILABLE": "Required data (DB rows, stored proc results) not yet available",
    "FILE_NOT_AVAILABLE": "Required input file not found or not yet written",
    "TIMEOUT":            "Connection or operation timed out (DB, network, queue)",
    "INFRA":              "Infrastructure or server load issue (OOM, CPU, worker down)",
    "DB_ERROR":           "Database error (connection, query failure, deadlock)",
    "UNKNOWN":            "Could not classify — requires manual investigation",
}

# ── Rule-based Patterns (keyword → category) ─────────────────────

PATTERNS = {
    "API_FAILURE": [
        r"503\s+service\s+unavailable", r"502\s+bad\s+gateway", r"connection\s+refused",
        r"max\s+retries\s+exceeded", r"httperror", r"requests\.exceptions",
        r"api.*not\s+respond", r"endpoint.*unavailable", r"connection\s+error",
        r"connectionerror", r"apiexception",
    ],
    "DATA_NOT_AVAILABLE": [
        r"returned\s+0\s+rows", r"empty\s+result\s+set", r"no\s+data\s+found",
        r"data\s+not\s+(yet\s+)?available", r"source\s+not\s+ready",
        r"datavalidationerror", r"empty.*dataframe", r"no\s+rows\s+to\s+process",
        r"stored.proc.*0\s+rows", r"expected.*>\s*0",
    ],
    "FILE_NOT_AVAILABLE": [
        r"filenotfounderror", r"no\s+such\s+file", r"file.*not\s+found",
        r"path.*does\s+not\s+exist", r"cannot\s+open.*file", r"ioerror.*file",
        r"ossystemerror.*enoent", r"missing.*\.csv", r"missing.*\.parquet",
    ],
    "TIMEOUT": [
        r"connection\s+timed?\s+out", r"read\s+timed?\s+out", r"timeout",
        r"operation\s+timed?\s+out", r"socket\s+timeout", r"gateway\s+timeout",
        r"504", r"request\s+timeout", r"execution\s+timeout",
    ],
    "INFRA": [
        r"out\s+of\s+memory", r"oom", r"worker.*unreachable", r"node.*down",
        r"killed", r"signal\s+9", r"sigkill", r"memory\s+limit\s+exceeded",
        r"disk\s+full", r"no\s+space\s+left", r"airflowexception.*worker",
    ],
    "DB_ERROR": [
        r"operationalerror", r"databaseerror", r"deadlock", r"lock\s+timeout",
        r"too\s+many\s+connections", r"queuepool\s+limit", r"psycopg2",
        r"pymysql", r"sqlalchemy.*error", r"connection\s+pool\s+exhausted",
    ],
}

# ── Result Model ──────────────────────────────────────────────────

class ClassificationResult(BaseModel):
    category:        str
    category_desc:   str
    confidence:      float          # 0.0 – 1.0
    root_cause:      str
    recommended_action: str
    key_evidence:    list[str]      # Log lines that led to classification
    classified_by:   str            # "llm" or "rules"

# ── Classifier ────────────────────────────────────────────────────

class ErrorClassifier:
    def __init__(self, llm=None):
        self.llm = llm

    def classify(self, log_text: str, dag_id: str = "", task_id: str = "") -> ClassificationResult:
        """Main entry point — classify an error from log text"""
        logger.info(f"Classifying error for dag={dag_id} task={task_id}")

        # Try LLM first for richer analysis
        if self.llm:
            try:
                return self._classify_with_llm(log_text, dag_id, task_id)
            except Exception as e:
                logger.warning(f"LLM classification failed: {e} — using rules")

        return self._classify_with_rules(log_text, dag_id, task_id)

    def _classify_with_llm(self, log_text: str, dag_id: str, task_id: str) -> ClassificationResult:
        prompt = f"""You are an Apache Airflow operations expert. Analyze this failed task log and classify the root cause.

DAG: {dag_id}
Task: {task_id}

Log Output (last 100 lines):
{log_text[-4000:]}

Respond with ONLY a valid JSON object — no markdown, no explanation, no extra text:
{{
  "category": "<one of: API_FAILURE | DATA_NOT_AVAILABLE | FILE_NOT_AVAILABLE | TIMEOUT | INFRA | DB_ERROR | UNKNOWN>",
  "confidence": <0.0 to 1.0>,
  "root_cause": "<2-3 sentence explanation of what went wrong and why>",
  "recommended_action": "<what the agent should do: wait for API, check file, wait for DB, etc.>",
  "key_evidence": ["<log line 1>", "<log line 2>", "<log line 3>"]
}}"""

        response = self.llm.invoke(prompt)
        clean = response.strip().lstrip("```json").rstrip("```").strip()
        data = json.loads(clean)

        cat = data.get("category", "UNKNOWN")
        return ClassificationResult(
            category=cat,
            category_desc=CATEGORIES.get(cat, "Unknown error type"),
            confidence=float(data.get("confidence", 0.8)),
            root_cause=data.get("root_cause", ""),
            recommended_action=data.get("recommended_action", ""),
            key_evidence=data.get("key_evidence", []),
            classified_by="llm"
        )

    def _classify_with_rules(self, log_text: str, dag_id: str, task_id: str) -> ClassificationResult:
        log_lower = log_text.lower()
        scores: dict[str, int] = {cat: 0 for cat in PATTERNS}
        evidence: dict[str, list[str]] = {cat: [] for cat in PATTERNS}

        for category, patterns in PATTERNS.items():
            for pattern in patterns:
                matches = re.findall(pattern, log_lower)
                if matches:
                    scores[category] += len(matches)
                    # Find actual log lines containing this match
                    for line in log_text.split("\n"):
                        if re.search(pattern, line, re.IGNORECASE):
                            evidence[category].append(line.strip())

        best_cat = max(scores, key=scores.get)
        best_score = scores[best_cat]

        if best_score == 0:
            best_cat = "UNKNOWN"
            confidence = 0.3
        else:
            total = sum(scores.values())
            confidence = min(0.95, best_score / max(total, 1) + 0.3)

        actions = {
            "API_FAILURE":        "Poll API endpoint every 60s until it responds. Retry when healthy.",
            "DATA_NOT_AVAILABLE": "Check stored procedure / DB query output every 60s. Retry when data available.",
            "FILE_NOT_AVAILABLE": "Watch file path until file appears. Retry when file size > 0.",
            "TIMEOUT":            "Wait for system load to decrease. Retry when CPU < 80% and DB responsive.",
            "INFRA":              "Monitor worker health and system resources. Alert if not resolved in 30 min.",
            "DB_ERROR":           "Check DB connection pool and query performance. Retry when connections available.",
            "UNKNOWN":            "Escalate to on-call engineer — could not auto-classify.",
        }

        root_causes = {
            "API_FAILURE":        f"External API endpoint is unavailable for DAG '{dag_id}'. Service may be down or returning errors.",
            "DATA_NOT_AVAILABLE": f"Required data source has not been populated yet for DAG '{dag_id}'. Upstream process may not have finished.",
            "FILE_NOT_AVAILABLE": f"Required input file is not yet available for DAG '{dag_id}'. File creation process may be delayed.",
            "TIMEOUT":            f"Connection or operation timed out in DAG '{dag_id}'. System may be under high load.",
            "INFRA":              f"Infrastructure issue detected in DAG '{dag_id}'. Worker or system resource problem.",
            "DB_ERROR":           f"Database error in DAG '{dag_id}'. Connection or query problem detected.",
            "UNKNOWN":            f"Could not automatically classify the error in DAG '{dag_id}'. Manual review required.",
        }

        key_evidence = evidence.get(best_cat, [])[:5]  # Top 5 relevant lines

        return ClassificationResult(
            category=best_cat,
            category_desc=CATEGORIES.get(best_cat, ""),
            confidence=round(confidence, 2),
            root_cause=root_causes.get(best_cat, ""),
            recommended_action=actions.get(best_cat, ""),
            key_evidence=key_evidence,
            classified_by="rules"
        )

    def extract_log_snippet(self, log_text: str, max_lines: int = 30) -> str:
        """Extract the most relevant lines from a large log"""
        lines = log_text.split("\n")
        error_lines = []
        for i, line in enumerate(lines):
            if any(kw in line.upper() for kw in ["ERROR", "EXCEPTION", "FAILED", "CRITICAL", "FATAL"]):
                # Include 2 lines before and after for context
                start = max(0, i - 2)
                end = min(len(lines), i + 3)
                error_lines.extend(lines[start:end])

        if not error_lines:
            # Fall back to last N lines
            error_lines = lines[-max_lines:]

        seen = set()
        unique = []
        for line in error_lines:
            if line not in seen:
                seen.add(line)
                unique.append(line)

        return "\n".join(unique[-max_lines:])
