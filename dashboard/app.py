"""
Airflow AI Agent — Streamlit Dashboard
----------------------------------------
Run: streamlit run dashboard/app.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import httpx
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import time

API = "http://localhost:8000"

st.set_page_config(
    page_title="Airflow AI Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
  .status-resolved   { color:#166534; font-weight:700; }
  .status-failed     { color:#991b1b; font-weight:700; }
  .status-waiting    { color:#92400e; font-weight:700; }
  .status-retrying   { color:#1e40af; font-weight:700; }
  .status-invest     { color:#5b21b6; font-weight:700; }
  .agent-card {
    background:#f0f9ff; border-left:4px solid #0ea5e9;
    padding:12px 16px; border-radius:6px; margin:6px 0;
  }
  .metric-up   { color:#dc2626; }
  .metric-down { color:#16a34a; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/bot.png", width=56)
    st.title("Airflow AI Agent")
    st.caption("Intelligent Workflow Supervisor")
    st.divider()

    page = st.selectbox("📌 Navigate", [
        "🏠 Overview",
        "🚨 Incidents",
        "🔍 Incident Detail",
        "🔗 Dependency Checks",
        "📊 System Resources",
        "🧠 Learning Patterns",
        "🧪 Simulate Failure",
        "📋 Agent Actions",
        "🩺 System Health",
    ])

    st.divider()
    # Live status in sidebar
    try:
        h = httpx.get(f"{API}/health", timeout=2).json()
        sup = h.get("supervisor", {})
        st.success("🟢 Agent Online")
        mode = "🧠 LLM" if sup.get("llm_mode") == "llm" else "📋 Rules"
        st.caption(f"Mode: {mode}")
        air = "🟢" if h.get("airflow_connected") else "🟡"
        st.caption(f"Airflow: {air}")
        st.caption(f"Active incidents: {sup.get('active_incidents', 0)}")
    except Exception:
        st.error("🔴 API Offline")
        st.caption("Start: uvicorn main:app --port 8000")

# ── Helpers ───────────────────────────────────────────────────────

def api_get(path, timeout=10):
    try:
        r = httpx.get(f"{API}{path}", timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

def api_post(path, json=None, timeout=120):
    try:
        r = httpx.post(f"{API}{path}", json=json, timeout=timeout)
        return r.json(), r.status_code
    except Exception as e:
        return {"error": str(e)}, 500

STATUS_ICONS = {
    "RESOLVED":               "✅",
    "FAILED":                 "❌",
    "INVESTIGATING":          "🔎",
    "WAITING_FOR_DATA":       "⏳",
    "WAITING_FOR_RESOURCES":  "⚙️",
    "RETRYING":               "🔄",
}
CATEGORY_ICONS = {
    "API_FAILURE":        "🌐",
    "DATA_NOT_AVAILABLE": "📊",
    "FILE_NOT_AVAILABLE": "📁",
    "TIMEOUT":            "⏱️",
    "INFRA":              "🖥️",
    "DB_ERROR":           "🗄️",
    "UNKNOWN":            "❓",
}

def status_pill(s):
    return f"{STATUS_ICONS.get(s,'•')} {s}"

def category_pill(c):
    return f"{CATEGORY_ICONS.get(c,'•')} {c}"

# ══════════════════════════════════════════════════════════════════
# OVERVIEW
# ══════════════════════════════════════════════════════════════════
if page == "🏠 Overview":
    st.title("🤖 Airflow AI Agent — Overview")

    status = api_get("/api/status")
    metrics = api_get("/api/metrics/latest")
    incidents = api_get("/api/incidents?limit=100")

    # KPI row
    if status:
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total Incidents",   status.get("total_incidents",0))
        c2.metric("✅ Resolved",        status.get("resolved_incidents",0))
        c3.metric("🔴 Active",          status.get("active_incidents",0),
                  delta_color="inverse")
        if metrics:
            c4.metric("CPU",  f"{metrics.get('cpu_percent',0):.1f}%")
            c5.metric("Memory", f"{metrics.get('mem_percent',0):.1f}%")

    st.divider()
    col_l, col_r = st.columns([3,2])

    with col_l:
        st.subheader("Recent Incidents")
        if incidents:
            df = pd.DataFrame(incidents)[[
                "id","dag_id","task_id","error_category","status","retry_count","detected_at"
            ]].head(10)
            df["status"]         = df["status"].apply(status_pill)
            df["error_category"] = df["error_category"].apply(category_pill)
            df.columns = ["ID","DAG","Task","Category","Status","Retries","Detected"]
            st.dataframe(df, use_container_width=True, hide_index=True)

    with col_r:
        st.subheader("Failure Categories")
        if incidents:
            cat_counts = pd.Series([i["error_category"] for i in incidents]).value_counts()
            fig = px.pie(values=cat_counts.values, names=cat_counts.index,
                         color_discrete_sequence=px.colors.qualitative.Bold,
                         hole=0.4)
            fig.update_layout(height=280, margin=dict(t=10,b=10,l=10,r=10),
                               showlegend=True, legend=dict(orientation="v"))
            st.plotly_chart(fig, use_container_width=True)

    # Resource sparklines
    st.subheader("📈 Resource History")
    hist = api_get("/api/metrics/history?limit=30")
    if hist:
        df_h = pd.DataFrame(hist)
        col_a, col_b = st.columns(2)
        with col_a:
            fig = go.Figure()
            fig.add_trace(go.Scatter(y=df_h["cpu"], mode="lines+markers",
                          line=dict(color="#ef4444", width=2), name="CPU %"))
            fig.update_layout(title="CPU %", height=200,
                              margin=dict(t=30,b=10,l=10,r=10),
                              yaxis=dict(range=[0,100]))
            st.plotly_chart(fig, use_container_width=True)
        with col_b:
            fig = go.Figure()
            fig.add_trace(go.Scatter(y=df_h["mem"], mode="lines+markers",
                          line=dict(color="#3b82f6", width=2), name="Mem %"))
            fig.update_layout(title="Memory %", height=200,
                              margin=dict(t=30,b=10,l=10,r=10),
                              yaxis=dict(range=[0,100]))
            st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════
# INCIDENTS
# ══════════════════════════════════════════════════════════════════
elif page == "🚨 Incidents":
    st.title("🚨 All Incidents")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        status_filter = st.selectbox("Status", ["All","RESOLVED","FAILED",
            "INVESTIGATING","WAITING_FOR_DATA","WAITING_FOR_RESOURCES","RETRYING"])
    with col_f2:
        dag_filter = st.text_input("DAG ID filter")
    with col_f3:
        if st.button("🔄 Refresh"):
            st.rerun()

    url = "/api/incidents?limit=100"
    if status_filter != "All":
        url += f"&status={status_filter}"
    if dag_filter:
        url += f"&dag_id={dag_filter}"
    incidents = api_get(url)

    if incidents:
        for inc in incidents:
            stat   = inc["status"]
            icon   = STATUS_ICONS.get(stat, "•")
            categ  = inc["error_category"]
            caticon= CATEGORY_ICONS.get(categ, "•")

            bg = {"RESOLVED":"#f0fdf4","FAILED":"#fef2f2","RETRYING":"#eff6ff",
                  "WAITING_FOR_DATA":"#fefce8","INVESTIGATING":"#faf5ff"}.get(stat,"#f9fafb")

            with st.expander(
                f"{icon} Incident #{inc['id']} | {inc['dag_id']} → {inc['task_id']} | {caticon} {categ} | {stat}",
                expanded=(stat not in ("RESOLVED","FAILED"))
            ):
                c1,c2,c3,c4 = st.columns(4)
                c1.markdown(f"**Status:** {status_pill(stat)}")
                c2.markdown(f"**Retries:** {inc['retry_count']}")
                c3.markdown(f"**Wait:** {inc.get('total_wait_mins',0):.0f} min")
                c4.markdown(f"**Resolution:** {inc.get('resolution') or '—'}")

                if inc.get("root_cause"):
                    st.info(f"**Root Cause:** {inc['root_cause']}")
                if inc.get("error_message"):
                    st.code(inc["error_message"][:400], language="text")
    else:
        st.success("No incidents found for the selected filter.")

# ══════════════════════════════════════════════════════════════════
# INCIDENT DETAIL
# ══════════════════════════════════════════════════════════════════
elif page == "🔍 Incident Detail":
    st.title("🔍 Incident Detail")
    incident_id = st.number_input("Incident ID", min_value=1, value=1, step=1)

    if st.button("Load Incident"):
        inc = api_get(f"/api/incidents/{incident_id}")
        if inc:
            st.markdown(f"## {STATUS_ICONS.get(inc['status'],'•')} {inc['dag_id']} / {inc['task_id']}")
            c1,c2,c3 = st.columns(3)
            c1.metric("Status",       inc["status"])
            c2.metric("Error Type",   inc["error_category"])
            c3.metric("Retry Count",  inc["retry_count"])
            c1.metric("Total Wait",   f"{inc.get('total_wait_mins',0):.0f} min")
            c2.metric("Resolution",   inc.get("resolution") or "Ongoing")
            c3.metric("Detected At",  inc["detected_at"][:16])

            if inc.get("root_cause"):
                st.subheader("🧠 AI Root Cause Analysis")
                st.info(inc["root_cause"])

            tabs = st.tabs(["📋 Error Log", "🔗 Dependency Checks", "📋 Retry Decisions"])

            with tabs[0]:
                st.code(inc.get("error_message","No log captured")[:1000], language="text")

            with tabs[1]:
                dep_checks = inc.get("dependency_checks", [])
                if dep_checks:
                    df = pd.DataFrame(dep_checks)
                    df["result"] = df["result"].apply(
                        lambda r: f"✅ {r}" if r=="AVAILABLE" else f"❌ {r}"
                    )
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No dependency checks recorded yet.")

            with tabs[2]:
                decisions = inc.get("retry_decisions", [])
                if decisions:
                    for d in decisions:
                        icon = {"RETRY":"🔄","WAIT":"⏳","ESCALATE":"🚨","ABORT":"🛑"}.get(d["decision"],"•")
                        st.markdown(f"{icon} **Retry #{d['retry_number']}** → **{d['decision']}**")
                        st.caption(f"CPU: {d.get('cpu',0):.1f}% | Mem: {d.get('mem',0):.1f}% | {d.get('decided_at','')[:16]}")
                        st.caption(d.get("reason",""))
                        st.divider()
                else:
                    st.info("No retry decisions recorded yet.")
        else:
            st.error(f"Incident #{incident_id} not found")

# ══════════════════════════════════════════════════════════════════
# DEPENDENCY CHECKS
# ══════════════════════════════════════════════════════════════════
elif page == "🔗 Dependency Checks":
    st.title("🔗 Dependency Check History")

    checks = api_get("/api/dependency-checks?limit=100")
    if checks:
        df = pd.DataFrame(checks)
        df["result"] = df["result"].apply(
            lambda r: f"✅ AVAILABLE" if r=="AVAILABLE" else f"❌ UNAVAILABLE"
        )
        st.dataframe(df[["id","dag_id","check_type","target","result",
                          "response_ms","detail","checked_at"]],
                     use_container_width=True, hide_index=True)

        avail_count = sum(1 for c in checks if c["result"]=="AVAILABLE")
        c1,c2,c3 = st.columns(3)
        c1.metric("Total Checks", len(checks))
        c2.metric("Available",    avail_count)
        c3.metric("Unavailable",  len(checks) - avail_count)
    else:
        st.info("No dependency checks yet. Simulate a failure to generate checks.")

# ══════════════════════════════════════════════════════════════════
# SYSTEM RESOURCES
# ══════════════════════════════════════════════════════════════════
elif page == "📊 System Resources":
    st.title("📊 System Resources")

    auto_refresh = st.checkbox("Auto-refresh every 10s", value=False)

    metrics = api_get("/api/metrics/latest")
    hist    = api_get("/api/metrics/history?limit=60")

    if metrics:
        c1,c2,c3,c4 = st.columns(4)
        cpu = metrics["cpu_percent"]
        mem = metrics["mem_percent"]
        c1.metric("CPU",         f"{cpu:.1f}%",    delta_color="inverse")
        c2.metric("Memory",      f"{mem:.1f}%",    delta_color="inverse")
        c3.metric("Disk",        f"{metrics.get('disk_percent',0):.1f}%")
        c4.metric("Load Avg 1m", f"{metrics.get('load_avg_1m',0):.2f}")

        # Gauges
        fig = go.Figure()
        fig.add_trace(go.Indicator(mode="gauge+number",
            value=cpu, title={"text":"CPU %"},
            gauge={"axis":{"range":[0,100]},
                   "bar":{"color":"#ef4444"},
                   "steps":[{"range":[0,60],"color":"#bbf7d0"},
                             {"range":[60,80],"color":"#fef08a"},
                             {"range":[80,100],"color":"#fecaca"}]},
            domain={"x":[0,0.45],"y":[0,1]}))
        fig.add_trace(go.Indicator(mode="gauge+number",
            value=mem, title={"text":"Memory %"},
            gauge={"axis":{"range":[0,100]},
                   "bar":{"color":"#3b82f6"},
                   "steps":[{"range":[0,60],"color":"#bfdbfe"},
                             {"range":[60,85],"color":"#fef08a"},
                             {"range":[85,100],"color":"#fecaca"}]},
            domain={"x":[0.55,1],"y":[0,1]}))
        fig.update_layout(height=280, margin=dict(t=30,b=10,l=20,r=20))
        st.plotly_chart(fig, use_container_width=True)

    if hist:
        df_h = pd.DataFrame(hist)
        fig = go.Figure()
        fig.add_trace(go.Scatter(y=df_h["cpu"], name="CPU %",
                      line=dict(color="#ef4444",width=2), mode="lines"))
        fig.add_trace(go.Scatter(y=df_h["mem"], name="Mem %",
                      line=dict(color="#3b82f6",width=2), mode="lines"))
        fig.add_hline(y=80, line_dash="dash", line_color="orange",
                      annotation_text="CPU Threshold 80%")
        fig.add_hline(y=85, line_dash="dash", line_color="red",
                      annotation_text="Mem Threshold 85%")
        fig.update_layout(title="Resource History", height=300,
                          margin=dict(t=40,b=10,l=10,r=10),
                          yaxis=dict(range=[0,100]))
        st.plotly_chart(fig, use_container_width=True)

    if auto_refresh:
        time.sleep(10)
        st.rerun()

# ══════════════════════════════════════════════════════════════════
# LEARNING PATTERNS
# ══════════════════════════════════════════════════════════════════
elif page == "🧠 Learning Patterns":
    st.title("🧠 Agent Learning Patterns")

    st.info("""
    The AI agent continuously learns from resolved incidents.
    For each DAG + error type combination, it learns:
    - **Average wait time** before the issue resolves itself
    - **Common failure hours** (time of day)
    - **Success retry count** (how many retries typically needed)
    
    Higher confidence = more historical data = smarter future decisions.
    """)

    patterns = api_get("/api/patterns")
    if patterns:
        df = pd.DataFrame(patterns)
        df["confidence_pct"] = (df["confidence"] * 100).round(1).astype(str) + "%"
        df["confidence_bar"] = df["confidence"].apply(
            lambda c: "🟢" if c > 0.7 else ("🟡" if c > 0.4 else "🔴")
        )
        st.dataframe(
            df[["dag_id","error_category","pattern_type","pattern_value",
                "sample_count","confidence_pct","confidence_bar","updated_at"]],
            use_container_width=True, hide_index=True
        )

        # Confidence chart
        if len(patterns) > 0:
            fig = px.bar(df, x="dag_id", y="confidence", color="error_category",
                         title="Pattern Confidence by DAG",
                         color_discrete_sequence=px.colors.qualitative.Bold,
                         barmode="group")
            fig.update_layout(height=280, margin=dict(t=40,b=10,l=10,r=10),
                               yaxis=dict(range=[0,1]))
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No patterns learned yet. Patterns appear after the agent resolves incidents.")

# ══════════════════════════════════════════════════════════════════
# SIMULATE FAILURE
# ══════════════════════════════════════════════════════════════════
elif page == "🧪 Simulate Failure":
    st.title("🧪 Simulate DAG Failure & Recovery")

    st.info("""
    This page lets you simulate a DAG failure and watch the AI agent:
    1. **Detect** the failure from logs
    2. **Classify** the root cause (API failure, missing data, file, timeout, etc.)
    3. **Check dependencies** repeatedly until they're available
    4. **Monitor resources** and wait for optimal conditions
    5. **Automatically trigger** the DAG retry
    6. **Learn** from the outcome for future incidents
    """)

    col_l, col_r = st.columns(2)
    with col_l:
        dag_id   = st.text_input("DAG ID",   value="daily_sales_pipeline")
        task_id  = st.text_input("Task ID",  value="fetch_api_data")
    with col_r:
        error_type = st.selectbox("Error Type", [
            "API_FAILURE",
            "DATA_NOT_AVAILABLE",
            "FILE_NOT_AVAILABLE",
            "TIMEOUT",
            "INFRA",
        ], format_func=lambda x: {
            "API_FAILURE":        "🌐 API endpoint not responding",
            "DATA_NOT_AVAILABLE": "📊 Stored proc / DB returned empty",
            "FILE_NOT_AVAILABLE": "📁 Required file not found",
            "TIMEOUT":            "⏱️ Connection / operation timeout",
            "INFRA":              "🖥️ Infrastructure / server load issue",
        }.get(x, x))

    st.markdown("---")
    st.subheader("📋 What will happen:")
    descriptions = {
        "API_FAILURE":        "Agent detects 503 error → polls API every 60s → retries DAG when API responds",
        "DATA_NOT_AVAILABLE": "Agent detects empty stored proc result → polls DB every 2 min → retries when rows > 0",
        "FILE_NOT_AVAILABLE": "Agent detects missing file → watches directory every 60s → retries when file appears",
        "TIMEOUT":            "Agent detects timeout → monitors system load → retries when CPU/Mem below thresholds",
        "INFRA":              "Agent detects infra failure → monitors resources → alerts if not resolved in 30 min",
    }
    st.info(descriptions.get(error_type, ""))

    if st.button("🚨 Simulate Failure + Start Recovery", type="primary"):
        with st.spinner("Starting failure simulation..."):
            result, code = api_post("/api/simulate", json={
                "dag_id": dag_id,
                "task_id": task_id,
                "error_type": error_type
            })

        if code == 200:
            st.success(f"✅ Incident #{result['incident_id']} created — recovery loop started!")

            c1,c2,c3 = st.columns(3)
            c1.metric("Incident ID", f"#{result['incident_id']}")
            c2.metric("Category",    result["classification"]["category"])
            c3.metric("Confidence",  f"{result['classification']['confidence']*100:.0f}%")

            st.subheader("🧠 AI Classification")
            cls = result["classification"]
            st.info(f"**Root Cause:** {cls['root_cause']}")
            st.markdown(f"**Recommended Action:** {cls['recommended_action']}")

            if cls.get("key_evidence"):
                with st.expander("Key Evidence from Logs"):
                    for ev in cls["key_evidence"]:
                        st.code(ev, language="text")

            st.markdown("---")
            st.markdown("**🔄 Recovery is running in the background.** Check the **Incidents** page to monitor progress in real-time.")
            st.markdown(f"→ Navigate to **🔍 Incident Detail** and enter ID **{result['incident_id']}**")
        else:
            st.error(f"Error: {result}")

# ══════════════════════════════════════════════════════════════════
# AGENT ACTIONS
# ══════════════════════════════════════════════════════════════════
elif page == "📋 Agent Actions":
    st.title("📋 Agent Action Audit Log")

    st.info("Full audit trail of every action the AI agent performs.")
    if st.button("🔄 Refresh"):
        st.rerun()

    actions = api_get("/api/actions?limit=100")
    if actions:
        action_icons = {
            "CLASSIFY":      "🏷️",
            "SIMULATE":      "🧪",
            "RETRY_TRIGGER": "🔄",
            "DEP_CHECK":     "🔗",
            "LOG_SCAN":      "🔍",
            "ALERT":         "🚨",
        }
        for act in actions[:50]:
            icon = action_icons.get(act["action_type"], "•")
            ok   = "✅" if act["success"] else "❌"
            ms   = f"{act['duration_ms']}ms" if act.get("duration_ms") else ""
            st.markdown(
                f"{ok} {icon} **{act['action_type']}** — `{act['dag_id'] or 'system'}` {ms}"
            )
            if act.get("details"):
                st.caption(str(act["details"])[:200])
            st.caption(act["created_at"][:16])
    else:
        st.info("No actions logged yet.")

# ══════════════════════════════════════════════════════════════════
# SYSTEM HEALTH
# ══════════════════════════════════════════════════════════════════
elif page == "🩺 System Health":
    st.title("🩺 System Health")

    health = api_get("/health")
    if health:
        st.success("✅ FastAPI Backend — Online")
        sup = health.get("supervisor", {})

        c1,c2,c3 = st.columns(3)
        c1.metric("Supervisor", "🟢 Running" if sup.get("supervisor_running") else "🔴 Stopped")
        c2.metric("LLM Mode",  "🧠 LLM Active" if sup.get("llm_mode") == "llm" else "📋 Rule-based")
        c3.metric("Airflow",   "🟢 Connected" if health.get("airflow_connected") else "🟡 Not Connected")

        ollama = health.get("ollama", {})
        st.divider()
        if ollama.get("status") == "online":
            st.success(f"✅ Ollama — Online")
            for m in ollama.get("models", []):
                st.code(m)
        else:
            st.warning("⚠️ Ollama Offline — Agent running in rule-based mode (still fully functional)")
            st.markdown("""
**Start Ollama for AI-powered analysis:**
```powershell
ollama pull llama3.1:8b-instruct-q4_K_M
ollama serve
```
            """)

        st.divider()
        st.subheader("Quick Start Commands")
        st.code("""# Terminal 1 — Start the AI Agent backend
cd airflow_ai_agent
.venv\\Scripts\\Activate.ps1
uvicorn main:app --reload --port 8000

# Terminal 2 — Start the Dashboard
streamlit run dashboard/app.py

# Terminal 3 — (Optional) Start Ollama for LLM mode
ollama serve""", language="bash")

    else:
        st.error("❌ Backend offline. Start with: uvicorn main:app --port 8000")
