import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import time

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="LLM Queue Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme injection ───────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* Ground */
html, body, [data-testid="stApp"] {
    background-color: #0B1120;
    color: #E2E8F0;
    font-family: 'Inter', sans-serif;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #0F1A2E !important;
    border-right: 1px solid #1E2D45;
}
[data-testid="stSidebar"] * { color: #94A3B8 !important; }
[data-testid="stSidebar"] .stRadio label { font-size: 0.85rem; }

/* Hide default header */
header[data-testid="stHeader"] { background: transparent; }

/* Metric cards */
div[data-testid="metric-container"] {
    background: #111827;
    border: 1px solid #1E2D45;
    border-radius: 8px;
    padding: 1rem 1.2rem;
}
div[data-testid="metric-container"] label {
    color: #64748B !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-family: 'Inter', sans-serif;
}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #00D4FF !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.6rem !important;
}
div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size: 0.75rem !important;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid #1E2D45;
    border-radius: 8px;
    overflow: hidden;
}
.stDataFrame thead tr th {
    background: #0F1A2E !important;
    color: #00D4FF !important;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem;
    letter-spacing: 0.05em;
    border-bottom: 1px solid #1E2D45;
}
.stDataFrame tbody tr td {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    color: #CBD5E1;
    border-bottom: 1px solid #0F1A2E;
}
.stDataFrame tbody tr:hover td { background: #1E2D45 !important; }

/* Inputs */
[data-testid="stSelectbox"] > div,
[data-testid="stTextInput"] > div > div > input,
[data-testid="stDateInput"] input {
    background: #111827 !important;
    border: 1px solid #1E2D45 !important;
    color: #E2E8F0 !important;
    border-radius: 6px;
    font-family: 'Inter', sans-serif;
    font-size: 0.82rem;
}

/* Divider */
hr { border-color: #1E2D45; }

/* Page title */
.page-title {
    font-family: 'Inter', sans-serif;
    font-weight: 600;
    font-size: 1.1rem;
    color: #E2E8F0;
    letter-spacing: -0.01em;
    margin-bottom: 0;
}
.page-eyebrow {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: #00D4FF;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 4px;
}
.page-sub {
    font-size: 0.78rem;
    color: #475569;
    margin-top: 2px;
}

/* Status pills */
.pill {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 0.7rem;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
}
.pill-completed  { background: #064E3B; color: #34D399; }
.pill-failed     { background: #450A0A; color: #FCA5A5; }
.pill-queued     { background: #1C1917; color: #A8A29E; }
.pill-processing { background: #1E3A5F; color: #7DD3FC; }

/* Empty state */
.empty-state {
    text-align: center;
    padding: 3rem;
    color: #334155;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
}

/* Section label */
.section-label {
    font-size: 0.68rem;
    color: #475569;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-family: 'Inter', sans-serif;
    margin-bottom: 0.5rem;
}

/* Refresh badge */
.refresh-badge {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: #475569;
}
</style>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="inhouse_queue",
        user="arun.kumar",
        password="",
    )

def query(sql, params=None):
    try:
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

def execute(sql, params=None):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        st.error(f"Execute failed: {e}")

# ── Sidebar nav ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding: 1rem 0 1.5rem 0;'>
        <div style='font-family: JetBrains Mono, monospace; font-size: 0.65rem;
                    color: #00D4FF; letter-spacing: 0.15em; text-transform: uppercase;
                    margin-bottom: 6px;'>LLM Queue</div>
        <div style='font-size: 1rem; font-weight: 600; color: #E2E8F0;'>Monitor</div>
    </div>
    """, unsafe_allow_html=True)

    TABLES = {
        "⚡  Audit Log":      "audit_log",
        "📋  Queue Requests": "queue_requests",
        "📦  Request Log":    "request_log",
        "⚙️  Model Configs":  "model_configs",
    }

    selected_label = st.radio(
        "Tables",
        list(TABLES.keys()),
        index=0,
        label_visibility="collapsed",
    )
    selected_table = TABLES[selected_label]

    st.markdown("<hr style='margin: 1.2rem 0;'>", unsafe_allow_html=True)

    auto_refresh = st.toggle("Auto-refresh", value=False)
    refresh_interval = st.select_slider(
        "Interval (s)", options=[5, 10, 30, 60], value=10,
        disabled=not auto_refresh,
    )

    st.markdown("<hr style='margin: 1.2rem 0;'>", unsafe_allow_html=True)
    if st.button("↺  Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    now_str = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"<div class='refresh-badge'>Last loaded {now_str}</div>", unsafe_allow_html=True)

# ── Auto-refresh logic ────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_interval)
    st.cache_data.clear()
    st.rerun()

# ── Helper: format status pill ────────────────────────────────────────────────
def fmt_status(val):
    cls = f"pill-{str(val).lower()}"
    return f"<span class='pill {cls}'>{val}</span>"

# ══════════════════════════════════════════════════════════════════════════════
# AUDIT LOG (default)
# ══════════════════════════════════════════════════════════════════════════════
if selected_table == "audit_log":
    st.markdown("""
    <div class='page-eyebrow'>Audit Log</div>
    <div class='page-title'>All dispatched requests</div>
    <div class='page-sub'>Every request sent to a vLLM model — success, failure, and timing.</div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1.2rem'></div>", unsafe_allow_html=True)

    # ── Top stats ─────────────────────────────────────────────────────────────
    stats = query("""
        SELECT
            COUNT(*)                                             AS total,
            COUNT(*) FILTER (WHERE status = 'completed')        AS completed,
            COUNT(*) FILTER (WHERE status = 'failed')           AS failed,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 hour') AS last_hour,
            ROUND(AVG(response_time_ms))                        AS avg_ms,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms)) AS p95_ms
        FROM audit_log
    """)

    if not stats.empty:
        r = stats.iloc[0]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Total Requests",  f"{int(r['total']):,}")
        c2.metric("Completed",       f"{int(r['completed']):,}")
        c3.metric("Failed",          f"{int(r['failed']):,}",
                  delta=f"{round(int(r['failed'])/max(int(r['total']),1)*100,1)}% failure rate",
                  delta_color="inverse")
        c4.metric("Last Hour",       f"{int(r['last_hour']):,}")
        c5.metric("Avg Response",    f"{int(r['avg_ms'] or 0)} ms")
        c6.metric("P95 Response",    f"{int(r['p95_ms'] or 0)} ms")

    st.markdown("<div style='margin: 1.2rem 0 0.5rem 0'></div>", unsafe_allow_html=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    st.markdown("<div class='section-label'>Filters</div>", unsafe_allow_html=True)
    fc1, fc2, fc3, fc4, fc5 = st.columns([2, 1.5, 1.5, 1.5, 1])

    models_df = query("SELECT DISTINCT model_name FROM audit_log ORDER BY model_name")
    model_opts = ["All"] + models_df["model_name"].tolist() if not models_df.empty else ["All"]
    sel_model  = fc1.selectbox("Model", model_opts, key="al_model")
    sel_mode   = fc2.selectbox("Mode",   ["All", "priority", "flex", "batch"], key="al_mode")
    sel_status = fc3.selectbox("Status", ["All", "completed", "failed", "queued", "processing"], key="al_status")
    sel_source = fc4.selectbox("Source", ["All", "immediate", "queue"], key="al_source")
    sel_limit  = fc5.selectbox("Rows",   [50, 100, 250, 500], key="al_limit")

    # date range
    dr1, dr2 = st.columns(2)
    date_from = dr1.date_input("From", value=datetime.now().date() - timedelta(days=7), key="al_from")
    date_to   = dr2.date_input("To",   value=datetime.now().date(), key="al_to")

    # ── Build query ───────────────────────────────────────────────────────────
    conditions = ["created_at >= %(from)s", "created_at < %(to)s + INTERVAL '1 day'"]
    params = {"from": date_from, "to": date_to, "limit": sel_limit}
    if sel_model  != "All": conditions.append("model_name = %(model)s");  params["model"]  = sel_model
    if sel_mode   != "All": conditions.append("mode::text = %(mode)s");   params["mode"]   = sel_mode
    if sel_status != "All": conditions.append("status::text = %(status)s"); params["status"] = sel_status
    if sel_source != "All": conditions.append("source::text = %(source)s"); params["source"] = sel_source

    where = " AND ".join(conditions)
    df = query(f"""
        SELECT
            request_id,
            model_name,
            mode,
            source,
            status,
            response_time_ms,
            error_message,
            dispatched_at,
            completed_at,
            created_at
        FROM audit_log
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """, params)

    st.markdown("<div style='margin-top:0.8rem'></div>", unsafe_allow_html=True)

    if df.empty:
        st.markdown("<div class='empty-state'>No records match the current filters.</div>", unsafe_allow_html=True)
    else:
        # Colour-code status column
        def style_row(row):
            colors = {
                "completed":  "#064E3B",
                "failed":     "#450A0A",
                "queued":     "#1C1917",
                "processing": "#1E3A5F",
            }
            bg = colors.get(str(row.get("status", "")).lower(), "")
            return [f"background-color: {bg}15" if bg else "" for _ in row]

        styled = df.style.apply(style_row, axis=1).format({
            "response_time_ms": lambda x: f"{int(x)} ms" if pd.notna(x) else "—",
            "dispatched_at":    lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(x) else "—",
            "completed_at":     lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(x) else "—",
            "created_at":       lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(x) else "—",
        })

        st.dataframe(styled, use_container_width=True, height=480,
                     column_config={
                         "request_id":       st.column_config.TextColumn("Request ID", width=280),
                         "model_name":       st.column_config.TextColumn("Model"),
                         "mode":             st.column_config.TextColumn("Mode", width=80),
                         "source":           st.column_config.TextColumn("Source", width=90),
                         "status":           st.column_config.TextColumn("Status", width=100),
                         "response_time_ms": st.column_config.TextColumn("Latency", width=90),
                         "error_message":    st.column_config.TextColumn("Error"),
                         "dispatched_at":    st.column_config.TextColumn("Dispatched"),
                         "completed_at":     st.column_config.TextColumn("Completed"),
                         "created_at":       st.column_config.TextColumn("Created"),
                     })
        st.markdown(f"<div class='refresh-badge' style='text-align:right'>{len(df):,} rows</div>",
                    unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# QUEUE REQUESTS
# ══════════════════════════════════════════════════════════════════════════════
elif selected_table == "queue_requests":
    st.markdown("""
    <div class='page-eyebrow'>Queue Requests</div>
    <div class='page-title'>Pending & processed queue items</div>
    <div class='page-sub'>Flex and batch requests that were held in the queue before dispatch.</div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1.2rem'></div>", unsafe_allow_html=True)

    stats = query("""
        SELECT
            COUNT(*)                                                    AS total,
            COUNT(*) FILTER (WHERE status = 'queued')                   AS queued,
            COUNT(*) FILTER (WHERE status = 'processing')               AS processing,
            COUNT(*) FILTER (WHERE status = 'completed')                AS completed,
            COUNT(*) FILTER (WHERE status = 'failed')                   AS failed,
            COUNT(*) FILTER (WHERE scheduled_at IS NOT NULL AND scheduled_at > NOW()) AS scheduled_future
        FROM queue_requests
    """)
    if not stats.empty:
        r = stats.iloc[0]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Total",           f"{int(r['total']):,}")
        c2.metric("Queued",          f"{int(r['queued']):,}")
        c3.metric("Processing",      f"{int(r['processing']):,}")
        c4.metric("Completed",       f"{int(r['completed']):,}")
        c5.metric("Failed",          f"{int(r['failed']):,}")
        c6.metric("Future Scheduled",f"{int(r['scheduled_future']):,}")

    st.markdown("<div style='margin: 1.2rem 0 0.5rem 0'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-label'>Filters</div>", unsafe_allow_html=True)

    fc1, fc2, fc3, fc4 = st.columns([2, 1.5, 1.5, 1])
    models_df = query("SELECT DISTINCT model_name FROM queue_requests ORDER BY model_name")
    model_opts = ["All"] + models_df["model_name"].tolist() if not models_df.empty else ["All"]
    sel_model  = fc1.selectbox("Model", model_opts, key="qr_model")
    sel_mode   = fc2.selectbox("Mode",  ["All", "flex", "batch"], key="qr_mode")
    sel_status = fc3.selectbox("Status",["All", "queued", "processing", "completed", "failed"], key="qr_status")
    sel_limit  = fc4.selectbox("Rows",  [50, 100, 250], key="qr_limit")

    conditions = ["1=1"]
    params = {"limit": sel_limit}
    if sel_model  != "All": conditions.append("model_name = %(model)s");  params["model"]  = sel_model
    if sel_mode   != "All": conditions.append("mode::text = %(mode)s");   params["mode"]   = sel_mode
    if sel_status != "All": conditions.append("status::text = %(status)s"); params["status"] = sel_status

    df = query(f"""
        SELECT id, model_name, mode, priority_weight, status,
               scheduled_at, created_at, updated_at, processed_at, error_message
        FROM queue_requests
        WHERE {" AND ".join(conditions)}
        ORDER BY COALESCE(scheduled_at, created_at) DESC
        LIMIT %(limit)s
    """, params)

    st.markdown("<div style='margin-top:0.8rem'></div>", unsafe_allow_html=True)
    if df.empty:
        st.markdown("<div class='empty-state'>No records match the current filters.</div>", unsafe_allow_html=True)
    else:
        st.dataframe(df, use_container_width=True, height=480,
                     column_config={
                         "id":               st.column_config.TextColumn("Request ID", width=280),
                         "model_name":       st.column_config.TextColumn("Model"),
                         "mode":             st.column_config.TextColumn("Mode", width=80),
                         "priority_weight":  st.column_config.NumberColumn("Priority", width=80),
                         "status":           st.column_config.TextColumn("Status", width=100),
                         "scheduled_at":     st.column_config.DatetimeColumn("Scheduled At"),
                         "created_at":       st.column_config.DatetimeColumn("Created"),
                         "updated_at":       st.column_config.DatetimeColumn("Updated"),
                         "processed_at":     st.column_config.DatetimeColumn("Processed"),
                         "error_message":    st.column_config.TextColumn("Error"),
                     })
        st.markdown(f"<div class='refresh-badge' style='text-align:right'>{len(df):,} rows</div>",
                    unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# REQUEST LOG
# ══════════════════════════════════════════════════════════════════════════════
elif selected_table == "request_log":
    st.markdown("""
    <div class='page-eyebrow'>Request Log</div>
    <div class='page-title'>Sliding-window RPM tracker</div>
    <div class='page-sub'>Lightweight log used for RPM counting — pruned every 2 minutes.</div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1.2rem'></div>", unsafe_allow_html=True)

    stats = query("""
        SELECT
            COUNT(*)                                                              AS total,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 minute')     AS last_1m,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '5 minutes')    AS last_5m,
            COUNT(*) FILTER (WHERE mode = 'priority')                             AS priority,
            COUNT(*) FILTER (WHERE mode = 'flex')                                 AS flex,
            COUNT(*) FILTER (WHERE mode = 'batch')                                AS batch
        FROM request_log
    """)
    if not stats.empty:
        r = stats.iloc[0]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Total (in table)", f"{int(r['total']):,}")
        c2.metric("Last 1 min",       f"{int(r['last_1m']):,}")
        c3.metric("Last 5 min",       f"{int(r['last_5m']):,}")
        c4.metric("Priority",         f"{int(r['priority']):,}")
        c5.metric("Flex",             f"{int(r['flex']):,}")
        c6.metric("Batch",            f"{int(r['batch']):,}")

    st.markdown("<div style='margin: 1.2rem 0 0.5rem 0'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-label'>Filters</div>", unsafe_allow_html=True)

    fc1, fc2, fc3 = st.columns([2, 2, 1])
    models_df = query("SELECT DISTINCT model_name FROM request_log ORDER BY model_name")
    model_opts = ["All"] + models_df["model_name"].tolist() if not models_df.empty else ["All"]
    sel_model = fc1.selectbox("Model", model_opts, key="rl_model")
    sel_mode  = fc2.selectbox("Mode",  ["All", "priority", "flex", "batch"], key="rl_mode")
    sel_limit = fc3.selectbox("Rows",  [100, 250, 500], key="rl_limit")

    conditions = ["1=1"]
    params = {"limit": sel_limit}
    if sel_model != "All": conditions.append("model_name = %(model)s"); params["model"] = sel_model
    if sel_mode  != "All": conditions.append("mode::text = %(mode)s");  params["mode"]  = sel_mode

    df = query(f"""
        SELECT id, model_name, mode, created_at
        FROM request_log
        WHERE {" AND ".join(conditions)}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """, params)

    st.markdown("<div style='margin-top:0.8rem'></div>", unsafe_allow_html=True)
    if df.empty:
        st.markdown("<div class='empty-state'>No records found.</div>", unsafe_allow_html=True)
    else:
        st.dataframe(df, use_container_width=True, height=480,
                     column_config={
                         "id":         st.column_config.NumberColumn("ID", width=80),
                         "model_name": st.column_config.TextColumn("Model"),
                         "mode":       st.column_config.TextColumn("Mode", width=100),
                         "created_at": st.column_config.DatetimeColumn("Created At"),
                     })
        st.markdown(f"<div class='refresh-badge' style='text-align:right'>{len(df):,} rows</div>",
                    unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# MODEL CONFIGS
# ══════════════════════════════════════════════════════════════════════════════
elif selected_table == "model_configs":
    st.markdown("""
    <div class='page-eyebrow'>Model Configs</div>
    <div class='page-title'>vLLM model configuration</div>
    <div class='page-sub'>Thresholds, limits, and metrics URLs for each registered model.</div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1.2rem'></div>", unsafe_allow_html=True)

    df = query("""
        SELECT
            model_name, is_active, rpm_limit,
            batch_threshold_pct, max_concurrent_requests,
            p95_threshold_seconds, load_score_threshold,
            metrics_urls,
            created_at, updated_at
        FROM model_configs
        ORDER BY model_name
    """)

    if df.empty:
        st.markdown("<div class='empty-state'>No model configs found.</div>", unsafe_allow_html=True)
    else:
        # Summary cards per model
        for _, row in df.iterrows():
            active_color = "#00D4FF" if row["is_active"] else "#475569"
            active_label = "ACTIVE" if row["is_active"] else "INACTIVE"
            st.markdown(f"""
            <div style='background:#111827; border:1px solid #1E2D45; border-radius:10px;
                        padding:1.2rem 1.4rem; margin-bottom:0.8rem;'>
                <div style='display:flex; justify-content:space-between; align-items:center;
                            margin-bottom:0.8rem;'>
                    <div style='font-family:JetBrains Mono,monospace; font-size:1rem;
                                font-weight:500; color:#E2E8F0;'>{row["model_name"]}</div>
                    <div style='font-family:JetBrains Mono,monospace; font-size:0.65rem;
                                color:{active_color}; letter-spacing:0.1em;
                                border:1px solid {active_color}33; padding:2px 10px;
                                border-radius:999px;'>{active_label}</div>
                </div>
                <div style='display:grid; grid-template-columns:repeat(5,1fr); gap:0.8rem;'>
                    <div>
                        <div style='font-size:0.65rem; color:#475569; text-transform:uppercase;
                                    letter-spacing:0.08em; margin-bottom:2px;'>RPM Limit</div>
                        <div style='font-family:JetBrains Mono,monospace; color:#00D4FF;
                                    font-size:1.1rem;'>{int(row["rpm_limit"])}</div>
                    </div>
                    <div>
                        <div style='font-size:0.65rem; color:#475569; text-transform:uppercase;
                                    letter-spacing:0.08em; margin-bottom:2px;'>Max Concurrent</div>
                        <div style='font-family:JetBrains Mono,monospace; color:#00D4FF;
                                    font-size:1.1rem;'>{int(row["max_concurrent_requests"])}</div>
                    </div>
                    <div>
                        <div style='font-size:0.65rem; color:#475569; text-transform:uppercase;
                                    letter-spacing:0.08em; margin-bottom:2px;'>P95 Threshold</div>
                        <div style='font-family:JetBrains Mono,monospace; color:#F59E0B;
                                    font-size:1.1rem;'>{row["p95_threshold_seconds"]}s</div>
                    </div>
                    <div>
                        <div style='font-size:0.65rem; color:#475569; text-transform:uppercase;
                                    letter-spacing:0.08em; margin-bottom:2px;'>Load Threshold</div>
                        <div style='font-family:JetBrains Mono,monospace; color:#F59E0B;
                                    font-size:1.1rem;'>{row["load_score_threshold"]}</div>
                    </div>
                    <div>
                        <div style='font-size:0.65rem; color:#475569; text-transform:uppercase;
                                    letter-spacing:0.08em; margin-bottom:2px;'>Batch Threshold</div>
                        <div style='font-family:JetBrains Mono,monospace; color:#94A3B8;
                                    font-size:1.1rem;'>{row["batch_threshold_pct"]}</div>
                    </div>
                </div>
                <div style='margin-top:0.8rem; font-size:0.7rem; color:#334155;
                            font-family:JetBrains Mono,monospace;'>
                    Metrics URLs: {', '.join(row["metrics_urls"]) if row["metrics_urls"] else "—"}
                </div>
            </div>
            """, unsafe_allow_html=True)

        with st.expander("Raw table view"):
            st.dataframe(df, use_container_width=True)
