import streamlit as st
import psycopg2
import pandas as pd
import redis
import time
from datetime import datetime, timedelta

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

html, body, [data-testid="stApp"] {
    background-color: #0B1120;
    color: #E2E8F0;
    font-family: 'Inter', sans-serif;
}
[data-testid="stSidebar"] {
    background-color: #0F1A2E !important;
    border-right: 1px solid #1E2D45;
}
[data-testid="stSidebar"] * { color: #94A3B8 !important; }
[data-testid="stSidebar"] .stRadio label { font-size: 0.85rem; }
header[data-testid="stHeader"] { background: transparent; }

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
hr { border-color: #1E2D45; }
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
.empty-state {
    text-align: center;
    padding: 3rem;
    color: #334155;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
}
.section-label {
    font-size: 0.68rem;
    color: #475569;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-family: 'Inter', sans-serif;
    margin-bottom: 0.5rem;
}
.refresh-badge {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: #475569;
}
.redis-card {
    background: #111827;
    border: 1px solid #1E2D45;
    border-radius: 10px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 0.8rem;
}
.redis-bar-bg {
    background: #1E2D45;
    border-radius: 4px;
    height: 8px;
    margin-top: 6px;
    overflow: hidden;
}
.redis-bar-fill {
    height: 8px;
    border-radius: 4px;
    background: #00D4FF;
    transition: width 0.3s ease;
}
.redis-bar-warn { background: #F59E0B; }
.redis-bar-crit { background: #EF4444; }
</style>
""", unsafe_allow_html=True)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="inhouse_queue", user="arun.kumar", password="",
    )

@st.cache_resource
def get_redis():
    return redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

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

# ── Redis helpers ─────────────────────────────────────────────────────────────
def redis_rpm(r, model_name):
    """Count dispatches in the last 60s for a model from the sorted set."""
    key = f"rpm:{model_name}"
    now_ms = int(time.time() * 1000)
    window_start_ms = now_ms - 60_000
    try:
        # Prune then count — mirrors Java RpmCounter.count()
        r.zremrangebyscore(key, 0, window_start_ms - 1)
        return r.zcount(key, window_start_ms, now_ms)
    except Exception:
        return 0

def redis_rpm_breakdown(r, model_name):
    """Return RPM counts for last 10s, 30s, 60s windows."""
    key = f"rpm:{model_name}"
    now_ms = int(time.time() * 1000)
    try:
        return {
            "10s":  r.zcount(key, now_ms - 10_000,  now_ms),
            "30s":  r.zcount(key, now_ms - 30_000,  now_ms),
            "60s":  r.zcount(key, now_ms - 60_000,  now_ms),
            "total": r.zcard(key),
        }
    except Exception:
        return {"10s": 0, "30s": 0, "60s": 0, "total": 0}

def redis_all_rpm_keys(r):
    """Return all rpm:<model> keys currently in Redis."""
    try:
        return [k.replace("rpm:", "") for k in r.scan_iter("rpm:*")]
    except Exception:
        return []

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
        "🔴  Redis Live":     "redis_live",
        "⚡  Audit Log":      "audit_log",
        "📋  Queue Requests": "queue_requests",
        "⚙️  Model Configs":  "model_configs",
    }

    selected_label = st.radio(
        "Tables", list(TABLES.keys()), index=0, label_visibility="collapsed",
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

    st.markdown("<hr style='margin: 1.2rem 0;'>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:0.65rem; color:#475569; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.6rem;'>Danger Zone</div>", unsafe_allow_html=True)

    if st.button("🗑  Clear all tables", use_container_width=True, type="secondary"):
        st.session_state["confirm_drop"] = True

    if st.session_state.get("confirm_drop"):
        st.warning("This will delete all rows from audit_log and queue_requests. model_configs will be kept.")
        col_y, col_n = st.columns(2)
        if col_y.button("Yes, delete", use_container_width=True, type="primary"):
            try:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE audit_log, queue_requests RESTART IDENTITY CASCADE;")
                conn.commit()
                st.session_state["confirm_drop"] = False
                st.success("All tables cleared.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed: {e}")
        if col_n.button("Cancel", use_container_width=True):
            st.session_state["confirm_drop"] = False
            st.rerun()

# ── Auto-refresh logic ────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_interval)
    st.cache_data.clear()
    st.rerun()

def fmt_status(val):
    cls = f"pill-{str(val).lower()}"
    return f"<span class='pill {cls}'>{val}</span>"

# ══════════════════════════════════════════════════════════════════════════════
# REDIS LIVE
# ══════════════════════════════════════════════════════════════════════════════
if selected_table == "redis_live":
    st.markdown("""
    <div class='page-eyebrow'>Redis Live</div>
    <div class='page-title'>Real-time RPM counters</div>
    <div class='page-sub'>Sliding-window request counts read directly from Redis sorted sets — updates on every refresh.</div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1.2rem'></div>", unsafe_allow_html=True)

    try:
        r = get_redis()
        r.ping()
        redis_ok = True
    except Exception as e:
        st.error(f"Cannot connect to Redis: {e}")
        redis_ok = False

    if redis_ok:
        # Pull model configs from DB for rpm_limit context
        configs_df = query("SELECT model_name, rpm_limit, is_active FROM model_configs ORDER BY model_name")
        config_map = {}
        if not configs_df.empty:
            for _, row in configs_df.iterrows():
                config_map[row["model_name"]] = {
                    "rpm_limit": int(row["rpm_limit"]),
                    "is_active": bool(row["is_active"]),
                }

        # Discover all models that have rpm keys in Redis (includes models not yet in DB)
        redis_models = set(redis_all_rpm_keys(r))
        db_models    = set(config_map.keys())
        all_models   = sorted(db_models | redis_models)

        if not all_models:
            st.markdown("<div class='empty-state'>No RPM data in Redis yet — send some requests first.</div>",
                        unsafe_allow_html=True)
        else:
            # ── Summary metrics row ───────────────────────────────────────────
            total_rpm = sum(redis_rpm(r, m) for m in all_models)
            active_keys = sum(1 for m in all_models if redis_rpm(r, m) > 0)
            try:
                redis_info = r.info("memory")
                used_mb = round(redis_info.get("used_memory", 0) / 1024 / 1024, 2)
            except Exception:
                used_mb = 0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total RPM (60s)",   f"{total_rpm:,}")
            c2.metric("Active Models",     f"{active_keys}")
            c3.metric("Redis Memory",      f"{used_mb} MB")
            c4.metric("Snapshot Time",     datetime.now().strftime("%H:%M:%S"))

            st.markdown("<div style='margin: 1.4rem 0 0.5rem 0'></div>", unsafe_allow_html=True)
            st.markdown("<div class='section-label'>Per-model RPM breakdown</div>", unsafe_allow_html=True)

            # ── Per-model cards ───────────────────────────────────────────────
            for model in all_models:
                cfg       = config_map.get(model, {})
                rpm_limit = cfg.get("rpm_limit", 0)
                is_active = cfg.get("is_active", False)
                breakdown = redis_rpm_breakdown(r, model)
                rpm_60    = breakdown["60s"]
                rpm_30    = breakdown["30s"]
                rpm_10    = breakdown["10s"]
                total_set = breakdown["total"]

                # Utilisation ratio for the progress bar (0–1)
                ratio = min(rpm_60 / rpm_limit, 1.0) if rpm_limit > 0 else 0.0
                pct   = round(ratio * 100, 1)

                if ratio >= 0.9:
                    bar_class = "redis-bar-crit"
                    pct_color = "#EF4444"
                elif ratio >= 0.7:
                    bar_class = "redis-bar-warn"
                    pct_color = "#F59E0B"
                else:
                    bar_class = ""
                    pct_color = "#00D4FF"

                active_dot  = "<span style='color:#34D399;'>●</span> ACTIVE" if is_active else "<span style='color:#475569;'>●</span> INACTIVE"
                limit_label = f"{rpm_limit} rpm limit" if rpm_limit else "no limit configured"

                if rpm_limit > 0:
                    capacity_bar = f"""
                    <div>
                        <div style="display:flex; justify-content:space-between; margin-bottom:3px;">
                            <span style="font-size:0.62rem; color:#475569; font-family:JetBrains Mono,monospace;">Capacity used</span>
                            <span style="font-size:0.62rem; color:{pct_color}; font-family:JetBrains Mono,monospace; font-weight:600;">{pct}%</span>
                        </div>
                        <div class="redis-bar-bg">
                            <div class="redis-bar-fill {bar_class}" style="width:{pct}%;"></div>
                        </div>
                    </div>"""
                else:
                    capacity_bar = "<div style='font-size:0.65rem; color:#334155; font-family:JetBrains Mono,monospace;'>No RPM limit configured for this model.</div>"

                card_html = (
                    "<div class='redis-card'>"
                    "<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:0.9rem;'>"
                    "<div style='font-family:JetBrains Mono,monospace; font-size:0.95rem; font-weight:500; color:#E2E8F0;'>" + model + "</div>"
                    "<div style='display:flex; gap:1rem; align-items:center;'>"
                    "<span style='font-family:JetBrains Mono,monospace; font-size:0.65rem; color:#475569;'>" + active_dot + "</span>"
                    "<span style='font-family:JetBrains Mono,monospace; font-size:0.7rem; color:#475569;'>" + limit_label + "</span>"
                    "</div></div>"
                    "<div style='display:grid; grid-template-columns:repeat(4,1fr); gap:1rem; margin-bottom:1rem;'>"
                    "<div>"
                    "<div style='font-size:0.62rem; color:#475569; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:2px;'>Last 10s</div>"
                    "<div style='font-family:JetBrains Mono,monospace; font-size:1.3rem; color:#E2E8F0;'>" + str(rpm_10) + "</div>"
                    "</div>"
                    "<div>"
                    "<div style='font-size:0.62rem; color:#475569; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:2px;'>Last 30s</div>"
                    "<div style='font-family:JetBrains Mono,monospace; font-size:1.3rem; color:#E2E8F0;'>" + str(rpm_30) + "</div>"
                    "</div>"
                    "<div>"
                    "<div style='font-size:0.62rem; color:#475569; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:2px;'>Last 60s (RPM)</div>"
                    "<div style='font-family:JetBrains Mono,monospace; font-size:1.3rem; color:" + pct_color + "; font-weight:600;'>" + str(rpm_60) + "</div>"
                    "</div>"
                    "<div>"
                    "<div style='font-size:0.62rem; color:#475569; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:2px;'>Set Size (raw)</div>"
                    "<div style='font-family:JetBrains Mono,monospace; font-size:1.3rem; color:#94A3B8;'>" + str(total_set) + "</div>"
                    "</div>"
                    "</div>"
                    + capacity_bar +
                    "</div>"
                )
                st.markdown(card_html, unsafe_allow_html=True)

            # ── Raw sorted set inspector ──────────────────────────────────────
            st.markdown("<div style='margin-top:1.4rem'></div>", unsafe_allow_html=True)
            st.markdown("<div class='section-label'>Raw sorted set inspector</div>", unsafe_allow_html=True)

            sel_inspect = st.selectbox("Select model to inspect", all_models, key="redis_inspect")
            if sel_inspect:
                key = f"rpm:{sel_inspect}"
                now_ms = int(time.time() * 1000)
                window_start_ms = now_ms - 60_000
                try:
                    entries = r.zrangebyscore(key, window_start_ms, now_ms, withscores=True)
                    if entries:
                        rows = []
                        for member, score in entries:
                            ts_ms = int(score)
                            rows.append({
                                "member":     member,
                                "timestamp":  datetime.fromtimestamp(ts_ms / 1000).strftime("%H:%M:%S.%f")[:-3],
                                "age_ms":     now_ms - ts_ms,
                            })
                        inspect_df = pd.DataFrame(rows)
                        st.dataframe(
                            inspect_df,
                            use_container_width=True,
                            height=300,
                            column_config={
                                "member":    st.column_config.TextColumn("Redis Member", width=280),
                                "timestamp": st.column_config.TextColumn("Dispatched At", width=140),
                                "age_ms":    st.column_config.NumberColumn("Age (ms)", width=100),
                            }
                        )
                        st.markdown(
                            f"<div class='refresh-badge' style='text-align:right'>"
                            f"{len(rows)} entries in last 60s window</div>",
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown("<div class='empty-state'>No entries in the 60s window for this model.</div>",
                                    unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Redis read failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# AUDIT LOG
# ══════════════════════════════════════════════════════════════════════════════
elif selected_table == "audit_log":
    st.markdown("""
    <div class='page-eyebrow'>Audit Log</div>
    <div class='page-title'>All dispatched requests</div>
    <div class='page-sub'>Every request sent to a vLLM model — success, failure, and timing.</div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1.2rem'></div>", unsafe_allow_html=True)

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
    st.markdown("<div class='section-label'>Filters</div>", unsafe_allow_html=True)
    fc1, fc2, fc3, fc4, fc5 = st.columns([2, 1.5, 1.5, 1.5, 1])

    models_df = query("SELECT DISTINCT model_name FROM audit_log ORDER BY model_name")
    model_opts = ["All"] + models_df["model_name"].tolist() if not models_df.empty else ["All"]
    sel_model  = fc1.selectbox("Model", model_opts, key="al_model")
    sel_mode   = fc2.selectbox("Mode",   ["All", "priority", "flex", "batch"], key="al_mode")
    sel_status = fc3.selectbox("Status", ["All", "completed", "failed", "queued", "processing"], key="al_status")
    sel_source = fc4.selectbox("Source", ["All", "immediate", "queue"], key="al_source")
    sel_limit  = fc5.selectbox("Rows",   [50, 100, 250, 500], key="al_limit")

    dr1, dr2 = st.columns(2)
    date_from = dr1.date_input("From", value=datetime.now().date() - timedelta(days=7), key="al_from")
    date_to   = dr2.date_input("To",   value=datetime.now().date(), key="al_to")

    conditions = ["created_at >= %(from)s", "created_at < %(to)s + INTERVAL '1 day'"]
    params = {"from": date_from, "to": date_to, "limit": sel_limit}
    if sel_model  != "All": conditions.append("model_name = %(model)s");  params["model"]  = sel_model
    if sel_mode   != "All": conditions.append("mode::text = %(mode)s");   params["mode"]   = sel_mode
    if sel_status != "All": conditions.append("status::text = %(status)s"); params["status"] = sel_status
    if sel_source != "All": conditions.append("source::text = %(source)s"); params["source"] = sel_source

    where = " AND ".join(conditions)
    df = query(f"""
        SELECT
            request_id, model_name, mode, source, status,
            response_time_ms, error_message,
            llm_response::text AS llm_response,
            dispatched_at, completed_at, created_at
        FROM audit_log
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """, params)

    st.markdown("<div style='margin-top:0.8rem'></div>", unsafe_allow_html=True)
    if df.empty:
        st.markdown("<div class='empty-state'>No records match the current filters.</div>", unsafe_allow_html=True)
    else:
        for col in ["dispatched_at", "completed_at", "created_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
        if "response_time_ms" in df.columns:
            df["response_time_ms"] = df["response_time_ms"].apply(
                lambda x: f"{int(x)} ms" if pd.notna(x) else "-"
            )
        st.dataframe(df, use_container_width=True, height=480,
                     column_config={
                         "request_id":       st.column_config.TextColumn("Request ID", width=280),
                         "model_name":       st.column_config.TextColumn("Model"),
                         "mode":             st.column_config.TextColumn("Mode", width=80),
                         "source":           st.column_config.TextColumn("Source", width=90),
                         "status":           st.column_config.TextColumn("Status", width=100),
                         "response_time_ms": st.column_config.TextColumn("Latency", width=90),
                         "error_message":    st.column_config.TextColumn("Error"),
                         "llm_response":     st.column_config.TextColumn("LLM Response", width=300),
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
        c1.metric("Total",            f"{int(r['total']):,}")
        c2.metric("Queued",           f"{int(r['queued']):,}")
        c3.metric("Processing",       f"{int(r['processing']):,}")
        c4.metric("Completed",        f"{int(r['completed']):,}")
        c5.metric("Failed",           f"{int(r['failed']):,}")
        c6.metric("Future Scheduled", f"{int(r['scheduled_future']):,}")

    st.markdown("<div style='margin: 1.2rem 0 0.5rem 0'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-label'>Filters</div>", unsafe_allow_html=True)

    fc1, fc2, fc3, fc4 = st.columns([2, 1.5, 1.5, 1])
    models_df = query("SELECT DISTINCT model_name FROM queue_requests ORDER BY model_name")
    model_opts = ["All"] + models_df["model_name"].tolist() if not models_df.empty else ["All"]
    sel_model  = fc1.selectbox("Model", model_opts, key="qr_model")
    sel_mode   = fc2.selectbox("Mode",  ["All", "flex", "batch"], key="qr_mode")
    sel_status = fc3.selectbox("Status", ["All", "queued", "processing", "completed", "failed"], key="qr_status")
    sel_limit  = fc4.selectbox("Rows",  [50, 100, 250], key="qr_limit")

    conditions = ["1=1"]
    params = {"limit": sel_limit}
    if sel_model  != "All": conditions.append("model_name = %(model)s");  params["model"]  = sel_model
    if sel_mode   != "All": conditions.append("mode::text = %(mode)s");   params["mode"]   = sel_mode
    if sel_status != "All": conditions.append("status::text = %(status)s"); params["status"] = sel_status

    df = query(f"""
        SELECT id, model_name, mode, priority_weight, status,
               scheduled_at, created_at, updated_at, processed_at,
               error_message, result::text AS result
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
                         "result":           st.column_config.TextColumn("LLM Response", width=300),
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
            metrics_urls, created_at, updated_at
        FROM model_configs
        ORDER BY model_name
    """)

    if df.empty:
        st.markdown("<div class='empty-state'>No model configs found.</div>", unsafe_allow_html=True)
    else:
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
                    Metrics URLs: {', '.join(row["metrics_urls"]) if row["metrics_urls"] else "-"}
                </div>
            </div>
            """, unsafe_allow_html=True)

        with st.expander("Raw table view"):
            st.dataframe(df, use_container_width=True)
