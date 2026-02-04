# app.py

import os
from datetime import date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import IntegrityError, DBAPIError

# -----------------------------------------------------------------------------
# STREAMLIT CONFIG
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="Central Farming Tool",
    page_icon="📊",
    layout="wide",
)

# -----------------------------------------------------------------------------
# DATABASE (SUPABASE SESSION POOLER)
# -----------------------------------------------------------------------------
# ✅ Prefer using env var SUPABASE_DB_URL instead of hardcoding.
DB_URL = os.getenv(
    "SUPABASE_DB_URL",
    (
        "postgresql+psycopg2://"
        "postgres.qglmgzurndrrgvfjbkqn:"
        "Supabase3008"
        "@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
    ),
)

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    connect_args={"sslmode": "require"},
)

# -----------------------------------------------------------------------------
# WORK ITEM STATUS CONFIG (UPDATED)
# -----------------------------------------------------------------------------

STATUS_KEYS = [
    "to_call",
    "rnr_1",
    "rnr_2",
    "rnr_final",
    "follow_up",
    "not_interested",
    "successful_call",
    "escalated",
]

STATUS_LABELS = {
    "to_call": "To Call",
    "rnr_1": "1st Attempt RNR",
    "rnr_2": "2nd Attempt RNR",
    "rnr_final": "Final RNR",
    "follow_up": "Follow up",
    "not_interested": "Not Interested",
    "successful_call": "Successful Call",
    "escalated": "Escalated",
}
RNR_STATUSES = {"rnr_1", "rnr_2", "rnr_final"}
CALL_STATUS_OPTIONS = ["Connected", "Switched OFF", "RNR"]


# -----------------------------------------------------------------------------
# PRIORITY BUCKETS (UPDATED)
# -----------------------------------------------------------------------------

PRIORITY_BUCKETS = [
    ("ar40", "AR40"),
    ("ar28", "AR28"),
    ("ar14", "AR14"),
    ("ar7", "AR7"),
    ("emerging_power_user", "Emerging Power User"),
    ("regular_activation", "Regular Activation"),
]

PRIORITY_LABEL_BY_KEY = {k: v for k, v in PRIORITY_BUCKETS}

PRIORITY_COLOR_BY_KEY = {
    "ar40": "#e53935",                # red
    "ar28": "#fb8c00",                # orange
    "ar14": "#fbc02d",                # amber
    "ar7": "#fdd835",                 # yellow
    "emerging_power_user": "#43a047", # green
    "regular_activation": "#9e9e9e",  # grey
}

# -----------------------------------------------------------------------------
# PARTNER TYPE TAG (NEW)
# -----------------------------------------------------------------------------

PARTNER_TYPE_TAG_OPTIONS = ["Portfolio", "Longtail"]  # stored in partner.partner_type_tag

# -----------------------------------------------------------------------------
# SESSION KEYS
# -----------------------------------------------------------------------------

FEEDBACK_FORM_URL = "https://forms.gle/4QpWEUAdxPobT636A"


def ensure_session():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "current_user" not in st.session_state:
        st.session_state.current_user = None
    if "show_portfolio" not in st.session_state:
        st.session_state.show_portfolio = False
    if "show_feedback_dialog" not in st.session_state:
        st.session_state.show_feedback_dialog = False
    if "pending_status_payload" not in st.session_state:
        st.session_state.pending_status_payload = None
    if "open_status_dialog" not in st.session_state:
        st.session_state.open_status_dialog = False


def on_status_change(work_item_id: str):
    work_item_id = str(work_item_id)

    row = st.session_state.get("_row_lookup", {}).get(work_item_id)
    if not row:
        return  # safe no-op

    selected_status = st.session_state.get(f"status_select_{work_item_id}")
    if not selected_status:
        return

    # If status didn't actually change, do nothing
    if selected_status == row.get("status"):
        return

    st.session_state.pending_status_payload = {
        "work_item_id": work_item_id,
        "partner_id": str(row.get("partner_id")),
        "external_partner_id": row.get("external_partner_id"),
        "partner_name": row.get("partner_name"),
        "agent_id": st.session_state.current_user["id"],
        "agent_name": st.session_state.current_user["name"],
        "status": selected_status,
    }
    st.session_state.open_status_dialog = True




# -----------------------------------------------------------------------------
# DB HELPERS
# -----------------------------------------------------------------------------

def get_connection():
    try:
        return engine.connect()
    except OperationalError as e:
        st.error("DB connection failed. Check Supabase pooler host/username/password.")
        st.exception(e)
        st.stop()


def ensure_workitem_columns():
    """
    Ensure required columns exist without manual migrations.
    - is_active: used to indicate current board row (kept for backward-compat)
    - created_at: first time row created (captures when item entered board for that date)
    - refreshed_at: last time board was refreshed/reset for that date
    """
    q = text(
        """
        ALTER TABLE public.work_item
          ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

        ALTER TABLE public.work_item
          ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

        ALTER TABLE public.work_item
          ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMPTZ;
        """
    )
    with get_connection() as conn:
        conn.execute(q)
        conn.commit()


def ensure_partner_type_tag_column():
    """
    Ensure partner.partner_type_tag exists (Portfolio / Longtail).
    Kept permissive: doesn't force check constraint (you can add in Supabase manually).
    """
    q = text(
        """
        ALTER TABLE public.partner
          ADD COLUMN IF NOT EXISTS partner_type_tag TEXT;
        """
    )
    with get_connection() as conn:
        conn.execute(q)
        conn.commit()

def ensure_activity_log_table():
    q = text("""
    CREATE TABLE IF NOT EXISTS work_item_activity_log (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        work_item_id UUID NOT NULL,
        partner_id UUID NOT NULL,
        external_partner_id TEXT,
        partner_name TEXT,
        agent_id UUID NOT NULL,
        agent_name TEXT,
        status TEXT NOT NULL,
        doctor_sentiment TEXT,
        primary_concern TEXT,
        next_suggested_action TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """)
    with get_connection() as conn:
        conn.execute(q)
        conn.commit()

def reset_work_items_for_agent(agent_id: str) -> int:
    """
    Explicit manual reset:
    - Sets all active work_items for this agent back to 'to_call'
    - Does NOT create rows
    - Does NOT change work_date
    """
    q = text("""
        UPDATE work_item
        SET status = 'to_call',
            refreshed_at = NOW(),
            updated_at = NOW()
        WHERE is_active = TRUE
          AND partner_id IN (
              SELECT partner_id
              FROM partner_agent_map
              WHERE agent_id = :agent_id
          );
    """)

    with get_connection() as conn:
        res = conn.execute(q, {"agent_id": agent_id})
        conn.commit()
        return int(res.rowcount or 0)


# -----------------------------------------------------------------------------
# DATA FETCH
# -----------------------------------------------------------------------------

def fetch_central_farmers() -> pd.DataFrame:
    q = text(
        """
        SELECT id, name, email, role
        FROM app_user
        WHERE role = 'central_farmer'
        ORDER BY name;
        """
    )
    with get_connection() as conn:
        return pd.read_sql(q, conn)


def fetch_work_items_for_agent(agent_id: str) -> pd.DataFrame:
    """
    Priority rules (exclusive, in order):
    - Emerging Power User: >=2 orders in last 8 days (proxy using MTD orders + recent activity)
    - AR40: last activity >= 40 days ago
    - AR28: last activity >= 28 days ago
    - AR14: last activity >= 14 days ago
    - AR7 : last activity >= 7 days ago
    - else Regular Activation
    """
    q = text(
        """
        WITH base AS (
          SELECT
            wi.id AS work_item_id,
            wi.partner_id,
            wi.work_date,
            wi.status,
            wi.reason_to_work,
            wi.is_active,
            wi.created_at,
            wi.refreshed_at,

            p.external_partner_id,
            p.partner_name,
            p.partner_type_tag,   -- NEW
            p.city,
            p.partner_type,
            p.handover_status,
            p.last_order_date,

            COALESCE(pm0.orders, 0)      AS orders_m0,
            COALESCE(pm0.net_revenue, 0) AS rev_m0,
            lf.follow_up_date AS latest_follow_up_date,
            lam.last_active_month,

            COALESCE(
              p.last_order_date::date,
              (lam.last_active_month + interval '1 month - 1 day')::date
            ) AS last_activity_date,

            CASE
              WHEN COALESCE(
                p.last_order_date::date,
                (lam.last_active_month + interval '1 month - 1 day')::date
              ) IS NULL THEN NULL
              ELSE (CURRENT_DATE::date - COALESCE(
                p.last_order_date::date,
                (lam.last_active_month + interval '1 month - 1 day')::date
              ))::int
            END AS days_since_last_activity

          FROM work_item wi
          JOIN partner p ON p.id = wi.partner_id
          JOIN partner_agent_map pam ON pam.partner_id = p.id

          LEFT JOIN partner_monthly_metrics pm0
            ON pm0.partner_id = p.id
           AND pm0.month_date = date_trunc('month', CURRENT_DATE)::date
            LEFT JOIN LATERAL (
                SELECT
                    wal.follow_up_date
                FROM work_item_activity_log wal
                WHERE wal.work_item_id = wi.id
                AND wal.follow_up_date IS NOT NULL
                ORDER BY wal.created_at DESC
                LIMIT 1
            ) lf ON TRUE

          LEFT JOIN LATERAL (
            SELECT MAX(pm.month_date)::date AS last_active_month
            FROM partner_monthly_metrics pm
            WHERE pm.partner_id = p.id
              AND COALESCE(pm.orders,0) > 0
          ) lam ON TRUE

          WHERE pam.agent_id = :agent_id
            AND wi.is_active = TRUE
        )
        SELECT
          *,
          CASE
            WHEN days_since_last_activity IS NOT NULL
              AND days_since_last_activity <= 8
              AND orders_m0 >= 2
              THEN 'emerging_power_user'
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 40 THEN 'ar40'
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 28 THEN 'ar28'
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 14 THEN 'ar14'
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 7  THEN 'ar7'
            ELSE 'regular_activation'
          END AS priority_bucket_key,

          CASE
            WHEN days_since_last_activity IS NOT NULL
              AND days_since_last_activity <= 8
              AND orders_m0 >= 2
              THEN 50
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 40 THEN 10
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 28 THEN 20
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 14 THEN 30
            WHEN days_since_last_activity IS NOT NULL AND days_since_last_activity >= 7  THEN 40
            ELSE 60
          END AS priority_bucket_rank

        FROM base
        ORDER BY
          priority_bucket_rank ASC,
          rev_m0 DESC,
          days_since_last_activity DESC NULLS LAST,
          partner_name;
        """
    )

    with get_connection() as conn:
        return pd.read_sql(q, conn, params={"agent_id": agent_id})


def update_work_item_status(work_item_id: str, new_status: str) -> None:
    q = text(
        """
        UPDATE work_item
        SET status = :status,
            updated_at = NOW()
        WHERE id = :id;
        """
    )

    try:
        with get_connection() as conn:
            res = conn.execute(q, {"status": new_status, "id": work_item_id})
            conn.commit()

            if hasattr(res, "rowcount") and res.rowcount == 0:
                st.error(f"No work_item found for id={work_item_id}. Data may be stale; refresh the page.")
                st.stop()

    except IntegrityError as e:
        st.error("DB integrity error while updating status.")
        st.exception(e)
        st.stop()
    except DBAPIError as e:
        st.error("DB error while updating status.")
        st.exception(e)
        st.stop()


def get_user_portfolio(agent_email: str) -> pd.DataFrame:
    q = text(
        """
        SELECT
            p.external_partner_id,
            p.partner_name,
            p.partner_type_tag,
            p.city,
            p.partner_type,
            COALESCE(last_m.net_revenue, 0) AS last_month_revenue,
            COALESCE(last_m.orders, 0)      AS last_month_orders,
            COALESCE(curr_m.net_revenue, 0) AS mtd_revenue,
            COALESCE(curr_m.orders, 0)      AS mtd_orders
        FROM partner p
        JOIN partner_agent_map pam ON pam.partner_id = p.id
        JOIN app_user au ON au.id = pam.agent_id
        LEFT JOIN partner_monthly_metrics last_m
          ON last_m.partner_id = p.id
         AND last_m.month_date = date_trunc('month', CURRENT_DATE - interval '1 month')::date
        LEFT JOIN partner_monthly_metrics curr_m
          ON curr_m.partner_id = p.id
         AND curr_m.month_date = date_trunc('month', CURRENT_DATE)::date
        WHERE au.email = :email
        ORDER BY p.partner_name;
        """
    )
    with get_connection() as conn:
        return pd.read_sql(q, conn, params={"email": agent_email})


# -----------------------------------------------------------------------------
# FEEDBACK DIALOG
# -----------------------------------------------------------------------------

def render_feedback_dialog():
    if not st.session_state.get("show_feedback_dialog"):
        return

    def _close():
        st.session_state.open_status_dialog = False
        st.session_state.pending_status_payload = None
        st.rerun()

    if hasattr(st, "dialog"):
        with st.dialog("Fill Feedback form"):
            st.write("Please fill feedback for the call.")
            if hasattr(st, "link_button"):
                if st.link_button("Open feedback form", FEEDBACK_FORM_URL):
                    _close()
            else:
                st.markdown(f"[Open feedback form]({FEEDBACK_FORM_URL})")
                if st.button("Close"):
                    _close()
    else:
        with st.container(border=True):
            st.subheader("Fill Feedback form")
            if hasattr(st, "link_button"):
                if st.link_button("Open feedback form", FEEDBACK_FORM_URL):
                    _close()
            else:
                st.markdown(f"[Open feedback form]({FEEDBACK_FORM_URL})")
                if st.button("Close"):
                    _close()


# -----------------------------------------------------------------------------
# LOGIN
# -----------------------------------------------------------------------------

def render_login():
    st.markdown("## Select User")
    st.caption("Choose your name to log in")

    farmers_df = fetch_central_farmers()
    if farmers_df.empty:
        st.error("No central_farmers configured in app_user.")
        return

    name_map = {r["name"]: r for _, r in farmers_df.iterrows()}
    selected_name = st.selectbox("User", list(name_map.keys()))

    if selected_name:
        r = name_map[selected_name]
        st.write(f"Email: `{r['email']}` | Role: `{r['role']}`")

    if st.button("Log In"):
        r = name_map[selected_name]
        st.session_state.current_user = {
            "id": r["id"],
            "name": r["name"],
            "email": r["email"],
            "role": r["role"],
        }
        st.session_state.logged_in = True
        st.session_state.show_portfolio = False
        st.rerun()


# -----------------------------------------------------------------------------
# KANBAN CARD (FIXED: all 'row' usage is inside this function)
# -----------------------------------------------------------------------------

def _fmt_dt(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    try:
        return str(val)[:19].replace("T", " ")
    except Exception:
        return str(val)


def render_account_card(row):
    latest_follow_up_date = row.get("latest_follow_up_date")
    bucket_key = row.get("priority_bucket_key", "regular_activation")
    bucket_label = PRIORITY_LABEL_BY_KEY.get(bucket_key, bucket_key)
    bucket_color = PRIORITY_COLOR_BY_KEY.get(bucket_key, "#616161")

    ext_id = str(row.get("external_partner_id") or "").strip()
    oms_url = f"https://oms.orangehealth.in/partner/{ext_id}" if ext_id else None

    created_at = _fmt_dt(row.get("created_at"))
    refreshed_at = _fmt_dt(row.get("refreshed_at"))

    # Card UI: only name, external id, bucket, OMS link, first added, last refresh
    st.markdown(
        f"""
        <div style="border-left: 6px solid {bucket_color}; padding-left: 10px;">
          <div style="font-size: 16px; font-weight: 700;">{row.get('partner_name','-')}</div>
          <div style="margin-top:2px;">
            <span style="font-weight:600;">External ID:</span>
            <code>{ext_id or '-'}</code>
          </div>
          <div style="margin-top:2px;">
            <span style="font-weight:600;">Category:</span>
            <span style="color:{bucket_color}; font-weight:800;">{bucket_label}</span>
          </div>
        </div>
        """.strip(),
        unsafe_allow_html=True,
    )
    if row["status"] == "follow_up" and latest_follow_up_date:
        st.markdown(
            f"<div style='margin-top:6px; font-size:13px;'>"
            f"🗓️ <b>Follow-up on:</b> "
            f"<code>{latest_follow_up_date}</code>"
            f"</div>",
            unsafe_allow_html=True,
        )

    if oms_url:
        st.markdown(f"[OMS Link: {oms_url}]({oms_url})")

    st.markdown(
        f"<div style='font-size:12px; opacity:0.8;'>First Added: <code>{created_at}</code> · Last Refresh: <code>{refreshed_at}</code></div>",
        unsafe_allow_html=True,
    )
    wid = str(row["work_item_id"])
    current_status = row["status"]
    new_status = st.selectbox(
        "Move to...",
        STATUS_KEYS,
        index=STATUS_KEYS.index(current_status) if current_status in STATUS_KEYS else 0,
        key=f"status_select_{wid}",
        label_visibility="collapsed",
        on_change=on_status_change,
        args=(wid,),
    )





# -----------------------------------------------------------------------------
# KANBAN BOARD
# -----------------------------------------------------------------------------

def render_board():
    user = st.session_state.current_user
    st.markdown("### Accounts to be Worked")
    st.caption(f"Logged in as **{user['name']}** (`{user['email']}` · {user['role']})")

    # Ensure required DB columns exist
    try:
        ensure_workitem_columns()
        ensure_partner_type_tag_column()
    except Exception as e:
        st.error("Could not ensure required columns exist on DB.")
        st.caption(f"DB message: {e}")
        return


    c1, c2 = st.columns([1, 2], vertical_alignment="center")
    with c1:
        if st.button("🔄 Refresh board (reset all to To Call)"):
            updated = reset_work_items_for_agent(user["id"])
            st.success(
                 f"Reset {updated} account(s) back to 'To Call'. "
                 "This does not affect priority or work dates."
            )
            st.rerun()


    with c2:
        st.caption(
            "Board is persistent across days. "
            "Refresh is a manual reset that sets all accounts back to 'To Call'. "
            "Priority is recalculated automatically."
        )


    df = fetch_work_items_for_agent(user["id"])
    if df.empty:
        st.info("No work items found for today. Try Refresh.")
        return

    df = df.copy()
    df["work_item_id"] = df["work_item_id"].astype(str)
    df["partner_id"] = df["partner_id"].astype(str)


    # Used by status dropdown callback
    st.session_state["_row_lookup"] = {
        str(r["work_item_id"]): r.to_dict()
        for _, r in df.iterrows()
    }


    # -----------------------------
    # GLOBAL FILTERS (incl partner type tag)
    # -----------------------------
    st.markdown("#### Filters")
    f1, f2, f3 = st.columns([1, 1, 1])

    with f1:
        bucket_options = [k for k, _ in PRIORITY_BUCKETS]
        selected_buckets = st.multiselect(
            "Category",
            options=bucket_options,
            default=[],
            format_func=lambda k: PRIORITY_LABEL_BY_KEY.get(k, k),
            key="filter_priority_buckets",
        )

    with f2:
        partner_type_tag_filter = st.multiselect(
            "Partner Type",
            options=PARTNER_TYPE_TAG_OPTIONS,
            default=[],
            key="filter_partner_type_tag",
            help="Portfolio / Longtail",
        )

    with f3:
        external_id_search = st.text_input(
            "External Partner ID (search)",
            value="",
            placeholder="e.g. 3579 (supports partial match)",
            key="filter_external_partner_id",
        )

    filtered_df = df.copy()

    if selected_buckets:
        filtered_df = filtered_df[filtered_df["priority_bucket_key"].isin(selected_buckets)]

    if partner_type_tag_filter:
        # partner_type_tag can be null for older partners; those will be excluded unless you select none
        filtered_df = filtered_df[filtered_df["partner_type_tag"].isin(partner_type_tag_filter)]

    if external_id_search and external_id_search.strip():
        s = external_id_search.strip().lower()
        filtered_df = filtered_df[
            filtered_df["external_partner_id"].astype(str).str.lower().str.contains(s, na=False)
        ]

    st.caption(f"Showing **{len(filtered_df)}** / {len(df)} accounts after filters.")

    status_counts = {s: int((filtered_df["status"] == s).sum()) for s in STATUS_KEYS}
    render_status_update_dialog()
    cols = st.columns(len(STATUS_KEYS))
    for col, status in zip(cols, STATUS_KEYS):
        label = STATUS_LABELS.get(status, status)
        with col:
            st.markdown(f"#### {label} ({status_counts.get(status, 0)})")
            subset = filtered_df[filtered_df["status"] == status]
            if subset.empty:
                st.caption("_No accounts in this column_")
            else:
                for _, r in subset.iterrows():
                    with st.container(border=True):
                        render_account_card(r)
    ensure_activity_log_table()
    

# Feedback Popup

def render_status_update_dialog():
    if not st.session_state.get("open_status_dialog"):
        return

    payload = st.session_state.get("pending_status_payload")
    if not payload or not isinstance(payload, dict):
        st.session_state.open_status_dialog = False
        st.session_state.pending_status_payload = None
        return

    def _close():
        st.session_state.open_status_dialog = False
        st.session_state.pending_status_payload = None
        st.rerun()

    def _save(call_status, sentiment, concern, next_action, follow_up_date):
        persist_status_change({
            **payload,
            "call_status": call_status,
            "doctor_sentiment": sentiment,
            "primary_concern": concern,
            "next_suggested_action": next_action,
            "follow_up_date": follow_up_date,
        })
        _close()

    # ----------------------------
    # UI START (orange container)
    # ----------------------------
    with st.container(border=True):
        st.markdown(
            """
            <style>
            .feedback-box {
                border: 2px solid #ff9800;
                border-radius: 10px;
                padding: 16px;
            }
            </style>
            <div class="feedback-box">
            """,
            unsafe_allow_html=True,
        )

        st.subheader("📋 Call Feedback")

        st.text_input("Partner ID", payload["external_partner_id"], disabled=True)
        st.text_input("Doctor Name", payload["partner_name"], disabled=True)
        st.text_input("Agent Name", payload["agent_name"], disabled=True)

        # ----------------------------
        # Call Status (NEW)
        # ----------------------------
        default_call_status = (
            "RNR" if payload["status"] in RNR_STATUSES else None
        )

        call_status = st.selectbox(
            "Call Status *",
            CALL_STATUS_OPTIONS,
            index=CALL_STATUS_OPTIONS.index(default_call_status)
            if default_call_status in CALL_STATUS_OPTIONS
            else 0,
            key="dlg_call_status",
        )

        # ----------------------------
        # Doctor Sentiment
        # ----------------------------
        sentiment = st.radio(
            "Doctor Sentiment *",
            ["Positive", "Neutral", "Negative"],
            horizontal=True,
            key="dlg_sentiment",
        )

        # ----------------------------
        # Mandatory text fields
        # ----------------------------
        concern = st.text_area(
            "Primary Concern *",
            key="dlg_concern",
        )

        next_action = st.text_area(
            "Next Suggested Action *",
            key="dlg_next_action",
        )

        # ----------------------------
        # Optional follow-up date
        # ----------------------------
        follow_up_date = None
        is_follow_up = payload["status"] == "follow_up"

        if is_follow_up:
              follow_up_date = st.date_input(
              "Follow Up Date *",
               key="dlg_follow_up_date",
            )


        # ----------------------------
        # Validation
        # ----------------------------
        is_valid = all([
            call_status,
            sentiment,
            concern and concern.strip(),
            next_action and next_action.strip(),
            (follow_up_date if is_follow_up else True),
        ])



        st.markdown("</div>", unsafe_allow_html=True)

        # ----------------------------
        # Buttons (bigger)
        # ----------------------------
        c1, c2 = st.columns(2)

        with c1:
            st.button(
                "💾 Save",
                key="dlg_save",
                disabled=not is_valid,
                use_container_width=True,
                on_click=lambda: _save(
                    call_status,
                    sentiment,
                    concern,
                    next_action,
                    follow_up_date,
                ),
            )

        with c2:
            st.button(
                "✖ Cancel",
                key="dlg_cancel",
                use_container_width=True,
                on_click=_close,
            )


def persist_status_change(payload):
    with get_connection() as conn:
        conn.execute(
            text("""
            UPDATE work_item
            SET status = :status,
                updated_at = NOW()
            WHERE id = :work_item_id
            """),
            payload,
        )

        conn.execute(
            text("""
            INSERT INTO work_item_activity_log (
                work_item_id,
                partner_id,
                external_partner_id,
                partner_name,
                agent_id,
                agent_name,
                status,
                doctor_sentiment,
                primary_concern,
                next_suggested_action,
                call_status,
                follow_up_date
            )
            VALUES (
                :work_item_id,
                :partner_id,
                :external_partner_id,
                :partner_name,
                :agent_id,
                :agent_name,
                :status,
                :doctor_sentiment,
                :primary_concern,
                :next_suggested_action,
                :call_status,
                :follow_up_date

            )
            """),
            payload,
        )
        conn.commit()

# -----------------------------------------------------------------------------
# UPLOAD DATA
# -----------------------------------------------------------------------------

def _normalize_partner_type(val: str) -> str:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {"at_home", "athome", "at-home", "home", "at home"}:
        return "At-Home"
    if s in {"in_clinic", "inclinic", "in-clinic", "clinic", "in clinic"}:
        return "In Clinic"
    if s in {"eclinic", "e-clinic"}:
        return "eClinic"
    return str(val).strip()


def _normalize_partner_type_tag(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if s in {"portfolio", "p"}:
        return "Portfolio"
    if s in {"longtail", "long tail", "lt", "l"}:
        return "Longtail"
    return None


def render_upload_tab():
    st.markdown("### Upload Data")

    # -------------------------------
    # A) Monthly metrics upload
    # -------------------------------
    st.subheader("Upload Monthly Metrics (CSV)")
    st.caption(
        "Upload monthly performance sheet for partners. "
        "Selected month will be saved into `partner_monthly_metrics.month_date` as the first day of that month. "
        "Missing partners will be auto-created.\n\n"
        "Optional: include a column `Type` (Portfolio/Longtail) to set partner type tag."
    )

    today = date.today()
    default_month_start = date(today.year, today.month, 1)
    month_date = st.date_input(
        "Select month (we'll use the 1st of this month as label)",
        value=default_month_start,
        format="YYYY-MM-DD",
        key="month_metrics_date",
    )
    month_date = month_date.replace(day=1)

    uploaded_file = st.file_uploader("Choose CSV file (monthly metrics)", type=["csv"], key="monthly_metrics_csv")
    if uploaded_file is not None:
        try:
            raw_df = pd.read_csv(uploaded_file)
        except Exception as e:
            st.error(f"Error reading CSV: {e}")
            raw_df = None

        if raw_df is not None:
            st.write("Preview of uploaded data:")
            st.dataframe(raw_df.head(50), use_container_width=True)

            df = raw_df.rename(
                columns={
                    "Partner City": "city",
                    "Partner ID": "external_partner_id",
                    "Partner Name": "partner_name",
                    "Partner Type": "partner_type",
                    "Price List": "price_list",
                    "#Orders": "orders",
                    "GMV": "gmv",
                    "Net Revenue": "net_revenue",
                    "Rev/GMV": "rev_per_gmv",
                    "Channel Share": "channel_share",
                    "Active Days": "active_days",
                    # NEW possible columns for tag
                    "Type": "partner_type_tag",
                    "Partner Segment": "partner_type_tag",
                    "Partner Tag": "partner_type_tag",
                }
            )

            if "external_partner_id" not in df.columns:
                st.error("Could not find `Partner ID` column to map to external_partner_id.")
            else:
                numeric_cols = ["orders", "gmv", "net_revenue", "rev_per_gmv", "channel_share", "active_days"]
                for c in numeric_cols:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                if "partner_type_tag" in df.columns:
                    df["partner_type_tag"] = df["partner_type_tag"].apply(_normalize_partner_type_tag)

                if st.button("Upload & Save Monthly Metrics", key="btn_upload_metrics"):
                    partner_upserts = 0
                    metric_upserts = 0
                    error_rows = []

                    with get_connection() as conn:
                        for idx, row in df.iterrows():
                            external_id = str(row.get("external_partner_id")).strip()
                            if not external_id or external_id.lower() == "nan":
                                continue

                            partner_params = {
                                "external_partner_id": external_id,
                                "partner_name": row.get("partner_name"),
                                "city": row.get("city"),
                                "partner_bd": row.get("partner_bd"),
                                "bd_cat": row.get("bd_cat"),
                                "partner_type": row.get("partner_type"),
                                "price_list": row.get("price_list"),
                                "partner_type_tag": row.get("partner_type_tag"),
                            }

                            try:
                                result = conn.execute(
                                    text(
                                        """
                                        INSERT INTO partner (
                                            external_partner_id,
                                            partner_name,
                                            city,
                                            partner_bd,
                                            bd_cat,
                                            partner_type,
                                            price_list,
                                            partner_type_tag,
                                            updated_at
                                        )
                                        VALUES (
                                            :external_partner_id,
                                            :partner_name,
                                            :city,
                                            :partner_bd,
                                            :bd_cat,
                                            :partner_type,
                                            :price_list,
                                            :partner_type_tag,
                                            NOW()
                                        )
                                        ON CONFLICT (external_partner_id) DO UPDATE SET
                                            partner_name      = EXCLUDED.partner_name,
                                            city              = EXCLUDED.city,
                                            partner_bd        = EXCLUDED.partner_bd,
                                            bd_cat            = EXCLUDED.bd_cat,
                                            partner_type      = EXCLUDED.partner_type,
                                            price_list        = EXCLUDED.price_list,
                                            partner_type_tag  = COALESCE(EXCLUDED.partner_type_tag, partner.partner_type_tag),
                                            updated_at        = NOW()
                                        RETURNING id;
                                        """
                                    ),
                                    partner_params,
                                )
                                partner_id = result.scalar()
                                partner_upserts += 1

                                metric_params = {
                                    "partner_id": partner_id,
                                    "month_date": month_date,
                                    "orders": row.get("orders", 0),
                                    "gmv": row.get("gmv", 0),
                                    "net_revenue": row.get("net_revenue", 0),
                                    "rev_per_gmv": row.get("rev_per_gmv", 0),
                                    "channel_share": row.get("channel_share", 0),
                                    "active_days": row.get("active_days", 0),
                                }

                                conn.execute(
                                    text(
                                        """
                                        INSERT INTO partner_monthly_metrics (
                                            partner_id,
                                            month_date,
                                            orders,
                                            gmv,
                                            net_revenue,
                                            rev_per_gmv,
                                            channel_share,
                                            active_days,
                                            updated_at
                                        )
                                        VALUES (
                                            :partner_id,
                                            :month_date,
                                            :orders,
                                            :gmv,
                                            :net_revenue,
                                            :rev_per_gmv,
                                            :channel_share,
                                            :active_days,
                                            NOW()
                                        )
                                        ON CONFLICT (partner_id, month_date) DO UPDATE SET
                                            orders        = EXCLUDED.orders,
                                            gmv           = EXCLUDED.gmv,
                                            net_revenue   = EXCLUDED.net_revenue,
                                            rev_per_gmv   = EXCLUDED.rev_per_gmv,
                                            channel_share = EXCLUDED.channel_share,
                                            active_days   = EXCLUDED.active_days,
                                            updated_at    = NOW();
                                        """
                                    ),
                                    metric_params,
                                )
                                metric_upserts += 1
                                conn.commit()

                            except Exception as e:
                                conn.rollback()
                                error_rows.append({"row_index": int(idx), "external_partner_id": external_id, "error": str(e)})

                    if metric_upserts > 0:
                        st.success(
                            f"✅ Created/updated {partner_upserts} partners and "
                            f"{metric_upserts} monthly metric rows for {month_date.strftime('%Y-%m')}."
                        )

                    if error_rows:
                        st.warning(f"{len(error_rows)} rows failed. They were skipped; details below.")
                        st.dataframe(pd.DataFrame(error_rows), use_container_width=True)

    st.divider()

    # -------------------------------
    # B) Add/Map partners to logged-in agent
    # -------------------------------
    st.subheader("Add / Map Partners to My Account (CSV)")
    st.caption(
        "We will (1) upsert into `partner` by external_partner_id, then (2) create mapping in `partner_agent_map`.\n\n"
        "Optional: include column `Type` (Portfolio/Longtail) to set partner type tag."
    )

    st.markdown(
        """
        **CSV headers expected (exact or close):**
        - `City`
        - `Partner ID` (maps to `external_partner_id`)
        - `Partner Name`
        - `Phone Num`
        - `Partner Type` (at_home / in_clinic / eclinic or similar)
        - `Wallet Amount`
        - `Type` (Portfolio/Longtail) [optional]
        """
    )

    partner_file = st.file_uploader("Choose CSV file (partners to map)", type=["csv"], key="partner_map_csv")
    if partner_file is None:
        return

    try:
        raw_p = pd.read_csv(partner_file)
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
        return

    st.write("Preview:")
    st.dataframe(raw_p.head(50), use_container_width=True)

    p_df = raw_p.rename(
        columns={
            "Partner ID": "external_partner_id",
            "Partner Name": "partner_name",
            "Phone Num": "phone",
            "Partner Type": "partner_type",
            "Wallet Amount": "wallet_amount",
            "City": "city",
            "Type": "partner_type_tag",
            "Partner Segment": "partner_type_tag",
            "Partner Tag": "partner_type_tag",
        }
    )

    required = ["external_partner_id", "partner_name"]
    missing_req = [c for c in required if c not in p_df.columns]
    if missing_req:
        st.error(f"Missing required columns: {missing_req}.")
        return

    p_df["external_partner_id"] = p_df["external_partner_id"].astype(str).str.strip()
    if "partner_type" in p_df.columns:
        p_df["partner_type"] = p_df["partner_type"].apply(_normalize_partner_type)

    if "wallet_amount" in p_df.columns:
        p_df["wallet_amount"] = pd.to_numeric(p_df["wallet_amount"], errors="coerce").fillna(0)

    if "partner_type_tag" in p_df.columns:
        p_df["partner_type_tag"] = p_df["partner_type_tag"].apply(_normalize_partner_type_tag)

    if st.button("Add / Map Partners to My Account", key="btn_add_map_partners"):
        user = st.session_state.current_user
        mapped = 0
        skipped = 0
        failed = []

        with get_connection() as conn:
            for idx, r in p_df.iterrows():
                ext = str(r.get("external_partner_id", "")).strip()
                if not ext or ext.lower() == "nan":
                    skipped += 1
                    continue

                try:
                    partner_sql = text(
                        """
                        INSERT INTO partner (
                          external_partner_id,
                          partner_name,
                          city,
                          phone,
                          partner_type,
                          wallet_amount,
                          partner_type_tag,
                          updated_at
                        )
                        VALUES (
                          :external_partner_id,
                          :partner_name,
                          :city,
                          :phone,
                          :partner_type,
                          :wallet_amount,
                          :partner_type_tag,
                          NOW()
                        )
                        ON CONFLICT (external_partner_id) DO UPDATE SET
                          partner_name      = EXCLUDED.partner_name,
                          city              = EXCLUDED.city,
                          phone             = EXCLUDED.phone,
                          partner_type      = EXCLUDED.partner_type,
                          wallet_amount     = EXCLUDED.wallet_amount,
                          partner_type_tag  = COALESCE(EXCLUDED.partner_type_tag, partner.partner_type_tag),
                          updated_at        = NOW()
                        RETURNING id;
                        """
                    )

                    partner_params = {
                        "external_partner_id": ext,
                        "partner_name": r.get("partner_name"),
                        "city": r.get("city"),
                        "phone": r.get("phone"),
                        "partner_type": r.get("partner_type"),
                        "wallet_amount": float(r.get("wallet_amount", 0)),
                        "partner_type_tag": r.get("partner_type_tag"),
                    }

                    partner_id = conn.execute(partner_sql, partner_params).scalar()

                    map_sql = text(
                        """
                        INSERT INTO partner_agent_map (partner_id, agent_id)
                        VALUES (:partner_id, :agent_id)
                        ON CONFLICT DO NOTHING;
                        """
                    )
                    map_res = conn.execute(map_sql, {"partner_id": partner_id, "agent_id": user["id"]})
                    mapped += int(map_res.rowcount or 0)

                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    failed.append({"row_index": int(idx), "external_partner_id": ext, "error": str(e)})

        st.success(f"✅ Done. Mappings created: {mapped}, skipped: {skipped}.")
        if failed:
            st.warning(f"{len(failed)} rows failed.")
            st.dataframe(pd.DataFrame(failed), use_container_width=True)

        st.info("These partners will show on today’s board immediately.")


# -----------------------------------------------------------------------------
# PORTFOLIO
# -----------------------------------------------------------------------------

def render_portfolio():
    user = st.session_state.current_user
    st.markdown("### My Portfolio")
    st.caption(f"Summary of all accounts owned by **{user['name']}**")

    if st.button("⬅ Back to Accounts Board"):
        st.session_state.show_portfolio = False
        st.rerun()

    df = get_user_portfolio(user["email"])
    if df.empty:
        st.info("No partners found in your portfolio.")
        return

    numeric_cols = ["last_month_revenue", "last_month_orders", "mtd_revenue", "mtd_orders"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    st.dataframe(df, use_container_width=True, hide_index=True)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    ensure_session()

    with st.sidebar:
        st.markdown("### Session")

        if st.session_state.logged_in:
            u = st.session_state.current_user
            st.write(f"**{u['name']}**")
            st.caption(f"{u['email']} · {u['role']}")

            if st.button("View My Portfolio"):
                st.session_state.show_portfolio = True
                st.rerun()

            if st.button("Logout"):
                st.session_state.logged_in = False
                st.session_state.current_user = None
                st.session_state.show_portfolio = False
                st.session_state.show_feedback_dialog = False
                st.rerun()
        else:
            st.caption("Not logged in")

    st.markdown("# 📊 Central Farming Tool")
    st.error("🚨 PROD BUILD CHECK: 2026-02-04 19:45 🚨")

    if not st.session_state.logged_in:
        render_login()
        return

    if st.session_state.show_portfolio:
        render_portfolio()
        return

    tab_accounts, tab_upload = st.tabs(["Accounts to be Worked", "Upload Data"])

    with tab_accounts:
        render_board()

    with tab_upload:
        render_upload_tab()


if __name__ == "__main__":
    main()
