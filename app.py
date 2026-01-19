# app.py

import os
from datetime import date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

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

# -----------------------------------------------------------------------------
# PRIORITY BUCKETS (UPDATED)
# -----------------------------------------------------------------------------

PRIORITY_BUCKETS = [
    ("ar40", "AR40 - No order since 40+ days"),
    ("ar28", "AR28 - No order since 28+ days (excluding AR40)"),
    ("ar14", "AR14 - No order since 14+ days (excluding AR40/AR28)"),
    ("ar7", "AR7 - No order since 7+ days (excluding AR40/AR28/AR14)"),
    ("emerging_power_user", "Emerging Power User - 2 orders in last 8 days (proxy)"),
    ("regular_activation", "Regular Activation"),
]

PRIORITY_LABEL_BY_KEY = {k: v for k, v in PRIORITY_BUCKETS}

PRIORITY_COLOR_BY_KEY = {
    "ar40": "#e53935",               # red
    "ar28": "#fb8c00",               # orange
    "ar14": "#fbc02d",               # amber
    "ar7": "#fdd835",                # yellow
    "emerging_power_user": "#43a047",# green
    "regular_activation": "#9e9e9e", # grey
}

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


# -----------------------------------------------------------------------------
# BOARD INIT / REFRESH (FIXED FOR UNIQUE(partner_id, work_date))
# -----------------------------------------------------------------------------

def ensure_today_rows_for_agent(agent_id: str) -> int:
    """
    Safe under uniq_active_work_item_partner (partner_id) WHERE is_active=TRUE:
    - Insert missing TODAY rows as is_active=FALSE (won't violate unique active)
    - Deactivate any currently active rows for these partners (any date, including today)
    - Activate TODAY rows
    """
    q_insert_missing_today_inactive = text(
        """
        INSERT INTO work_item (partner_id, work_date, status, is_active, updated_at, created_at)
        SELECT pam.partner_id, CURRENT_DATE::date, 'to_call', FALSE, NOW(), NOW()
        FROM partner_agent_map pam
        LEFT JOIN work_item wi
          ON wi.partner_id = pam.partner_id
         AND wi.work_date  = CURRENT_DATE::date
        WHERE pam.agent_id = :agent_id
          AND wi.id IS NULL;
        """
    )

    q_deactivate_any_active = text(
        """
        UPDATE work_item wi
        SET is_active = FALSE,
            updated_at = NOW()
        WHERE wi.is_active = TRUE
          AND wi.partner_id IN (
            SELECT partner_id
            FROM partner_agent_map
            WHERE agent_id = :agent_id
          );
        """
    )

    q_activate_today = text(
        """
        UPDATE work_item wi
        SET is_active = TRUE,
            updated_at = NOW()
        WHERE wi.work_date = CURRENT_DATE::date
          AND wi.partner_id IN (
            SELECT partner_id
            FROM partner_agent_map
            WHERE agent_id = :agent_id
          );
        """
    )

    with get_connection() as conn:
        res = conn.execute(q_insert_missing_today_inactive, {"agent_id": agent_id})
        conn.execute(q_deactivate_any_active, {"agent_id": agent_id})
        conn.execute(q_activate_today, {"agent_id": agent_id})
        conn.commit()
        return int(res.rowcount or 0)




def refresh_board_for_agent(agent_id: str) -> int:
    """
    Refresh today without violating uniq_active_work_item_partner:
    - Insert missing TODAY rows as inactive
    - Deactivate any active rows for these partners
    - Reset TODAY status to to_call, set refreshed_at, activate TODAY
    """
    q_insert_missing_today_inactive = text(
        """
        INSERT INTO work_item (partner_id, work_date, status, is_active, updated_at, created_at)
        SELECT pam.partner_id, CURRENT_DATE::date, 'to_call', FALSE, NOW(), NOW()
        FROM partner_agent_map pam
        LEFT JOIN work_item wi
          ON wi.partner_id = pam.partner_id
         AND wi.work_date  = CURRENT_DATE::date
        WHERE pam.agent_id = :agent_id
          AND wi.id IS NULL;
        """
    )

    q_deactivate_any_active = text(
        """
        UPDATE work_item wi
        SET is_active = FALSE,
            updated_at = NOW()
        WHERE wi.is_active = TRUE
          AND wi.partner_id IN (
            SELECT partner_id
            FROM partner_agent_map
            WHERE agent_id = :agent_id
          );
        """
    )

    q_reset_today = text(
        """
        UPDATE work_item wi
        SET status = 'to_call',
            is_active = TRUE,
            refreshed_at = NOW(),
            updated_at = NOW()
        WHERE wi.work_date = CURRENT_DATE::date
          AND wi.partner_id IN (
            SELECT partner_id
            FROM partner_agent_map
            WHERE agent_id = :agent_id
          );
        """
    )

    with get_connection() as conn:
        res = conn.execute(q_insert_missing_today_inactive, {"agent_id": agent_id})
        conn.execute(q_deactivate_any_active, {"agent_id": agent_id})
        conn.execute(q_reset_today, {"agent_id": agent_id})
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
            p.city,
            p.partner_type,
            p.handover_status,
            p.last_order_date,

            COALESCE(pm0.orders, 0)      AS orders_m0,
            COALESCE(pm0.net_revenue, 0) AS rev_m0,

            lam.last_active_month,

            -- ✅ robust activity date:
            -- prefer last_order_date, else approximate from last_active_month as month-end
            COALESCE(
              p.last_order_date::date,
              (lam.last_active_month + interval '1 month - 1 day')::date
            ) AS last_activity_date

          FROM work_item wi
          JOIN partner p ON p.id = wi.partner_id
          JOIN partner_agent_map pam ON pam.partner_id = p.id

          LEFT JOIN partner_monthly_metrics pm0
            ON pm0.partner_id = p.id
           AND pm0.month_date = date_trunc('month', CURRENT_DATE)::date

          LEFT JOIN LATERAL (
            SELECT MAX(pm.month_date)::date AS last_active_month
            FROM partner_monthly_metrics pm
            WHERE pm.partner_id = p.id
              AND COALESCE(pm.orders,0) > 0
          ) lam ON TRUE

          WHERE pam.agent_id = :agent_id
            AND wi.work_date = CURRENT_DATE::date
        )
        SELECT
          *,
          CASE
            -- Emerging Power User (proxy; depends on last_activity_date + MTD orders)
            WHEN last_activity_date IS NOT NULL
              AND last_activity_date >= (CURRENT_DATE - interval '8 days')::date
              AND orders_m0 >= 2
              THEN 'emerging_power_user'

            WHEN last_activity_date IS NOT NULL
              AND last_activity_date <= (CURRENT_DATE - interval '40 days')::date
              THEN 'ar40'

            WHEN last_activity_date IS NOT NULL
              AND last_activity_date <= (CURRENT_DATE - interval '28 days')::date
              THEN 'ar28'

            WHEN last_activity_date IS NOT NULL
              AND last_activity_date <= (CURRENT_DATE - interval '14 days')::date
              THEN 'ar14'

            WHEN last_activity_date IS NOT NULL
              AND last_activity_date <= (CURRENT_DATE - interval '7 days')::date
              THEN 'ar7'

            ELSE 'regular_activation'
          END AS priority_bucket_key,

          CASE
            WHEN last_activity_date IS NOT NULL
              AND last_activity_date >= (CURRENT_DATE - interval '8 days')::date
              AND orders_m0 >= 2
              THEN 50
            WHEN last_activity_date IS NOT NULL AND last_activity_date <= (CURRENT_DATE - interval '40 days')::date THEN 10
            WHEN last_activity_date IS NOT NULL AND last_activity_date <= (CURRENT_DATE - interval '28 days')::date THEN 20
            WHEN last_activity_date IS NOT NULL AND last_activity_date <= (CURRENT_DATE - interval '14 days')::date THEN 30
            WHEN last_activity_date IS NOT NULL AND last_activity_date <= (CURRENT_DATE - interval '7 days')::date  THEN 40
            ELSE 60
          END AS priority_bucket_rank

        FROM base
        ORDER BY
          priority_bucket_rank ASC,
          rev_m0 DESC,
          last_activity_date ASC NULLS LAST,
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
    with get_connection() as conn:
        conn.execute(q, {"status": new_status, "id": work_item_id})
        conn.commit()


def get_user_portfolio(agent_email: str) -> pd.DataFrame:
    q = text(
        """
        SELECT
            p.external_partner_id,
            p.partner_name,
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


# -----------------------------------------------------------------------------
# FEEDBACK DIALOG
# -----------------------------------------------------------------------------

def render_feedback_dialog():
    """
    Show a dialog after status update. Uses st.dialog if available.
    The link opens in a new tab via link_button (if available).
    """
    if not st.session_state.get("show_feedback_dialog"):
        return

    def _close():
        st.session_state.show_feedback_dialog = False
        st.rerun()

    # Streamlit has st.dialog in newer versions; if not, we fallback to a bordered container.
    if hasattr(st, "dialog"):
        with st.dialog("Fill Feedback form"):
            st.write("Please fill feedback for the call.")
            if hasattr(st, "link_button"):
                # Clicking this opens a new tab and triggers a click event in Streamlit
                if st.link_button("Open feedback form", FEEDBACK_FORM_URL):
                    _close()
            else:
                st.markdown(f"[Open feedback form]({FEEDBACK_FORM_URL})", unsafe_allow_html=False)
                if st.button("Close"):
                    _close()
    else:
        # Fallback (no modal)
        with st.container(border=True):
            st.subheader("Fill Feedback form")
            if hasattr(st, "link_button"):
                if st.link_button("Open feedback form", FEEDBACK_FORM_URL):
                    _close()
            else:
                st.markdown(f"[Open feedback form]({FEEDBACK_FORM_URL})", unsafe_allow_html=False)
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

    name_map = {row["name"]: row for _, row in farmers_df.iterrows()}
    selected_name = st.selectbox("User", list(name_map.keys()))

    if selected_name:
        row = name_map[selected_name]
        st.write(f"Email: `{row['email']}` | Role: `{row['role']}`")

    if st.button("Log In"):
        user_row = name_map[selected_name]
        st.session_state.current_user = {
            "id": user_row["id"],
            "name": user_row["name"],
            "email": user_row["email"],
            "role": user_row["role"],
        }
        st.session_state.logged_in = True
        st.session_state.show_portfolio = False
        st.rerun()


# -----------------------------------------------------------------------------
# KANBAN CARD
# -----------------------------------------------------------------------------

def render_account_card(row):
    bucket_key = row.get("priority_bucket_key", "")
    bucket_label = PRIORITY_LABEL_BY_KEY.get(bucket_key, bucket_key or "-")
    bucket_color = PRIORITY_COLOR_BY_KEY.get(bucket_key, "#616161")

    CATEGORY_LABEL_MAP = {
        "ar40": "AR40",
        "ar28": "AR28",
        "ar14": "AR14",
        "ar7": "AR7",
        "emerging_power_user": "Emerging Power User",
        "regular_activation": "Regular Activation",
    }
    category_label = CATEGORY_LABEL_MAP.get(bucket_key, bucket_key.upper() if bucket_key else "-")

    ext_id = row.get("external_partner_id", "-")
    oms_url = f"https://oms.orangehealth.in/partner/{ext_id}"

    created_at = row.get("created_at")
    refreshed_at = row.get("refreshed_at")

    st.markdown(
        f"""
        <div style="border-left: 6px solid {bucket_color}; padding-left: 10px;">
          <div style="display:flex; justify-content:space-between; align-items:center;">
            <div style="font-size: 16px; font-weight: 700;">
              {row['partner_name']}
            </div>
            <div style="
                background:{bucket_color};
                color:white;
                padding:4px 8px;
                border-radius:6px;
                font-size:12px;
                font-weight:700;">
              {category_label}
            </div>
          </div>

          <div style="margin-top:4px;">
            External ID: <code>{ext_id}</code>
          </div>

          <div style="margin-top:6px;">
            Priority Bucket:
            <span style="color:{bucket_color}; font-weight:700;">
              {bucket_label}
            </span>
          </div>

          <div style="margin-top:6px;">
            <a href="{oms_url}" target="_blank" rel="noopener noreferrer">
              OMS Link: {oms_url}
            </a>
          </div>

          <div style="font-size: 12px; opacity: 0.75; margin-top:6px;">
            First Added: <code>{created_at or '-'}</code>
            &nbsp;|&nbsp;
            Last Refresh: <code>{refreshed_at or '-'}</code>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    current_status = row["status"]
    new_status = st.selectbox(
        "Move to...",
        STATUS_KEYS,
        index=STATUS_KEYS.index(current_status) if current_status in STATUS_KEYS else 0,
        key=f"move_{row['work_item_id']}",
        label_visibility="collapsed",
    )

    if new_status != current_status:
        update_work_item_status(row["work_item_id"], new_status)
        st.session_state.show_feedback_dialog = True
        st.rerun()

# -----------------------------------------------------------------------------
# KANBAN BOARD
# -----------------------------------------------------------------------------

def render_board():
    user = st.session_state.current_user
    st.markdown("### Accounts to be Worked")
    st.caption(f"Logged in as **{user['name']}** (`{user['email']}` · {user['role']})")

    # Ensure required columns exist
    try:
        ensure_workitem_columns()
    except Exception as e:
        st.error("Could not ensure required columns exist on `work_item`.")
        st.caption(f"DB message: {e}")
        return

    # Ensure today's rows exist (one per partner per day)
    inserted = ensure_today_rows_for_agent(user["id"])
    if inserted > 0:
        st.success(f"Added {inserted} missing work item(s) to today’s board.")

    c1, c2 = st.columns([1, 2], vertical_alignment="center")
    with c1:
        if st.button("🔄 Refresh board (reset all to To Call)"):
            created = refresh_board_for_agent(user["id"])
            st.success(
                f"Refreshed today’s board. Inserted {created} new row(s) for partners that weren’t on today’s board yet; "
                f"reset statuses to To Call for all."
            )
            st.rerun()

    with c2:
        st.caption(
            "Board is daily: items are keyed by (partner_id, work_date = today). "
            "Refresh resets today’s statuses; it won’t create duplicates for the same day."
        )

    df = fetch_work_items_for_agent(user["id"])
    if df.empty:
        st.info("No work items found for today. Try Refresh.")
        return

    # -----------------------------
    # GLOBAL FILTERS
    # -----------------------------
    st.markdown("#### Filters")
    f1, f2 = st.columns([1, 1])

    with f1:
        bucket_options = [k for k, _ in PRIORITY_BUCKETS]
        selected_buckets = st.multiselect(
            "Priority Bucket",
            options=bucket_options,
            default=[],
            format_func=lambda k: PRIORITY_LABEL_BY_KEY.get(k, k),
            key="filter_priority_buckets",
        )

    with f2:
        external_id_search = st.text_input(
            "External Partner ID (search)",
            value="",
            placeholder="e.g. 3579 (supports partial match)",
            key="filter_external_partner_id",
        )

    filtered_df = df.copy()

    if selected_buckets:
        filtered_df = filtered_df[filtered_df["priority_bucket_key"].isin(selected_buckets)]

    if external_id_search and external_id_search.strip():
        s = external_id_search.strip().lower()
        filtered_df = filtered_df[
            filtered_df["external_partner_id"].astype(str).str.lower().str.contains(s, na=False)
        ]

    st.caption(f"Showing **{len(filtered_df)}** / {len(df)} accounts after filters.")

    status_counts = {s: int((filtered_df["status"] == s).sum()) for s in STATUS_KEYS}

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

    # Render feedback dialog at end of board render so it pops up after rerun
    render_feedback_dialog()


# -----------------------------------------------------------------------------
# UPLOAD DATA (UNCHANGED: note monthly upload DOES NOT auto-map partners)
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


def render_upload_tab():
    st.markdown("### Upload Data")

    # -------------------------------
    # A) Monthly metrics upload
    # -------------------------------
    st.subheader("Upload Monthly Metrics (CSV)")
    st.caption(
        "Upload monthly performance sheet for partners. "
        "Selected month will be saved into `partner_monthly_metrics.month_date` "
        "as the first day of that month. Missing partners will be auto-created.\n\n"
        "Note: This does NOT auto-map partners to your agent. Use the mapping upload below for that."
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
                }
            )

            if "external_partner_id" not in df.columns:
                st.error("Could not find `Partner ID` column to map to external_partner_id.")
            else:
                numeric_cols = ["orders", "gmv", "net_revenue", "rev_per_gmv", "channel_share", "active_days"]
                for c in numeric_cols:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

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
                                            NOW()
                                        )
                                        ON CONFLICT (external_partner_id) DO UPDATE SET
                                            partner_name = EXCLUDED.partner_name,
                                            city         = EXCLUDED.city,
                                            partner_bd   = EXCLUDED.partner_bd,
                                            bd_cat       = EXCLUDED.bd_cat,
                                            partner_type = EXCLUDED.partner_type,
                                            price_list   = EXCLUDED.price_list,
                                            updated_at   = NOW()
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
                                error_rows.append(
                                    {"row_index": int(idx), "external_partner_id": external_id, "error": str(e)}
                                )

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
        "Use this when partners are mapped to you but have no mapping yet. "
        "We will (1) upsert into `partner` by external_partner_id, then (2) create mapping in `partner_agent_map`."
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
                          updated_at
                        )
                        VALUES (
                          :external_partner_id,
                          :partner_name,
                          :city,
                          :phone,
                          :partner_type,
                          :wallet_amount,
                          NOW()
                        )
                        ON CONFLICT (external_partner_id) DO UPDATE SET
                          partner_name = EXCLUDED.partner_name,
                          city         = EXCLUDED.city,
                          phone        = EXCLUDED.phone,
                          partner_type = EXCLUDED.partner_type,
                          wallet_amount= EXCLUDED.wallet_amount,
                          updated_at   = NOW()
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
