"""
Dashboard Tab — TTV table, metrics, and chart.
"""
from datetime import date, datetime

import streamlit as st
import pandas as pd

from services.ttv_service import compute_ttv_table, load_cached_ttv_table, compute_summary


def _days_between(left, right):
    """Days from `left` to `right` (right − left). Returns '-' if either missing/invalid."""
    n = _days_between_int(left, right)
    return n if n is not None else "-"


def _days_between_int(left, right):
    """Integer version — returns None instead of '-' when missing/invalid."""
    if not left or not right:
        return None
    try:
        d0 = datetime.strptime(str(left)[:10], "%Y-%m-%d")
        d1 = datetime.strptime(str(right)[:10], "%Y-%m-%d")
        return (d1 - d0).days
    except ValueError:
        return None


def _date_with_days_since_close(date_val, days_val):
    """Format '2026-04-16 (15d)' — milestone date + days elapsed from close."""
    if not date_val:
        return "-"
    if days_val is None:
        return str(date_val)
    return f"{date_val} ({days_val}d)"


# "Stuck" thresholds — days at a stage before we flag it.
# Tunable: today's averages are ~63d to 10 contacts, ~71d to 50.
STUCK_DAYS = {
    "awaiting_pm": 60,   # days post-close with no PM kickoff
    "in_setup": 90,      # days post-PM-kickoff with no 10 contacts
    "ramping": 60,       # days at 10 contacts without hitting 50
}


def _reached(row, n):
    """True if bot has reached `n` unique contacts.

    Primary signal: `date_{n}` is set (milestone date from BigQuery).
    Fallback: live `total_unique_contacts >= n` — covers stale milestone
    cache (e.g. bot just crossed 50 between ingests).
    """
    if row.get(f"date_{n}"):
        return True
    try:
        return int(row.get("total_unique_contacts") or 0) >= n
    except (TypeError, ValueError):
        return False


def _compute_status(row):
    """Return a single status string combining stage + health overlay.

    Stage (last milestone reached): Tech Assist → Awaiting Account Executive → In Setup →
    Ramping (10) → Implemented (50) → Scaling (100).
    Health overlays (take precedence): Stuck (too long at stage), Late (past
    Expected Go Live and not Implemented).
    """
    # No WhatsApp flow linked → we can't measure contact milestones, so
    # Stuck/Ramping/etc. would be misleading. Surface the real blocker.
    if not row.get("bot_id"):
        return "🔗 Flow not linked"

    today = date.today()

    close = row.get("close_date") or None
    ta_start = (row.get("tech_assist_start") or "")[:10] or None
    pm = row.get("pm_start") or None
    d10 = row.get("date_10") or None
    d50 = row.get("date_50") or None
    expected_gl = row.get("expected_go_live_pm") or None

    # Keep d10/d50 for the "stuck" time-in-stage calculations below

    # Determine stage — highest milestone reached (use contact-count fallback
    # so a stale milestone cache doesn't hide an already-Implemented account)
    if _reached(row, 50):
        stage = "🟢 Implemented"
        stage_key = "implemented"
    elif _reached(row, 10):
        stage = "🟡 Ramping"
        stage_key = "ramping"
    elif pm:
        stage = "🔵 In Setup"
        stage_key = "in_setup"
    elif close:
        stage = "⏳ Awaiting Account Executive"
        stage_key = "awaiting_pm"
    elif ta_start:
        stage = "🛠️ Tech Assist"
        stage_key = "tech_assist"
    else:
        return "—"

    # Stuck overlay — time at current stage without progressing
    today_str = today.isoformat()
    stuck = False
    if stage_key == "awaiting_pm":
        days_at = _days_between_int(close, today_str)
        stuck = days_at is not None and days_at > STUCK_DAYS["awaiting_pm"]
    elif stage_key == "in_setup":
        days_at = _days_between_int(pm, today_str)
        stuck = days_at is not None and days_at > STUCK_DAYS["in_setup"]
    elif stage_key == "ramping":
        days_at = _days_between_int(d10, today_str)
        stuck = days_at is not None and days_at > STUCK_DAYS["ramping"]

    if stuck:
        return f"🔴 Stuck: {stage.split(' ', 1)[1]}"

    # Late overlay — past Expected Go Live without reaching Implemented (50)
    if expected_gl and not _reached(row, 50):
        overdue = _days_between_int(expected_gl, today_str)
        if overdue is not None and overdue > 0:
            return f"⚠️ Late: {stage.split(' ', 1)[1]}"

    return stage


from services import supabase_client as sb
from config.settings import MILESTONES


@st.dialog("Edit Account")
def _edit_account_dialog(row, ttv_rows):
    """Modal to edit Opportunity name (→ Salesforce) and Bot ID (→ Supabase)."""
    from services.salesforce_service import update_opportunity_name as _sf_update_opp
    from services.mapping_service import (
        save_mapping, clear_mapping,
        update_opportunity_name as _local_update_opp,
    )

    opp_id = row.get("opportunity_id")
    st.markdown(f"**{row['account_name']}**")
    if row.get("sf_url"):
        st.caption(f"[Open in Salesforce]({row['sf_url']})")

    original_opp = row.get("opportunity_name") or ""
    new_opp = st.text_input(
        "Opportunity Name",
        value=original_opp,
        help="Writes back to Salesforce Opportunity.Name",
    )

    # Load bot list lazily (shared with Matching tab via session state)
    bots = st.session_state.get("bots") or []
    if not bots:
        try:
            from services.bigquery_service import get_active_bots
            with st.spinner("Loading bots..."):
                bots = get_active_bots()
                st.session_state.bots = bots
        except Exception as exc:
            st.caption(f"(Could not load bot list: {exc})")

    all_bot_ids = [b["bot_id"] for b in bots]
    current_bot = row.get("bot_id") or ""
    UNMAPPED = "-- Not mapped --"
    options = [UNMAPPED] + all_bot_ids
    if current_bot and current_bot not in options:
        options.append(current_bot)
    default_idx = options.index(current_bot) if current_bot in options else 0
    new_bot = st.selectbox(
        "Bot ID / WhatsApp flow",
        options,
        index=default_idx,
        help="Saves to Supabase account→bot mapping",
    )

    # Scope of Work — clickable link when present, "+ Add" button when missing
    st.markdown("**Scope of Work**")
    sow_url = row.get("sow_url") or ""
    if sow_url and sow_url not in ("N/A", "-"):
        st.markdown(f"📄 [Open SOW document]({sow_url})")
    else:
        ta_url = st.session_state.get(f"_ta_url_{opp_id}", None)
        if ta_url is None:  # not yet fetched this session
            try:
                from services.salesforce_service import get_tech_assist_lightning_url
                with st.spinner("Finding Tech Assist record..."):
                    ta_url = get_tech_assist_lightning_url(opp_id) or ""
                st.session_state[f"_ta_url_{opp_id}"] = ta_url
            except Exception as exc:
                ta_url = ""
                st.caption(f"(Could not look up Tech Assist: {exc})")
        if ta_url:
            st.link_button(
                "➕ Add SOW on Tech Assist",
                url=ta_url,
                help="Opens the Tech Assist record in Salesforce — populate the SOW link there.",
            )
        else:
            st.caption("No SOW linked and no Tech Assist record found for this opportunity.")

    st.caption("⚠ Saving writes Opportunity Name to Salesforce. Your account must have Edit permission on Opportunity.")

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Save", type="primary", use_container_width=True, key="edit_dlg_save"):
            errors = []

            if new_opp and new_opp != original_opp:
                try:
                    if _sf_update_opp(opp_id, new_opp):
                        _local_update_opp(opp_id, new_opp)
                        row["opportunity_name"] = new_opp
                    else:
                        errors.append("Salesforce refused the opportunity name update (check write permissions).")
                except Exception as exc:
                    errors.append(f"Salesforce error: {exc}")

            if new_bot != current_bot:
                try:
                    if new_bot == UNMAPPED:
                        clear_mapping(opp_id)
                        row["bot_id"] = None
                    else:
                        save_mapping(opp_id, new_bot)
                        row["bot_id"] = new_bot
                except Exception as exc:
                    errors.append(f"Bot mapping error: {exc}")

            if errors:
                for msg in errors:
                    st.error(msg)
            else:
                st.session_state.ttv_rows = ttv_rows
                st.rerun()

    with col_b:
        if st.button("Cancel", use_container_width=True, key="edit_dlg_cancel"):
            st.rerun()


def render(filter_keywords=None, title="Time-to-Value Dashboard",
           table_key="dashboard_table", show_chart=True):
    """Render the TTV table, metrics, and chart.

    filter_keywords: optional list of case-insensitive substrings; only
        accounts whose name contains any keyword are shown. Used by the
        Fast-Track tab.
    table_key: unique key for the dataframe widget & dialog session state,
        so multiple tabs can host their own selection.
    """
    st.header(title)

    # Auto-load from Supabase on first visit (fast)
    if "ttv_rows" not in st.session_state and sb.is_available():
        cached = load_cached_ttv_table()
        if cached:
            st.session_state.ttv_rows = cached

    # Fetch controls only appear in the main (unfiltered) view — filtered
    # tabs like Fast Track just read from the shared session_state.
    if not filter_keywords:
        col_compute, col_cache = st.columns(2)
        with col_compute:
            if st.button(
                "Compute TTV (live from BigQuery)",
                type="primary",
                key=f"{table_key}_compute_btn",
            ):
                with st.spinner("Querying BigQuery for milestones... This may take a minute."):
                    ttv_rows = compute_ttv_table()
                    st.session_state.ttv_rows = ttv_rows
                    if ttv_rows:
                        st.success(f"Computed milestones for {len(ttv_rows)} accounts")

        with col_cache:
            if sb.is_available():
                if st.button("Reload from cache", key=f"{table_key}_reload_btn"):
                    cached = load_cached_ttv_table()
                    if cached:
                        st.session_state.ttv_rows = cached
                        st.success(f"Loaded {len(cached)} accounts")
                    else:
                        st.warning("No cached data. Click 'Compute TTV' first.")

    ttv_rows = st.session_state.get("ttv_rows", [])

    if not ttv_rows:
        st.info("No TTV data yet. Click **Compute TTV** to pull data, or wait for the daily ingestion cron.")
        return

    if filter_keywords:
        kws = [k.lower() for k in filter_keywords]
        ttv_rows = [
            r for r in ttv_rows
            if any(k in (r.get("account_name") or "").lower() for k in kws)
        ]
        if not ttv_rows:
            st.warning(
                "No accounts matched the fast-track keywords: "
                + ", ".join(filter_keywords)
            )
            return

    # Merge business_type from session (live from Salesforce)
    bt_map = st.session_state.get("business_types", {})
    for row in ttv_rows:
        if not row.get("business_type"):
            row["business_type"] = bt_map.get(row.get("account_name"), "")

    # Merge delivery process data from session (live from Salesforce)
    delivery_map = st.session_state.get("delivery_data", {})
    for row in ttv_rows:
        opp_id = row.get("opportunity_id", "")
        dd = delivery_map.get(opp_id, {})
        for field in ("tech_assist_start", "tech_assist_end", "pm_start",
                       "go_live_date", "sow_url", "expected_go_live_pm",
                       "expected_go_live_sow"):
            if not row.get(field):
                row[field] = dd.get(field)

    summary = compute_summary(ttv_rows)

    # Metrics row
    cols = st.columns(5)
    cols[0].metric("Total Accounts", summary["total_accounts"])
    cols[1].metric("Mapped", summary["mapped_accounts"])
    for i, m in enumerate(MILESTONES):
        reached = summary["reached"][m]
        avg = summary["avg_days"][m]
        avg_str = f"{avg} days" if avg is not None else "N/A"
        cols[i + 2].metric(
            f"Reached {m} contacts",
            f"{reached}/{summary['mapped_accounts']}",
            delta=avg_str,
            delta_color="off",
        )

    st.divider()

    # Business type warning
    missing_bt = [r["account_name"] for r in ttv_rows if not r.get("business_type")]
    if missing_bt:
        st.warning(
            f"**{len(missing_bt)} account(s) missing Business Type in Salesforce:** "
            + ", ".join(missing_bt)
            + ".  Please update the *Business Type* field on these Accounts in Salesforce, then Refresh."
        )

    # Build display dataframe
    # Column layout: Identity → Milestone Dates (chronological) → Gaps between consecutive milestones → Info
    display_data = []
    for row in ttv_rows:
        is_mapped = bool(row.get("bot_id"))
        btype = row.get("business_type") or ""
        ta_start = (row.get("tech_assist_start") or "")[:10] or None
        ta_end = (row.get("tech_assist_end") or "")[:10] or None
        close = row.get("close_date") or None
        pm = row.get("pm_start") or None
        d10 = row.get("date_10") or None
        d50 = row.get("date_50") or None
        d100 = row.get("date_100") or None
        go_live = row.get("go_live_date") or None

        d = {
            # Identity
            "Type": btype if btype else "⚠ NOT SET",
            "Account": row["account_name"],
            "Status": _compute_status(row),
            "Days Since Close": row.get("days_since_close", "-"),
            "Mapped": "✓" if is_mapped else "—",

            # Milestone dates (process order: plan → execution → contact milestones)
            "Tech Assist Start": ta_start or "-",
            "Tech Assist End": ta_end or "-",
            "Deal Close": close or "-",
            "PM Kick-off": pm or "-",
            "Expected Go Live (SOW)": row.get("expected_go_live_sow") or "-",
            "Expected Go Live (PM)": row.get("expected_go_live_pm") or "-",
            "10 Contacts Reached": _date_with_days_since_close(d10, row.get("days_to_10")),
            "50 Contacts Reached": _date_with_days_since_close(d50, row.get("days_to_50")),
            "100 Contacts Reached": _date_with_days_since_close(d100, row.get("days_to_100")),

            # Days between consecutive milestones
            "Tech Assist Duration (days)": _days_between(ta_start, ta_end),
            "Tech Assist End → Close (days)": _days_between(ta_end, close),
            "Close → PM Kick-off (days)": _days_between(close, pm),
            "PM Kick-off → 10 Contacts (days)": _days_between(pm, d10),
            "10 → 50 Contacts (days)": _days_between(d10, d50),
            "50 → 100 Contacts (days)": _days_between(d50, d100),

            # Info
            "Total Contacts": row["total_unique_contacts"] if is_mapped else "-",
            "SF Link": row.get("sf_url") or "",
        }
        display_data.append(d)

    df = pd.DataFrame(display_data)

    # Column config — tooltips explaining data source + SF link column
    col_cfg = {
        "Type": st.column_config.TextColumn(
            "Type", help="Salesforce: Account → Business_Type__c"),
        "Account": st.column_config.TextColumn(
            "Account", help="Salesforce: Account.Name — select a row and click Edit to change Opportunity name or Bot ID"),
        "Status": st.column_config.TextColumn(
            "Status",
            help=(
                "Stage (last milestone passed): Tech Assist → Awaiting Account Executive → "
                "In Setup → Ramping (10+) → Implemented (50+). "
                "🔴 Stuck = too long at current stage without progressing "
                f"(Awaiting AE >{STUCK_DAYS['awaiting_pm']}d, In Setup >{STUCK_DAYS['in_setup']}d, "
                f"Ramping >{STUCK_DAYS['ramping']}d). "
                "⚠️ Late = past Expected Go Live and not yet Implemented."
            )),
        "Mapped": st.column_config.TextColumn(
            "Mapped", help="Whether the account has a Bot ID mapping. Select a row and click Edit to change."),
        "Tech Assist Start": st.column_config.TextColumn(
            "Tech Assist Start", help="Salesforce: Tech_Assist__c start date"),
        "Tech Assist End": st.column_config.TextColumn(
            "Tech Assist End", help="Salesforce: Tech_Assist__c end date"),
        "Deal Close": st.column_config.TextColumn(
            "Deal Close", help="Salesforce: Opportunity.CloseDate — first closed-won date for this account"),
        "PM Kick-off": st.column_config.TextColumn(
            "PM Kick-off", help="Salesforce: Project__c.Internal_Kick_Off_Started_Date__c"),
        "10 Contacts Reached": st.column_config.TextColumn(
            "10 Contacts Reached", help="BigQuery: date when bot first hit 10 unique contacts. (Nd) = days from Deal Close."),
        "50 Contacts Reached": st.column_config.TextColumn(
            "50 Contacts Reached", help="BigQuery: date when bot first hit 50 unique contacts (Implemented threshold). (Nd) = days from Deal Close."),
        "100 Contacts Reached": st.column_config.TextColumn(
            "100 Contacts Reached", help="BigQuery: date when bot first hit 100 unique contacts. (Nd) = days from Deal Close."),
        "Expected Go Live (PM)": st.column_config.TextColumn(
            "Expected Go Live (PM)", help="Salesforce: Project__c.Expected_Go_Live_informed_by_PM__c"),
        "Expected Go Live (SOW)": st.column_config.TextColumn(
            "Expected Go Live (SOW)",
            help="Extracted by LLM from the SOW document (services/sow_extraction.py). Stored in Supabase: ttv_account_mappings.expected_go_live_sow"),
        "Tech Assist Duration (days)": st.column_config.TextColumn(
            "Tech Assist Duration (days)", help="Tech Assist End − Tech Assist Start"),
        "Tech Assist End → Close (days)": st.column_config.TextColumn(
            "Tech Assist End → Close (days)", help="Deal Close − Tech Assist End"),
        "Close → PM Kick-off (days)": st.column_config.TextColumn(
            "Close → PM Kick-off (days)", help="PM Kick-off − Deal Close"),
        "PM Kick-off → 10 Contacts (days)": st.column_config.TextColumn(
            "PM Kick-off → 10 Contacts (days)", help="10 Contacts Reached − PM Kick-off"),
        "10 → 50 Contacts (days)": st.column_config.TextColumn(
            "10 → 50 Contacts (days)", help="50 Contacts Reached − 10 Contacts Reached"),
        "50 → 100 Contacts (days)": st.column_config.TextColumn(
            "50 → 100 Contacts (days)", help="100 Contacts Reached − 50 Contacts Reached"),
        "Days Since Close": st.column_config.NumberColumn(
            "Days Since Close", help="Today − Deal Close"),
        "Total Contacts": st.column_config.TextColumn(
            "Total Contacts", help="BigQuery: unique users who interacted with the bot since close date"),
        "SF Link": st.column_config.LinkColumn(
            "SF", help="Open this Opportunity in Salesforce", display_text="Open"),
    }

    # Style unmapped and missing-type rows
    def highlight_rows(row):
        if row["Mapped"] == "—":
            return ["background-color: #fff3cd"] * len(row)
        if row["Type"] == "⚠ NOT SET":
            return ["background-color: #f8d7da"] * len(row)
        return [""] * len(row)

    styled = df.style.apply(highlight_rows, axis=1)
    event = st.dataframe(
        styled,
        column_config=col_cfg,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=table_key,
    )

    # Row click → auto-open the account details dialog.
    # Gate with session state so the dialog doesn't reopen after the user
    # closes it (selection persists across reruns in st.dataframe).
    dlg_state_key = f"_edit_dialog_opened_for__{table_key}"
    if event.selection and event.selection.rows:
        idx = event.selection.rows[0]
        if st.session_state.get(dlg_state_key) != idx:
            st.session_state[dlg_state_key] = idx
            _edit_account_dialog(ttv_rows[idx], ttv_rows)
    else:
        st.session_state.pop(dlg_state_key, None)

    if not show_chart:
        return

    # Stacked bar chart — projects per month by TTV speed tier
    st.subheader("Projects by Month — Speed to 50 Contacts")
    import altair as alt

    # Filter by business type
    bt_filter = st.radio(
        "Business Type",
        ["All", "B2B", "B2C"],
        horizontal=True,
        key="bt_filter",
    )

    chart_rows = []
    for row in ttv_rows:
        if not row.get("bot_id"):
            continue
        btype = row.get("business_type") or ""
        if bt_filter == "B2B" and "B2B" not in btype:
            continue
        if bt_filter == "B2C" and "B2C" not in btype:
            continue
        close = row.get("close_date", "")
        if not close:
            continue
        month_label = close[:7]  # "2025-06"
        days = row.get("days_to_50")
        if days is None:
            tier = "Not reached"
        elif days < 30:
            tier = "< 30 days"
        elif days < 50:
            tier = "30–50 days"
        elif days < 100:
            tier = "50–100 days"
        else:
            tier = "100+ days"
        chart_rows.append({
            "Month": month_label,
            "Speed Tier": tier,
            "Account": row["account_name"],
        })

    if chart_rows:
        chart_df = pd.DataFrame(chart_rows)
        counts = (
            chart_df.groupby(["Month", "Speed Tier"])
            .size()
            .reset_index(name="Projects")
        )

        tier_order = ["< 30 days", "30–50 days", "50–100 days", "100+ days", "Not reached"]
        tier_colors = ["#2ca02c", "#ffbf00", "#ff7f0e", "#d62728", "#bbb"]

        chart = (
            alt.Chart(counts)
            .mark_bar()
            .encode(
                x=alt.X("Month:O", title="Month", sort=None),
                y=alt.Y("Projects:Q", title="Number of Projects", stack="zero"),
                color=alt.Color(
                    "Speed Tier:N",
                    scale=alt.Scale(domain=tier_order, range=tier_colors),
                    legend=alt.Legend(title="Days to 50 contacts"),
                ),
                tooltip=["Month", "Speed Tier", "Projects"],
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.caption("No milestone data to chart yet.")
