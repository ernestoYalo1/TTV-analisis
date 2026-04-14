"""
Dashboard Tab — TTV table, metrics, and chart.
"""
import streamlit as st
import pandas as pd

from services.ttv_service import compute_ttv_table, compute_summary
from config.settings import MILESTONES


def render():
    st.header("Time-to-Value Dashboard")

    if st.button("Compute TTV", type="primary"):
        with st.spinner("Querying BigQuery for milestones... This may take a minute."):
            ttv_rows = compute_ttv_table()
            st.session_state.ttv_rows = ttv_rows

    ttv_rows = st.session_state.get("ttv_rows", [])

    if not ttv_rows:
        st.info("No TTV data yet. Map accounts in the **Matching** tab, then click **Compute TTV**.")
        return

    summary = compute_summary(ttv_rows)

    # Metrics row
    cols = st.columns(4)
    cols[0].metric("Mapped Accounts", summary["total_accounts"])
    for i, m in enumerate(MILESTONES):
        reached = summary["reached"][m]
        avg = summary["avg_days"][m]
        avg_str = f"{avg} days" if avg is not None else "N/A"
        cols[i + 1].metric(
            f"Reached {m} contacts",
            f"{reached}/{summary['total_accounts']}",
            delta=avg_str,
            delta_color="off",
        )

    st.divider()

    # Build display dataframe
    display_data = []
    for row in ttv_rows:
        d = {
            "Account": row["account_name"],
            "Opportunity": row.get("opportunity_name", ""),
            "Close Date": row["close_date"],
            "Bot ID": row["bot_id"],
            "Total Contacts": row["total_unique_contacts"],
        }
        for m in MILESTONES:
            d[f"Date @{m}"] = row.get(f"date_{m}") or "-"
            days = row.get(f"days_to_{m}")
            d[f"Days to {m}"] = days if days is not None else "-"
        display_data.append(d)

    df = pd.DataFrame(display_data)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Bar chart — days to each milestone
    st.subheader("Days to Milestone")
    chart_data = []
    for row in ttv_rows:
        for m in MILESTONES:
            days = row.get(f"days_to_{m}")
            if days is not None:
                chart_data.append({
                    "Account": row["account_name"],
                    "Milestone": f"{m} contacts",
                    "Days": days,
                })

    if chart_data:
        chart_df = pd.DataFrame(chart_data)
        # Pivot for grouped bar chart
        pivot = chart_df.pivot(index="Account", columns="Milestone", values="Days")
        st.bar_chart(pivot)
    else:
        st.caption("No milestone data to chart yet.")
