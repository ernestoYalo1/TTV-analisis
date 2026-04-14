"""
Matching Tab — Link Salesforce accounts to BigQuery bot_ids.
"""
import streamlit as st

from services.mapping_service import (
    get_all_mappings, upsert_accounts, save_mapping, clear_mapping
)
from services.salesforce_service import get_new_customers
from services.bigquery_service import get_active_bots


def render():
    st.header("Account ↔ Bot Matching")

    col_refresh, col_status = st.columns([1, 3])
    with col_refresh:
        if st.button("Refresh from Salesforce & BigQuery", type="primary"):
            _refresh_data()

    # Load bot list (cached in session)
    if "bots" not in st.session_state:
        st.session_state.bots = []

    bots = st.session_state.bots
    bot_ids = ["-- Not mapped --"] + [b["bot_id"] for b in bots]

    mappings = get_all_mappings()
    if not mappings:
        st.info(
            "No accounts loaded yet. Click **Refresh from Salesforce & BigQuery** "
            "to pull new customers and available bots."
        )
        return

    # Stats
    mapped_count = sum(1 for m in mappings if m.get("bot_id"))
    total_count = len(mappings)
    st.markdown(f"**{mapped_count}** of **{total_count}** accounts mapped")
    st.divider()

    # Show each account with a selectbox for bot_id
    for acct in mappings:
        opp_id = acct["opportunity_id"]
        current_bot = acct.get("bot_id") or ""

        with st.container():
            col_info, col_select, col_action = st.columns([3, 3, 1])

            with col_info:
                st.markdown(f"**{acct['account_name']}**")
                close = acct.get('close_date', 'N/A')
                opp_name = acct.get('opportunity_name', '')
                amt = f"${acct['amount']:,.0f}" if acct.get('amount') else ""
                st.caption(f"{opp_name} | Closed: {close} {amt}")
                if acct.get("sf_url"):
                    st.caption(f"[Salesforce link]({acct['sf_url']})")

            with col_select:
                # Find current index
                if current_bot and current_bot in bot_ids:
                    default_idx = bot_ids.index(current_bot)
                else:
                    default_idx = 0

                selected = st.selectbox(
                    "Bot ID",
                    bot_ids,
                    index=default_idx,
                    key=f"bot_select_{opp_id}",
                    label_visibility="collapsed",
                )

            with col_action:
                if selected != "-- Not mapped --":
                    if selected != current_bot:
                        if st.button("Save", key=f"save_{opp_id}", type="primary"):
                            save_mapping(opp_id, selected)
                            st.rerun()
                    else:
                        if st.button("Clear", key=f"clear_{opp_id}"):
                            clear_mapping(opp_id)
                            st.rerun()

            st.divider()


def _refresh_data():
    """Pull fresh data from Salesforce and BigQuery, store in SQLite + session."""
    with st.spinner("Querying Salesforce for new customers..."):
        try:
            customers = get_new_customers()
            upsert_accounts(customers)
            st.success(f"Loaded {len(customers)} new customer accounts from Salesforce")
        except Exception as e:
            st.error(f"Salesforce error: {e}")
            return

    with st.spinner("Querying BigQuery for active bots..."):
        try:
            bots = get_active_bots()
            st.session_state.bots = bots
            st.success(f"Found {len(bots)} active bots in BigQuery")
        except Exception as e:
            st.error(f"BigQuery error: {e}")
            return

    st.rerun()
