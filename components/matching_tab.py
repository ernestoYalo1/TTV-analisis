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

    col_refresh, col_search = st.columns([1, 2])
    with col_refresh:
        if st.button("Refresh from Salesforce & BigQuery", type="primary"):
            _refresh_data()

    # Load bot list (cached in session)
    if "bots" not in st.session_state:
        st.session_state.bots = []

    bots = st.session_state.bots
    all_bot_ids = [b["bot_id"] for b in bots]

    # Global search filter for bots
    with col_search:
        search_term = st.text_input(
            "Search bots",
            placeholder="Type to filter bot IDs (e.g. 'pepsico', 'santander')",
            key="bot_search",
        )

    if search_term:
        filtered_bot_ids = [b for b in all_bot_ids if search_term.lower() in b.lower()]
    else:
        filtered_bot_ids = all_bot_ids

    bot_options = ["-- Not mapped --"] + filtered_bot_ids

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

    if search_term:
        st.markdown(
            f"**{mapped_count}** of **{total_count}** accounts mapped  ·  "
            f"Showing **{len(filtered_bot_ids)}** of {len(all_bot_ids)} bots matching '{search_term}'"
        )
    else:
        st.markdown(f"**{mapped_count}** of **{total_count}** accounts mapped  ·  {len(all_bot_ids)} bots available")

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
                # If already mapped, show current bot even if not in filtered list
                if current_bot:
                    display_options = bot_options if current_bot in bot_options else ["-- Not mapped --", current_bot] + filtered_bot_ids
                else:
                    display_options = bot_options

                if current_bot and current_bot in display_options:
                    default_idx = display_options.index(current_bot)
                else:
                    default_idx = 0

                selected = st.selectbox(
                    "Bot ID",
                    display_options,
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
    """Pull fresh data from Salesforce and BigQuery, store in SQLite/Supabase + session."""
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
