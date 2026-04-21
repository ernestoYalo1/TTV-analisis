# TTV Analysis — Roadmap

Forward-looking work for the Time-to-Value dashboard. Organized by area, with rough priority (P0 = next, P2 = later).

---

## 🎨 Visual Redesign (indigitall-style)

- [ ] **P1** — Redesign the look & feel of the Streamlit app to match the indigitall Analytics UI (reference screenshot: `indigitall.frepi.ai/datos`). Key elements to replicate:
  - **Header bar**: logo on the left, centered horizontal nav with icon + label pills (active tab has a soft blue pill background `#DBEAFE` / blue text `#2563EB`), right-aligned cluster with project/context selector, dark-mode toggle, and a primary CTA button (`+ Crear`-style, solid blue `#2563EB`, white text, rounded-md).
  - **Typography**: sans-serif (Inter or system stack), bold page title (~28–32px), muted gray subtitle (`#6B7280`) directly beneath it. Section headers are medium weight, not uppercase.
  - **Info banner**: soft blue background (`#EFF6FF`) with a blue info icon, 1px blue border, rounded-lg, used for warnings like the current "Business Type missing" notice (today it's yellow — swap to the blue informational style or keep yellow only for true warnings).
  - **Cards / panels**: white background, subtle 1px gray border (`#E5E7EB`), rounded-lg, generous padding (24px). No heavy drop shadows.
  - **Empty states**: centered icon in a light-gray circle + muted helper text (see the "Selecciona una tabla" preview pane in the reference).
  - **Inputs**: rounded-md, 1px border, leading icon (search glass for filters), subtle focus ring in the primary blue.
  - **Accents**: primary = blue `#2563EB`, success/brand = green `#10B981` (for the logo-equivalent), warning = amber, destructive = red. Use orange `#F97316` only for secondary counters ("datos subidos (0)" style).
  - **Dark mode**: add a toggle in the header; use a dark-gray surface (`#0F172A` bg, `#1E293B` panels, `#E2E8F0` text). Confirm Streamlit theming supports this via `.streamlit/config.toml` + custom CSS.
  - **Spacing**: switch to a denser 8px grid; reduce vertical gaps between Streamlit containers (they currently feel stretched).
  - **Icons**: adopt a single icon family (Lucide or Heroicons outline) throughout — nav, table, buttons.

  Additional elements from the second reference (indigitall Tableros / `Dahboard Visionamos V2`):
  - **Page header row**: large bold page title left; right-aligned button group `Actualizar` / `Editar` / `← Tableros` using the same icon+label bordered-ghost button style (white bg, 1px gray border, rounded-md, small icon before the label). Primary CTAs stay solid blue; secondary/utility buttons use this ghost style.
  - **Filter / grouping pills**: prefix label ("AGRUPAR POR:") in uppercase muted gray, followed by a row of pill toggles — active pill is solid blue `#2563EB` with white text, inactive pills are white with 1px gray border and dark text. Use the same pill component for the `Filtros` button (with funnel icon leading).
  - **Sub-tab strip** (General / WhatsApp bot / Contact center / SMS): same pill styling as the grouping toggles — active = solid blue pill, inactive = plain gray text, all left-aligned in a row under the page header's filter section.
  - **KPI cards**: white card, 1px gray border, rounded-lg, padding 20–24px. Small title top-left (regular weight, dark gray). Refresh + info icon cluster top-right. Big primary-blue number centered (~48–56px, bold, color `#2563EB`). Small uppercase muted label below the number (`letter-spacing: 0.05em`, color `#6B7280`, ~11px). Use a 4-column grid on desktop that collapses responsively.
  - **Chart cards**: same card chrome as KPIs. Title top-left, action icons top-right (settings/swap, refresh, info). Chart body fills the card with a legend below. Stick to the brand palette — primary blue for main series, lighter blue for secondary, amber/orange for tertiary/line overlays.
  - **Section banner** (`Vista general KPIs y Métricas Visionamos`): full-width card acting as a section header strip — center-aligned bold title, no border or softest possible border, clear separation from KPI grid below.
  - **Floating AI assistant panel** (nice-to-have, P2): docked bottom-right panel titled "Analista IA" with a tab badge (e.g. `tab-1774442706353`), a centered helper line, a list of suggested-prompt rows (icon + label, each row hoverable), and an input with send button at the bottom. Matches how the indigitall analyst is positioned — could map to a future Claude-powered ask-about-your-data feature for the TTV dashboard.

  Implementation path:
  1. Create `components/styles.py` (or extend existing) with a single `inject_css()` function that `st.markdown("<style>…</style>", unsafe_allow_html=True)` on app load from `app.py`. Organize CSS into logical groups: header, pills/tabs, cards (KPI + chart), alerts, inputs, tables.
  2. Update `.streamlit/config.toml` `[theme]` block: `primaryColor="#2563EB"`, `backgroundColor="#FFFFFF"`, `secondaryBackgroundColor="#F9FAFB"`, `textColor="#111827"`, `font="sans serif"`.
  3. Replace the current tab bar in `app.py` / `components/dashboard_tab.py` with a custom styled `st.radio` or `streamlit-option-menu` to get the pill look. Reuse the same component for the "Agrupar por" grouping toggles.
  4. Build a reusable `kpi_card(title, value, label)` helper that renders the blue-number card — apply to the existing summary metrics at the top of the dashboard tab.
  5. Audit every `st.warning` / `st.info` call and pick the right variant; restyle via CSS selectors on `[data-testid="stAlert"]`.
  6. Screenshot before/after for each tab (Dashboard, Matching) and confirm with user before shipping.

---

## 📊 Dashboard & UX

- [ ] **P0** — Add filters: date range (close date), business type (beyond the chart-only filter), and "unmapped only" toggle on the table
- [ ] **P0** — Export table to CSV / Google Sheets from the dashboard
- [ ] **P1** — Add "Expected vs Actual Go Live" delta column (compare `expected_go_live_pm` and `expected_go_live_sow` vs `go_live_date`)
- [ ] **P1** — Second chart: cumulative TTV distribution curve (% of accounts reaching 50 contacts within N days)
- [ ] **P1** — Row detail panel: show the raw BigQuery contact-count time series as a sparkline
- [ ] **P2** — Per-account drill-in page (deep link) with full milestone history
- [ ] **P2** — Segment comparison view: B2B vs B2C side-by-side median/P90 TTV
- [x] **P1** — Collapse the `Opportunity` and `Bot ID` (WhatsApp flow) columns out of the main table to save horizontal space. Clicking the **Account** cell should open a popup/modal showing: (a) Opportunity name — editable, writes back to Salesforce opportunity record; (b) Bot ID / WhatsApp flow — editable, writes back to the Supabase account→bot mapping used by `services/mapping_service.py`. Keep the account row highlighted while the popup is open, and refresh the row after save. Touches `components/dashboard_tab.py` (table render), `services/mapping_service.py` (bot_id update), and `services/salesforce_service.py` (opportunity name update — confirm write permissions first). _Done 2026-04-18 — modal opens via ✏️ Edit button beneath selection card._

---

## 🔌 Data & Ingestion

- [ ] **P0** — Alerting: notify on daily_ingest failure (currently fails silently to log file)
- [ ] **P0** — Dedupe opportunities per account — current "first close date" logic should be verified against multi-opp accounts
- [ ] **P1** — Track ingestion runs in Supabase (timestamp, accounts processed, errors) and surface "last synced" in dashboard header
- [ ] **P1** — Backfill historical milestone dates for accounts mapped *after* they crossed a threshold
- [ ] **P2** — Incremental BigQuery queries (only recompute accounts with recent activity) to cut cron runtime

---

## 🧾 SOW Extraction (Phase 2)

- [ ] **P0** — Surface the Salesforce SOW link in the dashboard row. Behavior:
  - If the opportunity has a `sow_url` (or equivalent SF field — confirm field API name in `services/salesforce_service.py`), render it as a clickable 📄 icon / "SOW" link in the Account popup (see the column-compression TODO above) and optionally as a small icon in the table row.
  - If the field is empty, render a **+ (add)** icon in the same slot. Clicking it opens the Tech Assist record for that opportunity in a new tab (Tech Assist is where the SOW link should be populated). The Tech Assist URL pattern should come from SF (related object) — verify whether Tech Assist is a child record on the Opportunity or a separate custom object; grab its `Id` via `salesforce_service` and build `https://<instance>.lightning.force.com/lightning/r/<TechAssistObj>/<Id>/view`.
  - On hover, tooltip: "No SOW linked — click to add on the Tech Assist record".
  - This unblocks the rest of the SOW extraction pipeline: today many opportunities silently have no `sow_url`, and there's no in-app nudge to fix it. This button makes it a one-click fix for the PM.
- [ ] **P0** — Handle Drive folder URLs in `sow_url` (currently returns None — pick most recent SOW doc inside)
- [ ] **P0** — Store extraction metadata in Supabase: `sow_extraction_confidence`, `sow_extraction_source`, `sow_extracted_at`
- [ ] **P1** — Re-extract on SOW URL change (currently only runs when `expected_go_live_sow` is NULL)
- [ ] **P1** — Show SOW-extracted date with confidence badge in dashboard row detail
- [ ] **P2** — Extract additional fields: phases, scope items, delivery team size

---

## 🚀 Deployment & Ops

- [ ] **P0** — Confirm cron job is installed and running on the server (`scripts/daily_ingest.py`)
- [ ] **P1** — Log rotation for `/opt/ttv_analysis/logs/daily_ingest.log`
- [ ] **P1** — Healthcheck endpoint or uptime monitor on the Streamlit app
- [ ] **P2** — Move secrets out of env into a secret manager (Supabase key, Anthropic key, SF creds)

---

## 🧪 Quality

- [ ] **P1** — Unit tests for `services/ttv_service.py` milestone computation (pure logic, no I/O — easy win)
- [ ] **P1** — Unit tests for `_extract_doc_id` in `sow_extraction.py` (many URL formats)
- [ ] **P2** — Integration test harness using the sample CSVs in `data/`

---

## 📈 Analysis Deliverables

- [ ] **P1** — Monthly TTV report (PDF or Slides) auto-generated from the dashboard
- [ ] **P2** — Correlation study: does Tech Assist duration predict faster TTV?
- [ ] **P2** — Cohort analysis by customer segment / region / deal size

---

_Last updated: 2026-04-18_
