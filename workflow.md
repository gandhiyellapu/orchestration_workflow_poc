Treasure Workflow (Digdag):
This skill builds a complete orchestration workflow for POCs by customizing the template at workflow-skills/workflow-template. It creates a full data pipeline with conditional path:
staging → unification → IDU dashboard → golden → analytics → segmentation

---

Prerequisites:
Before customizing, ask the user these questions:
1. Client name (used as `sub` prefix for all databases, e.g. `us_company`)
2. Project name (used as workflow project name, e.g. `us-company-orchestration`)
3. Source database name (existing database with tables, or "create demo data" if starting fresh)
4. Ask for any task to be skipped like IDU dashboard or analytics or any other step
5. Unification ID name (e.g. `canonical_id`, `td_id`)
6. idu dashboard step : ask the user whether this step is needed or can skip (if asked to skip comment out this logic) 
7. Dashboard user emails — for dashboard access grants (e.g. `['user@company.com']`)
8. Notification emails — for workflow success/error alerts (e.g. `['user@company.com']`)
9. Schedule: on or off (default: daily at 4 AM ET, use `cron>` in wf00_orchestration.dig)


Output directory: Create the customized workflow in `<sub>_workflow/` (e.g. `poc_ai_workflow/`)

---

Helper Skills (use these for specific steps):
  - ps_poc_automation/ — Data profiling, staging transformations, unification key discovery, golden layer construction. Use its profiling logic to scan tables, detect PII, identify join keys, and assess data quality before writing staging/unification/golden queries.
  - sql-skills/trino — TD Trino SQL patterns (td_interval, td_time_range, partition pruning). Use when writing time-based queries in staging, golden, or analytics layers.
  - sql-skills/hive — TD Hive SQL patterns (lateral view, MAPJOIN). Use for analytics queries like market basket analysis that need Hive engine.
  - sql-skills/trino-optimizer — Query performance optimization (CTAS, UDP bucketing, approx functions). Use when golden or analytics queries need performance tuning.
  - sql-skills/time-filtering — Advanced td_interval patterns for time-based filtering and partition pruning.
  - tdx-skills/tdx-basic — tdx CLI commands for database/table exploration, query execution (used in Step 0).
  - tdx-skills/workflow — tdx wf commands for push, run, monitor, retry (used in Step 9).
  - tdx-skills/parent-segment — tdx ps commands for parent segment validation and preview (used in Step 7).
  - tdx-skills/segment — tdx sg commands for child segment creation and activation (used in Step 7).
  - aps-doc-skills/ — Generate documentation for any layer (staging, id-unification, golden, master-segment). Use after building each layer to auto-generate technical docs.

---

Template Structure Overview:
The template contains these .dig workflow files that form the execution chain:

  wf00_orchestration.dig — Main entry point, orchestrates all steps sequentially then parallel
  wf01_run_workflow_with_logging.dig — Wrapper that adds logging and conditional skip logic
  wf02_mapping.dig — Runs prep queries then mapping transformation (map.sql)
  wf03_validate.dig — Schema validation, deviation reports, HTML email alerts
  wf04_stage.dig — Data cleaning and trfmd_* column creation
  wf05_unify.dig — Calls TD unification API with unify.yml config
  wf06_idu_dash.dig - analyze and validate the results of the ID Unification workflow
  wf06_golden.dig — Creates golden tables: copy enriched/unified data, build attribute aggregations
  wf07_analytics.dig — Sales analytics, web analytics, Sankey conversion, IDU dashboard, datamodel creation
  wf08_create_refresh_master_segment.dig — Creates/refreshes parent segments from templates
  wf09_create_segment.dig — Creates child segments from segment templates (optional)

Supporting workflows:
  utilities/error.dig — Error notification emails
  utilities/success.dig — Success notification emails
  idu_dashboard/idu_dashboard_launch.dig — IDU dashboard orchestration (called from wf07)
  idu_dashboard/idu_dashboard_data_prep.dig — IDU data prep (column mappings, stats, histograms)
  idu_dashboard/idu_qa.dig — Over-merge detection and QA analysis
  idu_dashboard/idu_datamodel_create.dig — Creates IDU reporting datamodel
  idu_dashboard/idu_datamodel_build.dig — Builds/refreshes IDU datamodel
  idu_dashboard/idu_cleanup_runner.dig — Cleans up temp tables
  analytics/dashboard/analytics_dashboard_launch.dig — Analytics dashboard orchestration
  analytics/dashboard/analytics_datamodel_create.dig — Creates analytics datamodels
  analytics/dashboard/analytics_datamodel_build.dig — Builds analytics datamodels

Execution order in wf00_orchestration:
  1. Create databases (va_config, prp, src, stg, gld, analytics)
  2. Prepare log table
  3. Conditional on skip_prep_mapping (set in src_params.yml):
     - If skip_prep_mapping=false (raw database available):
       Sequential: wf02 → wf03 → wf04 → wf05 → wf06 (each wrapped in wf01 for logging)
     - If skip_prep_mapping=true (no raw database):
       Skip wf02 (mapping) and wf03 (validation), go directly to wf04 (staging)
       Staging reads from source_database directly instead of src_<sub>
       Then: wf04 → wf05 → wf06
  4. Parallel: wf07 (analytics + IDU dashboard) || wf08 (parent segment)
  5. Optional: wf09 (child segments, commented out by default)
  6. Success/error notification via utilities/*.dig

---

Step 0: Source Database Discovery
Helper skills: tdx-skills/tdx-basic (for tdx commands), ps_poc_automation/ (for data profiling)
Before any customization, explore the source database to understand the data:
  - Run `tdx tables <database>` to list all tables
  - Run `tdx tables <database> <table>` for each table to get column schemas
  - Use ps_poc_automation profiling logic to: detect PII columns, identify join keys, assess duplicates, classify time columns, evaluate JSON/ARRAY fields
  - Identify which tables contain the unification key
  - Identify catalog/reference tables (e.g. products) vs customer tables
  - Map source columns to template patterns (email, phone, name, dates, amounts)
  - Note column types (string, long, int, double) for schema_map.yml

Step 1: Prep & Mapping logic customization
NOTE: This step is SKIPPED when skip_prep_mapping=true (no raw database). When skipped, staging reads directly from the source database.
Reference: workflow-template/prep/, workflow-template/mapping/, workflow-template/config/
Workflow: wf02_mapping.dig runs prep/queries/<table>.sql then mapping/queries/map.sql
  - Create prep/queries/<table>.sql for each source table (DROP/CREATE TABLE AS SELECT *)
  - Generate config/schema_map.yml mapping source columns to standardized names
    - schema_map.yml drives mapping/queries/map.sql which transforms columns in the src database
  - Configure config/src_params.yml with the actual table list from Step 0
  - Configure config/email_ids.yml with notification emails
  - Prep copies raw data from source database into prp_<sub> database
  - Mapping transforms prp data into src_<sub> using schema_map.yml column mappings

Step 2: Validation logic customization
NOTE: This step is SKIPPED when skip_prep_mapping=true (no raw database). Validation only applies when data passes through prep/mapping.
Reference: workflow-template/validation/
Workflow: wf03_validate.dig
  - Create validation/queries/schema/<table>.sql for each table — defines expected schema (column names, types)
  - validation/queries/src_tmp_ddl.sql and src_tmp_drop_ddl.sql — create/drop temp reference tables
  - validation/queries/check_src_vs_ref.sql — compares source tables against reference schemas
  - validation/queries/report_deviations.sql — generates deviation report (missing tables, columns, type mismatches)
  - validation/queries/get_errors.sql and get_warnings.sql — classify issues by severity
  - validation/queries/fetch_email_body.sql — prepares email content for alerts
  - validation/python/ — gen_html.py, global_var.py, pretty_html_table.py generate HTML email reports
  - If critical deviations found (missing tables/columns/type mismatches), workflow fails and sends error alert

Step 3: Staging logic customization
Reference: workflow-template/staging/
Helper skills: ps_poc_automation/ (staging transformations), sql-skills/trino (SQL patterns), sql-skills/time-filtering (date conversions)
Workflow: wf04_stage.dig
  - When skip_prep_mapping=true: staging reads from source_database directly (not src_<sub>)
  - When skip_prep_mapping=false: staging reads from src_<sub> (after prep/mapping)
  - Create staging/queries/<table>.sql for each table
  - Use ps_poc_automation staging logic for data-driven transformation discovery (auto-detect email, phone, name, date columns)
  - Use sql-skills/trino for TD-specific functions (td_time_parse, CAST, COALESCE patterns)
  - Clean messy data and standardize formats:
    - Emails: lowercase, trimmed, validated format
    - Phones: digits only, standardized length
    - Names: Title Case
    - Dates: Unix timestamps (seconds)
    - Currencies: consistent decimal format
  - Create trfmd_* columns with clean values alongside original columns
  - Create staging/queries/invalid_emails.sql for email quality flagging
  - Note: some tables may need split transformations (e.g. consents_email.sql, consents_phone.sql for channel-specific consent processing)

Step 4: Unification logic customization
Reference: workflow-template/unification/
Helper skills: ps_poc_automation/ (unification key discovery and matching logic)
Workflow: wf05_unify.dig — calls TD CDP unification API with unify.yml
  - Use ps_poc_automation unification logic to identify best keys and matching strategy
  - First check: does the data already have a common ID across all tables?
    - If YES (pre-unified): simplify unify.yml, use the existing ID as primary key
    - If NO: identify best unification keys (email, phone, customer_id, device_id, session_id)
  - Configure unification/unify.yml with:
    - Match rules and key priorities
    - Survivorship rules (most recent, most complete, etc.)
    - Source table mappings
  - Create unified customer IDs in the output
  - Output goes to cdp_unif_<sub> database with <unification_id>_lookup table

Step 5: Golden logic customization
Reference: workflow-template/golden/
Helper skills: ps_poc_automation/ (golden layer construction), sql-skills/trino-optimizer (query performance), sql-skills/trino (aggregation patterns)
Workflow: wf06_golden.dig
  - Use ps_poc_automation golden layer logic to build single customer view with unified IDs
  - Use sql-skills/trino-optimizer for CTAS patterns (5x faster table creation) and approx functions for large datasets
  - Create golden/queries/all_profile_identifiers.sql — master identity table (one row per customer)
  - Create golden/queries/copy_enriched_table.sql — copies enriched tables from unification output (cdp_unif_<sub>)
  - Create golden/queries/copy_table.sql — copies tables from staging (stg_<sub>)
  - Create golden/queries/ for row-level tables that need drill-down (e.g. orders, consent, transactions)
    - These are copied from staging/unification with the unification_id joined in
  - Create golden/queries/attributes/ for aggregated metrics per customer:
    - Derive attribute tables dynamically from the source tables discovered in Step 0
    - For each source table category, create an aggregation query with relevant metrics:
      - Transaction tables → lifetime revenue, total orders, AOV, first/last dates
      - Engagement tables (email, app, SMS) → interaction counts, rates, last activity
      - Web/pageview tables → views, unique pages, products browsed
      - Cart/basket tables → abandonment counts, product/category breakdowns
      - Service/support tables → ticket counts by status/priority
      - Survey/feedback tables → response counts, scores, averages
    - Only create attribute tables for source data that actually exists — skip categories with no matching source tables
    - Each attribute table should aggregate to one row per customer (GROUP BY unification_id)
  - All golden tables join on the unification_id

Step 6: Idu dashboard customization

  IDU Dashboard (ID Unification quality reporting — called from wf07):
  - idu_dashboard/idu_dashboard_launch.dig orchestrates:
    - idu_dashboard_data_prep.dig — column mappings, known/unknown stats, ID histograms, matching rates
    - idu_qa.dig — over-merge detection (profiles with count > 2.5*stdev), frequent ID analysis
    - idu_datamodel_create.dig / idu_datamodel_build.dig — creates and refreshes reporting datamodel
    - idu_cleanup_runner.dig — drops temporary tables after processing
  - idu_dashboard/queries/ — 30+ SQL files for data prep, statistics, and QA analysis
  - idu_dashboard/queries/qa/ — 15 SQL files for over-merge detection and ID quality analysis
  - idu_dashboard/python_files/ — create_datamodel.py, set_params.py
  - idu_dashboard/dashboard_template/ — .dash template files
  - idu_dashboard/config/ — params.yml, unify.yml for IDU-specific settings
  
Step 7: Analytics logic customization
Reference: workflow-template/analytics/, workflow-template/idu_dashboard/
Helper skills: sql-skills/hive (for market basket analysis), sql-skills/trino (for web analytics), sql-skills/time-filtering (for time-based aggregations)
Workflow: wf07_analytics.dig (calls IDU dashboard and analytics dashboard sub-workflows)

  Analytics queries (customize based on available data):
  - analytics/queries/sales/ — sales trends, market basket analysis (uses Hive engine — see sql-skills/hive for MAPJOIN and lateral view patterns)
  - analytics/queries/web_analytics/ — web analytics, conversion funnels, Sankey visualization
    - web_analytics.sql, web_analytics_agg.sql, web_analytics_agg_others.sql
    - sankey_web_conversion.sql, sankey_generate_labels.sql, sankey_step_statistics.sql, sankey_data_model_final.sql
  - analytics/queries/create_model_config.sql — creates reporting datamodel config
  - analytics/python/create_datamodel.py — Python script to create datamodels via API
  - Only include analytics queries relevant to the available source data

  Analytics dashboards:
  - analytics/config/datamodels/ — JSON datamodel definitions (sales_analytics.json, web_analytics.json, idu_dashboard.json)
  - analytics/config/web_analytics.yml — web analytics configuration
  - analytics/dashboard/ — dashboard launch, datamodel create/build workflows
  - analytics/dashboard/template/ — .dash dashboard template files
  - analytics/dashboard/config/ — config.json and params.yml for dashboard setup


Step 8: Segment logic customization
Reference: workflow-template/segment/
Helper skills: tdx-skills/parent-segment (tdx ps validate/preview), tdx-skills/segment (tdx sg for child segments)
Workflows: wf08_create_refresh_master_segment.dig, wf09_create_segment.dig

  Parent segment (wf08):
  - Optimize segment/config/parent_segment_templates/retail_parent_segment_template.yml: use template from  workflow-template/segment/config/parent_segment_templates/retail_parent_segment_template.yml
    - Read ALL golden layer SQL files to extract exact output table names and column names
    - Set master.parentDatabaseName to gldn_<sub>
    - Set master.parentTableName to profile_identifiers (or equivalent master table)
    - Map every golden ATTRIBUTE table to parent segment attributes:
      - Set parentKey and foreignKey to the unification_id
      - Use actual column names from the golden SQL output (not template placeholders)
      - Group attributes into logical categories based on what exists (e.g. Identity, Demographics, Purchase, Engagement, etc.)
    - Map golden ROW-LEVEL tables to parent segment behaviors:
      - Use whichever row-level tables exist in the golden layer (e.g. orders, consent, identifiers)
      - Include key columns for drill-down (IDs, amounts, dates, categories, etc.)
    - Remove any template attributes/behaviors that reference tables not in the golden layer
    - Use pre-aggregated golden attributes for metrics (not raw behavior tables)
  - segment/python/ — Python scripts for segment creation:
    - create_parent_segments.py — creates parent segments from templates
    - list_templates.py — discovers available templates
    - td.py — TD API helper
    - api/folder.py, ps.py, segment.py — API wrappers for folders, parent segments, child segments
    - helper/global_var.py, utils.py — shared utilities
  - segment/queries/ — SQL queries for segment management:
    - lookup_templates.sql, lookup_segment_templates.sql — discover templates in config
    - active_audience.sql, get_active_audience.sql — identify active audiences for refresh
    - cleanup_log.sql — clean up segment logs

  Child segments (wf09, optional):
  - segment/config/segment_templates/ — contains segment template YAML files (segments_1.yml through segments_N.yml)
  - Customize segment templates with audience rules based on golden layer attributes
  - wf09 reads templates and creates child segments via segment/python/create_segment.py

Step 9: Utilities
Reference: workflow-template/utilities/
  - utilities/error.dig — sends error notification email using error_body.txt template
  - utilities/success.dig — sends success notification email using success_body.txt template
  - utilities/generic.txt — generic email template
  - utilities/python/set_params.py — parameter setup utility
  - utilities/queries/log_tbl.sql — creates/manages the workflow execution log table
  - wf01_run_workflow_with_logging.dig uses log_tbl to track step execution history
    - If run_all=true: runs all steps regardless of history
    - If run_all!=true: skips steps that already succeeded in the current session

Step 10: Add secrets/keys 

Step 11: Push & Deploy
Helper skills: tdx-skills/workflow (tdx wf push, run, sessions, timeline)
  - Run `tdx wf push --revision <version> --yes` from the output directory
  - Fix any .dig validation errors (YAML indentation, missing files) before pushing
  - Verify push success and provide the TD Console URL to the user
  - Inform the user how to run manually: `tdx wf run <project>.wf00_orchestration`
  - Inform the user how to monitor: `tdx wf sessions <project>`
