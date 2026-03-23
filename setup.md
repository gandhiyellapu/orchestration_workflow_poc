# TD CDP Pipeline Setup - Master Orchestration Guide

## Overview

This is the **master orchestration skill** that guides you through setting up a complete Treasure Data CDP pipeline from scratch with **automated validation** and **conditional optional workflows**.

**Pipeline Flow:**
```
profiling → staging → unification → golden → segmentation
   (0)        (1)         (2)         (3)         (4)

Optional (user choice):
  - idu_dashboard (after Step 2: unification)
  - analytics (after Step 3: golden)
```

**Duration:** 4-8 hours (depending on data size)
**Prerequisites:**
- TD account with access
- TD CLI configured and authenticated
- Python 3.11+ installed
- Node.js 22+ installed (optional, for TDX CLI)
- Source database with data in Treasure Data

---

## Validation Utilities

**IMPORTANT:** Before starting the pipeline, ensure these validation helper functions are available.

### Helper Function: Check File Exists
```bash
check_file_exists() {
  local file=$1
  local description=$2

  if [ ! -f "$file" ]; then
    echo "❌ ERROR: $description not found: $file"
    echo "   Please complete the previous step before proceeding."
    return 1
  fi
  return 0
}
```

### Helper Function: Check Workflow Status
```bash
check_workflow_status() {
  local session_id=$1
  local workflow_name=$2

  echo "Checking $workflow_name status (session: $session_id)..."

  # Try with jq first, fallback to grep if jq not available
  if command -v jq &> /dev/null; then
    STATUS=$(tdx wf session $session_id --format json 2>/dev/null | jq -r '.status' 2>/dev/null || echo "unknown")
  else
    # Fallback: parse status from text output
    STATUS=$(tdx wf session $session_id 2>/dev/null | grep -i "status" | awk '{print $2}' | tr -d ',' || echo "unknown")
  fi

  if [ "$STATUS" = "success" ]; then
    echo "✅ $workflow_name completed successfully!"
    return 0
  elif [ "$STATUS" = "error" ] || [ "$STATUS" = "failed" ]; then
    echo "❌ ERROR: $workflow_name failed with status: $STATUS"
    echo "   View logs: tdx wf session $session_id"
    echo "   Fix the issue and re-run the workflow before proceeding."
    return 1
  elif [ "$STATUS" = "running" ]; then
    echo "⏳ $workflow_name is still running (status: $STATUS)"
    echo "   Please wait for completion before proceeding."
    return 1
  else
    echo "⚠️  WARNING: Unable to determine $workflow_name status: $STATUS"
    echo "   Manually verify: tdx wf session $session_id"
    return 1
  fi
}
```

### Helper Function: Check Database Exists
```bash
check_database_exists() {
  local database=$1
  local description=$2

  echo "Checking if database '$database' exists..."

  if tdx tables $database &> /dev/null; then
    echo "✅ Database $database found."
    return 0
  else
    echo "❌ ERROR: $description database '$database' does not exist."
    echo "   Please complete the previous step to create this database."
    return 1
  fi
}
```

### Helper Function: Check Table Exists
```bash
check_table_exists() {
  local database=$1
  local table=$2
  local description=$3

  echo "Checking if table '$database.$table' exists..."

  if tdx tables $database $table &> /dev/null; then
    TABLE_COUNT=$(tdx tables $database $table 2>&1 | grep -c "$table" || echo "0")
    if [ "$TABLE_COUNT" -gt 0 ]; then
      echo "✅ Table $database.$table found."
      return 0
    fi
  fi

  echo "❌ ERROR: $description table '$database.$table' does not exist."
  echo "   Please ensure the previous step completed successfully."
  return 1
}
```

### Helper Function: Check Config Value
```bash
check_config_value() {
  local config_file=$1
  local key=$2
  local default_value=$3

  if [ ! -f "$config_file" ]; then
    echo "$default_value"
    return 1
  fi

  # Extract value from YAML (simple grep-based parser)
  VALUE=$(grep "^\s*$key:" "$config_file" | awk '{print $2}' | tr -d '"' | tr -d "'" || echo "$default_value")
  echo "$VALUE"
  return 0
}
```

---

## Step 0: Environment Setup & Prerequisites

### 0.1: Verify Prerequisites

Ensure the following tools are installed and configured:

**Required:**
- TD CLI (Treasure Data CLI) - Already installed ✅
- Python 3.11+ - For workflow automation
- Node.js 22+ - For TDX CLI (if available)

**Verification:**
```bash
# Verify TD CLI
td --version

# Verify Python
python3 --version

# Verify Node.js
node --version

# Verify TD authentication
td account
```

**Optional:**
- TDX CLI - For advanced workflow management (if available)
- jq - For JSON parsing in validation scripts
- TD MCP Server - For database schema extraction

**Note:** This guide works with TD CLI. TDX CLI commands are shown for reference but can be adapted to TD CLI or TD Console UI.

### 0.2: Gather Requirements from User

**Ask the user these questions:**

1. **Client/Subscriber name** (used as `sub` prefix)
   - Example: `acme_retail`
   - Used for: Database naming (`stg_acme_retail`, `gldn_acme_retail`)

2. **Project name** (used for TD workflow project)
   - Example: `acme-retail-cdp`
   - Used for: Workflow project name in TD Console

3. **Source database name** (existing TD database with raw data)
   - Example: `raw_acme_retail`
   - Must exist in TD account

4. **Unification ID name** (the unified customer identifier)
   - Example: `canonical_id`, `td_unified_id`, `master_id`
   - Recommended: `canonical_id`

5. **IDU Dashboard** (optional quality dashboard)
   - Ask: "Do you want to include IDU (ID Unification) dashboard for quality monitoring?"
   - Options: yes/no
   - **Store choice**: `include_idu_dashboard: true/false`
   - If no: Will skip IDU dashboard deployment entirely

6. **Analytics Dashboard** (optional business intelligence)
   - Ask: "Do you want to include analytics dashboards (sales, web, etc.)?"
   - Options: yes/no
   - **Store choice**: `include_analytics_dashboard: true/false`
   - If no: Will skip analytics dashboard deployment entirely

7. **Dashboard user emails** (for access grants, if dashboards selected)
   - Example: `['user@company.com', 'analyst@company.com']`
   - Used for: Dashboard sharing
   - **Only ask if** `include_idu_dashboard: true` OR `include_analytics_dashboard: true`

8. **Notification emails** (for workflow alerts)
   - Example: `['ops@company.com']`
   - Used for: Success/error notifications

9. **Scheduling** (when to run the pipeline)
   - Options:
     - Off (manual runs only)
     - Daily at 4 AM ET
     - Custom cron schedule
   - Example: `cron>: "0 4 * * *"` (daily 4 AM UTC)

### 0.3: Create Project Structure

```bash
# Navigate to AI_orchestratin directory (adjust path as needed)
cd /Users/gandhi.yellapu/AI_orchestratin

# Create project directory
mkdir -p <sub>_workflow
cd <sub>_workflow

# Copy workflow template (if available - optional, can create manually)
# cp -r ../workflow-skills/workflow-template/* .

# Create working directories
mkdir -p .poc-state/profiles
mkdir -p staging/queries
mkdir -p golden/queries/attributes
mkdir -p unification
mkdir -p segment/config/parent_segment_templates
mkdir -p config

# Create optional directories based on user choice
if [ "$include_idu_dashboard" = "true" ]; then
  mkdir -p idu_dashboard/queries
  mkdir -p idu_dashboard/python_files
fi

if [ "$include_analytics_dashboard" = "true" ]; then
  mkdir -p analytics/queries
  mkdir -p analytics/dashboard
fi

echo "✅ Project structure created successfully"
```

### 0.4: Initialize Configuration

Create `config/src_params.yml`:

```yaml
# Project configuration
sub: <client_name>
project_name: <project_name>

# Source configuration
source_database: <source_database_name>

# Target databases
src: <source_database_name>
stg: stg
gld: gldn
analytics: analytics

# TD Configuration
site: us01  # or eu01, ap02, etc.

# Unification configuration
unification_id: <unification_id_name>

# Workflow configuration
run_all: false  # Set to true to force re-run all steps

# Optional workflows (set based on user choice)
optional_workflows:
  include_idu_dashboard: <true/false>
  include_analytics_dashboard: <true/false>

# Segment configuration
segment:
  run_type: "create"
  tables:
    parent_segment_templates: ps_templates
    parent_segment_creation: ps_creation_log
    active_audience: active_audiences

# Notification emails
notification_emails:
  - <email1>
  - <email2>

# Dashboard user emails (if dashboards enabled)
dashboard_users:
  - <email1>
  - <email2>
```

---

## Step 1: Data Profiling (Discovery & Analysis)

**Skill:** `profiling_skill.md`

**Goal:** Understand source data structure, detect PII, identify join keys, assess quality.

### 1.1: Execute Profiling

Run through `profiling_skill.md`:
- List all tables in source database
- Get schema for each table
- Run data quality queries (null rates, duplicates)
- Detect PII (email, phone, name, dates)
- Identify join keys across tables
- Classify time columns (event vs import)
- Assess JSON/ARRAY fields

### 1.2: Review Profiling Report

Present `profiling-report.md` to user:
```
📊 Profiling Results:

Source Database: <database>
Tables Found: <count>
Total Rows: <count>

Tables with Customer Data:
- customers: <row_count> rows, PII detected: email, phone, name
- orders: <row_count> rows, PII detected: customer_email
- consents: <row_count> rows, PII detected: email, phone
- pageviews: <row_count> rows, PII detected: user_email

Recommended Transformations:
- <N> email columns → trfmd_email
- <N> phone columns → trfmd_phone
- <N> name columns → trfmd_first_name, trfmd_last_name
- <N> date columns → trfmd_*_unix

Recommended Join Keys:
1. customer_id (coverage: <pct>%)
2. email (coverage: <pct>%)
3. phone (coverage: <pct>%)

Ready to proceed to staging? (yes/no)
```

### 1.3: User Approval

Get user confirmation:
- Review profiling findings
- Approve transformations
- Confirm tables to include in staging
- Approve unification keys

Store approved choices in `profiling-choices.json`.

### 1.4: Checkpoint

✅ Profiling complete
- [ ] profiling-report.md created
- [ ] profiling-choices.json created
- [ ] User approved transformations
- [ ] Ready for staging

---

## Step 2: Staging (Data Cleaning)

**Skill:** `staging_skill.md`

**Goal:** Create clean staging tables with standardized `trfmd_*` columns.

**IMPORTANT:** This step requires Step 1 (Profiling) to be completed successfully.

### 2.0: **PRE-CHECK: Validate Previous Step**

**CRITICAL: Verify profiling completed before proceeding to staging**

```bash
echo "=========================================="
echo "Step 2: Staging - Pre-flight Validation"
echo "=========================================="

# Check profiling report exists
check_file_exists "profiling-report.md" "Profiling report" || exit 1

# Check profiling choices exists
check_file_exists "profiling-choices.json" "Profiling choices" || exit 1

echo "✅ All pre-flight checks passed. Proceeding to staging..."
```

### 2.1: Generate Staging SQL Queries

Run through `staging_skill.md`:
- Read `profiling-choices.json`
- Generate staging SQL for each table
- Apply transformations (email, phone, name, date, etc.)
- Create `staging/queries/<table>.sql` for each table
- Create `staging/queries/invalid_emails.sql` for quality checks

### 2.2: Update Workflow Files

- Update `wf2_stage.dig` with table list
- Update `config/src_params.yml` with staging tables

### 2.3: Push to TD Console

```bash
cd <sub>_workflow

# Push workflow
tdx wf push <project_name>

# Verify upload
tdx wf list <project_name> | grep staging
```

**Get user confirmation:**
```
Ready to push staging workflow to TD Console?
Tables to stage: <list>
Target database: stg_<sub>

Push? (yes/no)
```

### 2.4: Run Staging Workflow

```bash
# Run staging
tdx wf run <project_name> wf2_stage

# Get session ID
SESSION_ID=<returned_id>
echo "Staging session ID: $SESSION_ID"

# Monitor
tdx wf session $SESSION_ID
```

Monitor status every 30 seconds until success or error.

### 2.5: **POST-VALIDATION: Check Workflow Success**

**CRITICAL: Verify staging workflow completed successfully**

```bash
echo "=========================================="
echo "Step 2: Staging - Post-execution Validation"
echo "=========================================="

# Check workflow status
check_workflow_status "$SESSION_ID" "Staging" || {
  echo ""
  echo "🛑 STAGING FAILED - CANNOT PROCEED"
  echo "Action required:"
  echo "1. Review error logs: tdx wf session $SESSION_ID"
  echo "2. Fix the failing queries in staging/queries/"
  echo "3. Push updates: tdx wf push <project_name>"
  echo "4. Re-run: tdx wf run <project_name> wf2_stage"
  exit 1
}

echo "✅ Staging validation passed!"
```

### 2.6: Handle Errors (if any)

If workflow fails:
1. Get error message from logs: `tdx wf session $SESSION_ID`
2. Identify failing table/query
3. Fix `staging/queries/<table>.sql`
4. Push updated workflow: `tdx wf push <project_name>`
5. Re-run workflow: `tdx wf run <project_name> wf2_stage`
6. Repeat until success

### 2.7: Validate Results

```bash
# Check staging database exists
check_database_exists "stg_<sub>" "Staging" || exit 1

# Check staging tables created
tdx tables stg_<sub>

# Check row counts
tdx query "SELECT COUNT(*) FROM stg_<sub>.customers"

# Verify transformations
tdx query "SELECT email, trfmd_email, trfmd_email_status FROM stg_<sub>.customers LIMIT 10"
```

### 2.8: Generate Staging Report

Present `staging-report.md` to user:
```
✓ Staging Complete!

Staging Database: stg_<sub>
Tables Created: <count>
Total Rows: <count>

Tables:
- customers: <rows>, transformations: email, phone, name
- orders: <rows>, transformations: email
- consents: <rows>, transformations: email, phone

Email Quality: <valid_pct>% valid
Phone Quality: <valid_pct>% valid

Ready to proceed to unification? (yes/no)
```

### 2.9: Checkpoint

✅ Staging complete
- [ ] Staging workflow status: **success** ✅
- [ ] All staging tables created in `stg_<sub>`
- [ ] Transformations validated
- [ ] staging-report.md created
- [ ] User confirmed ready for unification

---

## Step 3: Unification (Identity Resolution)

**Skill:** `unification_skill.md`

**Goal:** Match customer records across tables to create unified customer IDs.

**IMPORTANT:** This step requires Step 2 (Staging) to be completed successfully.

### 3.0: **PRE-CHECK: Validate Previous Step**

**CRITICAL: Verify staging completed successfully before proceeding to unification**

```bash
echo "=========================================="
echo "Step 3: Unification - Pre-flight Validation"
echo "=========================================="

# Check staging database exists
check_database_exists "stg_<sub>" "Staging" || exit 1

# Check staging report exists
check_file_exists "staging-report.md" "Staging report" || exit 1

# Verify key staging tables exist
echo "Verifying required staging tables..."
REQUIRED_TABLES=("customers" "orders")  # Adjust based on your requirements

for table in "${REQUIRED_TABLES[@]}"; do
  check_table_exists "stg_<sub>" "$table" "Required staging" || {
    echo "⚠️  WARNING: Table stg_<sub>.$table not found."
    echo "   Continuing, but unification may have limited data."
  }
done

echo "✅ All pre-flight checks passed. Proceeding to unification..."
```

### 3.1: Configure Unification

Run through `unification_skill.md`:
- Analyze unification strategy
- Define primary/secondary keys
- Configure `unification/unify.yml`
- Set match rules
- Define survivorship rules

### 3.2: Add TD API Key Secret

```bash
# Add API key as secret
td wf secret set <project_name> td.apikey

# Enter API key when prompted
# Format: TD1 <account_id>/<api_key>
```

### 3.3: Push to TD Console

```bash
# Push unification workflow
tdx wf push <project_name>

# Verify
tdx wf list <project_name> | grep unif
```

**Get user confirmation:**
```
Ready to run unification?
Unification ID: <unification_id>
Match rules: customer_id (exact), email (exact), phone (exact)
Expected duration: 30 min - 2 hours

Run unification? (yes/no)
```

### 3.4: Run Unification Workflow

```bash
# Run unification
tdx wf run <project_name> wf3_unify

# Get session ID
SESSION_ID=<returned_id>
echo "Unification session ID: $SESSION_ID"

# Monitor (check every 2 minutes due to long runtime)
tdx wf session $SESSION_ID
```

This is a long-running process. Monitor until completion.

### 3.5: **POST-VALIDATION: Check Workflow Success**

**CRITICAL: Verify unification workflow completed successfully**

```bash
echo "=========================================="
echo "Step 3: Unification - Post-execution Validation"
echo "=========================================="

# Check workflow status
check_workflow_status "$SESSION_ID" "Unification" || {
  echo ""
  echo "🛑 UNIFICATION FAILED - CANNOT PROCEED"
  echo "Action required:"
  echo "1. Review error logs: tdx wf session $SESSION_ID"
  echo "2. Common issues:"
  echo "   - Missing TD API key: td wf secret set <project_name> td.apikey"
  echo "   - Invalid unify.yml: Check table/column names"
  echo "   - Staging data issues: Verify staging tables have data"
  echo "3. Fix unification/unify.yml"
  echo "4. Push updates: tdx wf push <project_name>"
  echo "5. Re-run: tdx wf run <project_name> wf3_unify"
  exit 1
}

echo "✅ Unification validation passed!"
```

### 3.6: Handle Errors (if any)

If workflow fails:
1. Get error from session logs: `tdx wf session $SESSION_ID`
2. Fix `unification/unify.yml`
3. Push updated workflow: `tdx wf push <project_name>`
4. Re-run: `tdx wf run <project_name> wf3_unify`
5. Repeat until success

### 3.7: Validate Unification Results

```bash
# Check unification database exists
check_database_exists "cdp_unif_<sub>" "Unification" || exit 1

# Check unified customer count
tdx query "SELECT COUNT(DISTINCT ${unification_id}) FROM cdp_unif_<sub>.${unification_id}_master"

# Check coverage
tdx query "
SELECT
  source_table,
  COUNT(*) as records,
  COUNT(${unification_id}) as unified,
  ROUND(100.0 * COUNT(${unification_id}) / COUNT(*), 2) as coverage_pct
FROM cdp_unif_<sub>.${unification_id}_lookup
GROUP BY 1
"

# Check for over-merging
tdx query "
SELECT
  ${unification_id},
  COUNT(DISTINCT source_id) as num_merged
FROM cdp_unif_<sub>.${unification_id}_lookup
GROUP BY 1
HAVING COUNT(DISTINCT source_id) > 20
LIMIT 10
"
```

### 3.8: Generate Unification Report

Present `unification-report.md` to user:
```
✓ Unification Complete!

Unified Customers: <count>
Unification Coverage: <pct>%
Average Merge Rate: <avg> source IDs per customer

Match Statistics:
- customer_id matches: <pct>%
- email matches: <pct>%
- phone matches: <pct>%

Quality Check:
- Over-merging: <status>
- Coverage: <status>

Ready to proceed to golden layer? (yes/no)
```

### 3.9: Checkpoint

✅ Unification complete
- [ ] Unification workflow status: **success** ✅
- [ ] Unification database created: `cdp_unif_<sub>`
- [ ] Master table exists: `${unification_id}_master`
- [ ] Coverage validated (>95%)
- [ ] No critical over-merging detected
- [ ] unification-report.md created
- [ ] User confirmed ready for golden or IDU dashboard

---

## Step 3.5: IDU Dashboard (OPTIONAL)

**ONLY RUN IF**: User requested IDU dashboard in Step 0.2

**Skill:** `idu_dashboard_skill.md`

**Goal:** Deploy ID Unification quality monitoring dashboard.

### 3.5.0: **Check if IDU Dashboard is Required**

```bash
echo "=========================================="
echo "Step 3.5: IDU Dashboard - Checking if required"
echo "=========================================="

# Read config
INCLUDE_IDU=$(check_config_value "config/src_params.yml" "include_idu_dashboard" "false")

if [ "$INCLUDE_IDU" != "true" ]; then
  echo "ℹ️  IDU Dashboard not requested by user."
  echo "   Skipping Step 3.5 - IDU Dashboard deployment."
  echo "   Proceeding to Step 4: Golden Layer..."
  exit 0
fi

echo "✅ IDU Dashboard requested. Proceeding with deployment..."
```

### 3.5.1: **PRE-CHECK: Validate Unification Completed**

```bash
echo "=========================================="
echo "Step 3.5: IDU Dashboard - Pre-flight Validation"
echo "=========================================="

# Verify unification database exists
check_database_exists "cdp_unif_<sub>" "Unification" || {
  echo "Cannot deploy IDU dashboard without unification."
  exit 1
}

# Verify unification master table exists
check_table_exists "cdp_unif_<sub>" "${unification_id}_master" "Unification master" || {
  echo "Cannot deploy IDU dashboard without unification master table."
  exit 1
}

echo "✅ Unification validation passed. Deploying IDU dashboard..."
```

### 3.5.2: Configure IDU Dashboard

Run through `idu_dashboard_skill.md`:
- Generate IDU quality queries
- Create dashboard configuration
- Set up data model

### 3.5.3: Push IDU Dashboard Workflow

```bash
# Push IDU dashboard workflow
tdx wf push <project_name>

# Verify
tdx wf list <project_name> | grep idu
```

### 3.5.4: Run IDU Dashboard Workflow

```bash
# Run IDU dashboard deployment
tdx wf run <project_name> wf_idu_dashboard

# Get session ID
SESSION_ID=<returned_id>
echo "IDU Dashboard session ID: $SESSION_ID"

# Monitor
tdx wf session $SESSION_ID
```

### 3.5.5: **POST-VALIDATION: Check Workflow Success**

```bash
echo "=========================================="
echo "Step 3.5: IDU Dashboard - Post-execution Validation"
echo "=========================================="

# Check workflow status
check_workflow_status "$SESSION_ID" "IDU Dashboard" || {
  echo ""
  echo "⚠️  WARNING: IDU Dashboard deployment failed"
  echo "This is an optional component. Proceeding to Golden layer."
  echo "You can retry IDU dashboard deployment later if needed."
  exit 0  # Don't block pipeline for optional component
}

echo "✅ IDU Dashboard deployed successfully!"
```

### 3.5.6: Checkpoint

✅ IDU Dashboard status
- [ ] IDU dashboard workflow: **success** / **skipped** / **failed (non-blocking)**
- [ ] Dashboard accessible in TD Console (if successful)
- [ ] Proceeding to golden layer

---

## Step 4: Golden Layer (Single Customer View)

**Skill:** `golden_skill.md`

**Goal:** Create unified golden database with master identity + attributes + behaviors.

**IMPORTANT:** This step requires Step 3 (Unification) to be completed successfully.

### 4.0: **PRE-CHECK: Validate Previous Step**

**CRITICAL: Verify unification completed successfully before proceeding to golden layer**

```bash
echo "=========================================="
echo "Step 4: Golden Layer - Pre-flight Validation"
echo "=========================================="

# Check unification database exists
check_database_exists "cdp_unif_<sub>" "Unification" || exit 1

# Check unification master table exists
check_table_exists "cdp_unif_<sub>" "${unification_id}_master" "Unification master" || exit 1

# Check unification report exists
check_file_exists "unification-report.md" "Unification report" || exit 1

# Verify staging database still exists (needed for golden queries)
check_database_exists "stg_<sub>" "Staging" || exit 1

echo "✅ All pre-flight checks passed. Proceeding to golden layer..."
```

### 4.1: Generate Golden SQL Queries

Run through `golden_skill.md`:
- Create `golden/queries/all_profile_identifiers.sql`
- Create `golden/queries/copy_enriched_table.sql`
- Create row-level table queries (orders, consents, etc.)
- Create attribute table queries (transactions, email_activity, pageviews, etc.)

### 4.2: Update Workflow

Update `wf5_golden.dig` with table lists.

### 4.3: Push to TD Console

```bash
# Push golden workflow
tdx wf push <project_name>

# Verify
tdx wf list <project_name> | grep golden
```

**Get user confirmation:**
```
Ready to build golden layer?
Master table: profile_identifiers
Attributes: transactions, email_activity, pageviews, etc.
Behaviors: orders, consents, etc.
Target database: gldn_<sub>

Build golden layer? (yes/no)
```

### 4.4: Run Golden Workflow

```bash
# Run golden
tdx wf run <project_name> wf5_golden

# Get session ID
SESSION_ID=<returned_id>
echo "Golden layer session ID: $SESSION_ID"

# Monitor
tdx wf session $SESSION_ID
```

### 4.5: **POST-VALIDATION: Check Workflow Success**

**CRITICAL: Verify golden workflow completed successfully**

```bash
echo "=========================================="
echo "Step 4: Golden Layer - Post-execution Validation"
echo "=========================================="

# Check workflow status
check_workflow_status "$SESSION_ID" "Golden Layer" || {
  echo ""
  echo "🛑 GOLDEN LAYER FAILED - CANNOT PROCEED"
  echo "Action required:"
  echo "1. Review error logs: tdx wf session $SESSION_ID"
  echo "2. Common issues:"
  echo "   - Missing ${unification_id} column: Check unification completed"
  echo "   - Aggregation errors: Verify GROUP BY clauses"
  echo "   - JOIN errors: Check table/column names"
  echo "3. Fix golden/queries/*.sql"
  echo "4. Push updates: tdx wf push <project_name>"
  echo "5. Re-run: tdx wf run <project_name> wf5_golden"
  exit 1
}

echo "✅ Golden layer validation passed!"
```

### 4.6: Handle Errors (if any)

If workflow fails:
1. Get error from logs: `tdx wf session $SESSION_ID`
2. Fix golden SQL queries in `golden/queries/`
3. Push updated workflow: `tdx wf push <project_name>`
4. Re-run: `tdx wf run <project_name> wf5_golden`
5. Repeat until success

### 4.7: Validate Golden Results

```bash
# Check golden database exists
check_database_exists "gldn_<sub>" "Golden" || exit 1

# Check golden tables
tdx tables gldn_<sub>

# Check master identity count
tdx query "SELECT COUNT(*) FROM gldn_<sub>.profile_identifiers"

# Check attribute tables
tdx query "SELECT COUNT(*) FROM gldn_<sub>.attr_transactions"

# Verify data
tdx query "
SELECT
  p.${unification_id},
  p.trfmd_email,
  t.total_orders,
  t.lifetime_revenue
FROM gldn_<sub>.profile_identifiers p
LEFT JOIN gldn_<sub>.attr_transactions t USING (${unification_id})
LIMIT 10
"
```

### 4.8: Generate Golden Report

Present `golden-report.md` to user:
```
✓ Golden Layer Complete!

Golden Database: gldn_<sub>
Total Customers: <count>

Tables Created:
- profile_identifiers: <count> customers
- attr_transactions: <count> customers (<pct>% coverage)
- attr_email_activity: <count> customers (<pct>% coverage)
- attr_pageviews: <count> customers (<pct>% coverage)
- orders: <count> rows
- consents: <count> rows

Ready to proceed to segmentation? (yes/no)
```

### 4.9: Checkpoint

✅ Golden layer complete
- [ ] Golden workflow status: **success** ✅
- [ ] Golden database created: `gldn_<sub>`
- [ ] Master identity table: `profile_identifiers`
- [ ] All attribute tables created
- [ ] All row-level tables created
- [ ] golden-report.md created
- [ ] User confirmed ready for segmentation or analytics

---

## Step 4.5: Analytics Dashboard (OPTIONAL)

**ONLY RUN IF**: User requested analytics dashboard in Step 0.2

**Skill:** `analytics_skill.md`

**Goal:** Deploy business intelligence analytics dashboards.

### 4.5.0: **Check if Analytics Dashboard is Required**

```bash
echo "=========================================="
echo "Step 4.5: Analytics Dashboard - Checking if required"
echo "=========================================="

# Read config
INCLUDE_ANALYTICS=$(check_config_value "config/src_params.yml" "include_analytics_dashboard" "false")

if [ "$INCLUDE_ANALYTICS" != "true" ]; then
  echo "ℹ️  Analytics Dashboard not requested by user."
  echo "   Skipping Step 4.5 - Analytics Dashboard deployment."
  echo "   Proceeding to Step 5: Segmentation..."
  exit 0
fi

echo "✅ Analytics Dashboard requested. Proceeding with deployment..."
```

### 4.5.1: **PRE-CHECK: Validate Golden Layer Completed**

```bash
echo "=========================================="
echo "Step 4.5: Analytics Dashboard - Pre-flight Validation"
echo "=========================================="

# Verify golden database exists
check_database_exists "gldn_<sub>" "Golden" || {
  echo "Cannot deploy analytics dashboard without golden layer."
  exit 1
}

# Verify golden tables exist
check_table_exists "gldn_<sub>" "profile_identifiers" "Golden master" || {
  echo "Cannot deploy analytics dashboard without golden tables."
  exit 1
}

echo "✅ Golden layer validation passed. Deploying analytics dashboard..."
```

### 4.5.2: Configure Analytics Dashboard

Run through `analytics_skill.md`:
- Generate analytics queries
- Create dashboard configuration
- Set up data models (sales, web, etc.)

### 4.5.3: Push Analytics Dashboard Workflow

```bash
# Push analytics dashboard workflow
tdx wf push <project_name>

# Verify
tdx wf list <project_name> | grep analytics
```

### 4.5.4: Run Analytics Dashboard Workflow

```bash
# Run analytics dashboard deployment
tdx wf run <project_name> wf_analytics

# Get session ID
SESSION_ID=<returned_id>
echo "Analytics Dashboard session ID: $SESSION_ID"

# Monitor
tdx wf session $SESSION_ID
```

### 4.5.5: **POST-VALIDATION: Check Workflow Success**

```bash
echo "=========================================="
echo "Step 4.5: Analytics Dashboard - Post-execution Validation"
echo "=========================================="

# Check workflow status
check_workflow_status "$SESSION_ID" "Analytics Dashboard" || {
  echo ""
  echo "⚠️  WARNING: Analytics Dashboard deployment failed"
  echo "This is an optional component. Proceeding to Segmentation."
  echo "You can retry analytics dashboard deployment later if needed."
  exit 0  # Don't block pipeline for optional component
}

echo "✅ Analytics Dashboard deployed successfully!"
```

### 4.5.6: Checkpoint

✅ Analytics Dashboard status
- [ ] Analytics dashboard workflow: **success** / **skipped** / **failed (non-blocking)**
- [ ] Dashboard accessible in TD Console (if successful)
- [ ] Proceeding to segmentation

---

## Step 5: Segmentation (Parent Segment)

**Skill:** `segmentation_skill.md`

**Goal:** Create parent segment for audience building and activation.

**IMPORTANT:** This step requires Step 4 (Golden Layer) to be completed successfully.

### 5.0: **PRE-CHECK: Validate Previous Step**

**CRITICAL: Verify golden layer completed successfully before proceeding to segmentation**

```bash
echo "=========================================="
echo "Step 5: Segmentation - Pre-flight Validation"
echo "=========================================="

# Check golden database exists
check_database_exists "gldn_<sub>" "Golden" || exit 1

# Check master identity table exists
check_table_exists "gldn_<sub>" "profile_identifiers" "Master identity" || exit 1

# Check golden report exists
check_file_exists "golden-report.md" "Golden report" || exit 1

# Verify key attribute tables exist
echo "Verifying attribute tables..."
EXPECTED_ATTR_TABLES=("attr_transactions")  # Adjust based on your setup

for table in "${EXPECTED_ATTR_TABLES[@]}"; do
  if ! check_table_exists "gldn_<sub>" "$table" "Golden attribute" 2>/dev/null; then
    echo "⚠️  WARNING: Expected attribute table gldn_<sub>.$table not found."
    echo "   Parent segment may have limited attributes."
  fi
done

echo "✅ All pre-flight checks passed. Proceeding to segmentation..."
```

### 5.1: Configure Parent Segment

Run through `segmentation_skill.md`:
- Read golden table schemas
- Configure `segment/config/parent_segment_templates/retail_parent_segment_template.yml`
- Map attributes (identity, demographics, purchase, engagement, web)
- Map behaviors (orders, consents, etc.)
- Validate configuration

### 5.2: Push to TD Console

```bash
# Push segment workflow
tdx wf push <project_name>

# Verify
tdx wf list <project_name> | grep segment
```

**Get user confirmation:**
```
Ready to create parent segment?
Master table: gldn_<sub>.profile_identifiers
Attributes: <count>
Behaviors: <count>

Create parent segment? (yes/no)
```

### 5.3: Run Segmentation Workflow

```bash
# Run segmentation
tdx wf run <project_name> wf7_segment

# Get session ID
SESSION_ID=<returned_id>
echo "Segmentation session ID: $SESSION_ID"

# Monitor
tdx wf session $SESSION_ID
```

### 5.4: **POST-VALIDATION: Check Workflow Success**

**CRITICAL: Verify segmentation workflow completed successfully**

```bash
echo "=========================================="
echo "Step 5: Segmentation - Post-execution Validation"
echo "=========================================="

# Check workflow status
check_workflow_status "$SESSION_ID" "Segmentation" || {
  echo ""
  echo "🛑 SEGMENTATION FAILED"
  echo "Action required:"
  echo "1. Review error logs: tdx wf session $SESSION_ID"
  echo "2. Common issues:"
  echo "   - Missing TD API key: td wf secret set <project_name> td_apikey"
  echo "   - Invalid parent segment YAML: Check table/column names"
  echo "   - Data type mismatches: Verify column types match YAML"
  echo "3. Fix segment/config/parent_segment_templates/*.yml"
  echo "4. Push updates: tdx wf push <project_name>"
  echo "5. Re-run: tdx wf run <project_name> wf7_segment"
  exit 1
}

echo "✅ Segmentation validation passed!"
echo "✅ Parent segment created successfully!"
```

### 5.5: Handle Errors (if any)

If workflow fails:
1. Get error from logs: `tdx wf session $SESSION_ID`
2. Fix parent segment YAML in `segment/config/parent_segment_templates/`
3. Push updated workflow: `tdx wf push <project_name>`
4. Re-run: `tdx wf run <project_name> wf7_segment`
5. Repeat until success

### 5.6: Verify Parent Segment

```bash
# List parent segments
tdx ps list

# Get details
PARENT_SEGMENT_ID=<parent_segment_id>
tdx ps get $PARENT_SEGMENT_ID

# Preview data
tdx ps preview $PARENT_SEGMENT_ID --limit 100
```

### 5.7: Create Test Segments

```bash
# Test segment: High-value customers
tdx sg create \
  --parent-segment $PARENT_SEGMENT_ID \
  --name "High Value Customers" \
  --filter "lifetime_revenue > 1000"

# Test segment: Recent purchasers
tdx sg create \
  --parent-segment $PARENT_SEGMENT_ID \
  --name "Recent Purchasers" \
  --filter "days_since_last_purchase < 30"
```

### 5.8: Generate Segmentation Report

Present `segmentation-report.md` to user:
```
✓ Segmentation Complete!

Parent Segment ID: <parent_segment_id>
Total Profiles: <count>
Attributes: <count>
Behaviors: <count>

Test Segments Created:
- High Value Customers: <count>
- Recent Purchasers: <count>

Your CDP pipeline is complete! ✅
```

### 5.9: Checkpoint

✅ Segmentation complete
- [ ] Segmentation workflow status: **success** ✅
- [ ] Parent segment created: `<parent_segment_id>`
- [ ] Test segments validated
- [ ] segmentation-report.md created
- [ ] **Pipeline is fully operational** 🎉

---

## Step 6: Final Validation & Handoff

### 6.1: Run End-to-End Validation

**Verify all pipeline components are operational:**

```bash
echo "=========================================="
echo "Final Pipeline Validation"
echo "=========================================="

# 1. Check all databases exist
echo ""
echo "1. Verifying databases..."
check_database_exists "stg_<sub>" "Staging" || echo "⚠️  Staging database issue"
check_database_exists "cdp_unif_<sub>" "Unification" || echo "⚠️  Unification database issue"
check_database_exists "gldn_<sub>" "Golden" || echo "⚠️  Golden database issue"

# 2. Check data flow
echo ""
echo "2. Validating data flow..."

# Get counts
if command -v jq &> /dev/null; then
  STAGING_COUNT=$(tdx query "SELECT COUNT(*) as cnt FROM stg_<sub>.customers" --format json 2>/dev/null | jq -r '.[0].cnt' || echo "0")
  UNIFIED_COUNT=$(tdx query "SELECT COUNT(DISTINCT ${unification_id}) as cnt FROM cdp_unif_<sub>.${unification_id}_master" --format json 2>/dev/null | jq -r '.[0].cnt' || echo "0")
  GOLDEN_COUNT=$(tdx query "SELECT COUNT(*) as cnt FROM gldn_<sub>.profile_identifiers" --format json 2>/dev/null | jq -r '.[0].cnt' || echo "0")
else
  # Fallback without jq
  STAGING_COUNT=$(tdx query "SELECT COUNT(*) as cnt FROM stg_<sub>.customers" 2>/dev/null | tail -1 | awk '{print $1}' || echo "0")
  UNIFIED_COUNT=$(tdx query "SELECT COUNT(DISTINCT ${unification_id}) as cnt FROM cdp_unif_<sub>.${unification_id}_master" 2>/dev/null | tail -1 | awk '{print $1}' || echo "0")
  GOLDEN_COUNT=$(tdx query "SELECT COUNT(*) as cnt FROM gldn_<sub>.profile_identifiers" 2>/dev/null | tail -1 | awk '{print $1}' || echo "0")
fi

echo "   Staging customers: $STAGING_COUNT"
echo "   Unified customers: $UNIFIED_COUNT"
echo "   Golden customers: $GOLDEN_COUNT"

# 3. Validation summary
echo ""
echo "3. Validation Summary:"
if [ "$STAGING_COUNT" -gt 0 ] && [ "$UNIFIED_COUNT" -gt 0 ] && [ "$GOLDEN_COUNT" -gt 0 ]; then
  echo "   ✅ All validation checks passed!"
  echo "   ✅ Data flow is healthy across all layers!"
else
  echo "   ⚠️  WARNING: Some counts are zero or validation failed."
  echo "   Please investigate data flow issues."
fi

echo ""
echo "=========================================="
```

### 6.2: Generate Final Summary

Create `pipeline-summary.md`:

```markdown
# CDP Pipeline Summary - <project_name>

Generated: $(date)

## Configuration
- Client: <sub>
- Project: <project_name>
- Source Database: <source_database>
- Unification ID: ${unification_id}

## Pipeline Status: ✅ COMPLETE

### Workflow Execution Summary

| Step | Workflow | Status | Duration | Tables/Objects Created |
|------|----------|--------|----------|------------------------|
| 1. Profiling | N/A | ✅ Success | <duration> | profiling-report.md, profiling-choices.json |
| 2. Staging | wf2_stage | ✅ Success | <duration> | <N> tables in stg_<sub> |
| 3. Unification | wf3_unify | ✅ Success | <duration> | cdp_unif_<sub> database |
| 3.5. IDU Dashboard | wf_idu_dashboard | <✅ Success / ⊘ Skipped> | <duration> | IDU dashboard (if enabled) |
| 4. Golden Layer | wf5_golden | ✅ Success | <duration> | <N> tables in gldn_<sub> |
| 4.5. Analytics | wf_analytics | <✅ Success / ⊘ Skipped> | <duration> | Analytics dashboards (if enabled) |
| 5. Segmentation | wf7_segment | ✅ Success | <duration> | Parent segment <id> |

### Databases Created
- `stg_<sub>` - Staging (clean data): <count> customers
- `cdp_unif_<sub>` - Unification (unified IDs): <count> unified customers
- `gldn_<sub>` - Golden (single customer view): <count> customers

### Data Summary
- Total Customers: <count>
- Staging Tables: <count>
- Golden Attribute Tables: <count>
- Golden Behavior Tables: <count>
- Parent Segment Attributes: <count>
- Parent Segment Behaviors: <count>

### Parent Segment
- ID: <parent_segment_id>
- Profiles: <count>
- Ready for activation: ✅

### Optional Components
- IDU Dashboard: <Deployed ✅ / Not Requested ⊘>
- Analytics Dashboards: <Deployed ✅ / Not Requested ⊘>

## Validation Results

### Data Flow Health Check
- Staging → Unification: ✅ Healthy
- Unification → Golden: ✅ Healthy
- Golden → Parent Segment: ✅ Healthy

### Coverage Metrics
- Unification Coverage: >95% ✅
- Golden Attribute Coverage: <pct>%
- Parent Segment Population: 100% ✅

## Next Steps
1. ✅ Create custom audience segments in TD Console
2. ✅ Activate segments to marketing channels (Facebook, Google Ads, etc.)
3. ✅ Build customer journeys using Audience Studio
4. ✅ Monitor and optimize segment performance

## Generated Reports
- Profiling Report: `profiling-report.md`
- Staging Report: `staging-report.md`
- Unification Report: `unification-report.md`
- Golden Report: `golden-report.md`
- Segmentation Report: `segmentation-report.md`
- **Pipeline Summary**: `pipeline-summary.md` (this file)

## Support Resources
- TD Console: https://console.treasuredata.com
- Documentation: https://docs.treasuredata.com
- Support: support@treasuredata.com

---
**Pipeline Deployment Status: SUCCESSFUL ✅**
```

### 6.3: User Handoff

Present final message:
```
========================================
🎉 CDP PIPELINE DEPLOYMENT COMPLETE! 🎉
========================================

Your Treasure Data CDP Pipeline is successfully deployed and running!

Pipeline Summary:
├── Customers: <count>
├── Databases: stg_<sub>, cdp_unif_<sub>, gldn_<sub>
├── Parent Segment: <parent_segment_id>
└── Test Segments: 2 created ✅

Optional Components:
├── IDU Dashboard: <Deployed ✅ / Not Deployed ⊘>
└── Analytics Dashboard: <Deployed ✅ / Not Deployed ⊘>

What you can do now:
1. Create audience segments in TD Console
2. Activate segments to Facebook, Google Ads, etc.
3. Build customer journeys
4. Run analytics on your unified data

Documentation Generated:
├── profiling-report.md
├── staging-report.md
├── unification-report.md
├── golden-report.md
├── segmentation-report.md
└── pipeline-summary.md ← Complete overview

Need help?
- TD Console: https://console.treasuredata.com
- Documentation: https://docs.treasuredata.com
- Support: support@treasuredata.com

========================================
Thank you for using TD CDP Pipeline Automation! ✅
========================================
```

---

## Error Recovery

If any step fails:
1. **Don't panic** - Errors are normal, especially on first runs
2. **Get logs** - Use `tdx wf session <SESSION_ID>` to view detailed logs
3. **Identify root cause** - Find the specific failing query or config
4. **Fix the issue** - Update SQL, YAML, or workflow file
5. **Push updates** - `tdx wf push <project_name>`
6. **Re-run** - `tdx wf run <project_name> <workflow>`
7. **Validate** - Confirm success before proceeding to next step
8. **Repeat** - Keep iterating until success

**⚠️ CRITICAL:** Do NOT proceed to the next step until the current step shows **success** status in validation checks.

### Common Issues & Solutions

| Error | Cause | Fix |
|-------|-------|-----|
| Column not found | Schema mismatch | Check table schema, verify column exists |
| Data type mismatch | Wrong CAST | Add explicit CAST in SQL query |
| Table not found | Previous step failed | Verify previous step completed successfully |
| API auth failed | Missing secret | Run: `td wf secret set <project> td.apikey` |
| Timeout | Large dataset | Reduce data size or optimize query |
| Division by zero | Null values | Add NULLIF or COALESCE wrappers |
| Workflow status unknown | Connection issue | Manually check: `tdx wf session <SESSION_ID>` |

---

## Automated Validation Summary

Each step now includes **3-phase validation**:

### Phase 1: PRE-CHECK (Before Execution)
- ✅ Validates previous step completed successfully
- ✅ Checks required files exist (reports, configs)
- ✅ Verifies required databases exist
- ✅ Confirms required tables are present
- ❌ **Blocks execution** if validation fails

### Phase 2: WORKFLOW EXECUTION
- Runs the actual data processing workflow
- Monitors execution status
- Captures session ID for tracking

### Phase 3: POST-VALIDATION (After Execution)
- ✅ Checks workflow completed with **success** status
- ✅ Verifies output databases/tables created
- ✅ Validates data counts are reasonable
- ❌ **Blocks progression** if validation fails

**Result:** Pipeline only progresses when each step completes successfully.

---

## Optional Workflow Handling

The pipeline supports **conditional optional workflows**:

### Configuration (Step 0.2)
User is asked during setup:
- "Do you want IDU Dashboard?" → Stores `include_idu_dashboard: true/false`
- "Do you want Analytics Dashboard?" → Stores `include_analytics_dashboard: true/false`

### Execution Logic

**IDU Dashboard (Step 3.5):**
```bash
if include_idu_dashboard = true:
  - PRE-CHECK: Verify unification completed
  - EXECUTE: Deploy IDU dashboard
  - POST-VALIDATION: Check deployment (non-blocking if fails)
else:
  - SKIP: Log "IDU Dashboard not requested"
  - PROCEED: Go to Step 4 (Golden Layer)
```

**Analytics Dashboard (Step 4.5):**
```bash
if include_analytics_dashboard = true:
  - PRE-CHECK: Verify golden layer completed
  - EXECUTE: Deploy analytics dashboard
  - POST-VALIDATION: Check deployment (non-blocking if fails)
else:
  - SKIP: Log "Analytics Dashboard not requested"
  - PROCEED: Go to Step 5 (Segmentation)
```

### Key Features
1. ✅ **User choice honored** - Only deploys what user requested
2. ✅ **Non-blocking failures** - Optional component failures don't halt pipeline
3. ✅ **Clear status reporting** - Final summary shows what was deployed
4. ✅ **Graceful skipping** - No errors when components are not requested

---

## Helper Skills Reference

Use these skills for each pipeline step:

| Step | Skill | Purpose |
|------|-------|---------|
| 1 | `profiling_skill.md` | Data discovery, PII detection, quality assessment |
| 2 | `staging_skill.md` | Data cleaning, transformation |
| 3 | `unification_skill.md` | Identity resolution, customer matching |
| 4 | `golden_skill.md` | Single customer view, aggregations |
| 5 | `segmentation_skill.md` | Audience segmentation, activation |

Optional:
- `idu_dashboard_skill.md` - Unification quality dashboard (Step 3.5)
- `analytics_skill.md` - Business intelligence dashboards (Step 4.5)

---

## Success Criteria

Pipeline is complete when **ALL** of these are true:

- [✅] All **required** workflows completed with **success** status
- [✅] All databases created and validated:
  - `stg_<sub>` - Staging database
  - `cdp_unif_<sub>` - Unification database
  - `gldn_<sub>` - Golden database
- [✅] Parent segment created and validated
- [✅] Test segments working (at least 2)
- [✅] All reports generated (5 reports minimum)
- [✅] Optional workflows deployed (if requested by user)
- [✅] End-to-end validation passed
- [✅] User confirmed and accepted handoff

**Congratulations! You've built a production-ready CDP pipeline! 🚀**
