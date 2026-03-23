Purpose: Create clean, standardized staging tables and trfmd_* columns directly from the source database.

Template reference: workflow-template/staging/
Helper skills:
  - ps_poc_automation/ — Staging transformations
  - sql-skills/trino — TD Trino SQL patterns
  - sql-skills/time-filtering — td_interval, time parsing, partition pruning

---

## Step 1: Staging (Data Cleaning & Transformation)

**Goal**: Clean raw data and create standardized `trfmd_*` columns for consistent downstream processing.

### Prerequisites
Before staging, ensure:
1. Profiling is complete (`profiling-choices.json` exists)
2. Source database name is confirmed
3. Target staging database: `stg_<sub>` (will be auto-created)
4. User has approved transformations from profiling

---

### Input from Profiling
Read `profiling-choices.json`:
- Tables to include in staging
- Column transformations per table (email, phone, name, date, etc.)
- Source database name

---

### Staging Workflow

#### 1. Create Staging Database
```bash
tdx query "CREATE DATABASE IF NOT EXISTS stg_<sub>"
```

---

#### 2. Generate Staging SQL Queries (Per Table)

For each table in `profiling-choices.json` with `include_in_staging: true`:

Create `staging/queries/<table_name>.sql`:

```sql
-- staging/queries/<table_name>.sql
-- Generated from profiling-choices.json

SELECT
  -- Original columns (preserve all)
  *,

  -- Transformed email columns
  ${email_transformations}

  -- Transformed phone columns
  ${phone_transformations}

  -- Transformed name columns
  ${name_transformations}

  -- Transformed date columns
  ${date_transformations}

  -- Transformed ID columns
  ${id_transformations}

  -- Transformed currency columns
  ${currency_transformations}

  -- Transformed boolean/flag columns
  ${flag_transformations}

FROM <source_database>.<table_name>
WHERE time >= TD_TIME_ADD(TD_SCHEDULED_TIME(), '-30d', 'JST')  -- Adjust time filter as needed
```

**Transformation Templates:**

**Email Transformations:**
```sql
-- For column: email
LOWER(TRIM(email)) as trfmd_email,
CASE
  WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'valid'
  WHEN email IS NULL THEN 'missing'
  ELSE 'invalid'
END as trfmd_email_status
```

**Phone Transformations:**
```sql
-- For column: phone_number
REGEXP_REPLACE(phone_number, '[^0-9]', '') as trfmd_phone,
CASE
  WHEN LENGTH(REGEXP_REPLACE(phone_number, '[^0-9]', '')) >= 10 THEN 'valid'
  WHEN phone_number IS NULL THEN 'missing'
  ELSE 'invalid'
END as trfmd_phone_status
```

**Name Transformations (Trino SQL):**
```sql
-- For column: first_name (Title Case - capitalizes first letter of each word)
CASE
  WHEN NULLIF(LOWER(LTRIM(RTRIM(first_name))), 'null') IS NULL THEN NULL
  WHEN NULLIF(LOWER(LTRIM(RTRIM(first_name))), '') IS NULL THEN NULL
  ELSE ARRAY_JOIN(
    TRANSFORM(
      SPLIT(LOWER(TRIM(first_name)), ' '),
      x -> CONCAT(UPPER(SUBSTR(x, 1, 1)), SUBSTR(x, 2, LENGTH(x)))
    ),
    ' ',
    ''
  )
END AS trfmd_first_name,

-- For column: last_name (Title Case)
CASE
  WHEN NULLIF(LOWER(LTRIM(RTRIM(last_name))), 'null') IS NULL THEN NULL
  WHEN NULLIF(LOWER(LTRIM(RTRIM(last_name))), '') IS NULL THEN NULL
  ELSE ARRAY_JOIN(
    TRANSFORM(
      SPLIT(LOWER(TRIM(last_name)), ' '),
      x -> CONCAT(UPPER(SUBSTR(x, 1, 1)), SUBSTR(x, 2, LENGTH(x)))
    ),
    ' ',
    ''
  )
END AS trfmd_last_name,

-- Optional: full name concatenation (Title Case)
ARRAY_JOIN(
  TRANSFORM(
    SPLIT(LOWER(TRIM(CONCAT(first_name, ' ', last_name))), ' '),
    x -> CONCAT(UPPER(SUBSTR(x, 1, 1)), SUBSTR(x, 2, LENGTH(x)))
  ),
  ' ',
  ''
) AS trfmd_full_name
```

**Date Transformations (Treasure Data Functions):**
```sql
-- For string date column: date_of_birth (format: YYYY-MM-DD)
TD_TIME_PARSE(date_of_birth) as trfmd_dob_unix,

-- For timestamp column: created_at
TD_TIME_PARSE(created_at) as trfmd_created_unix,

-- For unix timestamp (already in seconds): registration_time
registration_time as trfmd_registration_unix
```

**ID Transformations:**
```sql
-- For column: customer_id
UPPER(TRIM(CAST(customer_id AS VARCHAR))) as trfmd_customer_id
```

**Currency Transformations:**
```sql
-- For column: amount
CAST(REGEXP_REPLACE(REGEXP_REPLACE(amount, '[$,]', ''), '[^0-9.]', '') AS DOUBLE) as trfmd_amount
```

**Boolean/Flag Transformations:**
```sql
-- For column: consent_flag
CASE
  WHEN LOWER(CAST(consent_flag AS VARCHAR)) IN ('1', 'true', 't', 'yes', 'y') THEN 'True'
  WHEN LOWER(CAST(consent_flag AS VARCHAR)) IN ('0', 'false', 'f', 'no', 'n') THEN 'False'
  ELSE NULL
END as trfmd_consent_flag
```

---

#### 3. Create Email Quality Check Query

Create `staging/queries/invalid_emails.sql`:

```sql
-- Invalid email report across all staging tables
SELECT
  'customers' as table_name,
  email as original_email,
  trfmd_email as cleaned_email,
  trfmd_email_status as status
FROM stg_<sub>.customers
WHERE trfmd_email_status = 'invalid'

UNION ALL

SELECT
  'orders' as table_name,
  customer_email as original_email,
  trfmd_customer_email as cleaned_email,
  trfmd_customer_email_status as status
FROM stg_<sub>.orders
WHERE trfmd_customer_email_status = 'invalid'

-- Add more tables as needed
```

---

#### 4. Update wf2_stage.dig Workflow

Edit `wf2_stage.dig`:

```yaml
_export:
  !include : 'config/src_params.yml'
  td:
    database: <source_database>  # Read from source

+create_staging_database:
  td_ddl>:
  create_databases: ["stg_${sub}"]

+transformed_tables:
  _parallel: true
  for_each>:
    tbl: ${staging_tables}  # From profiling-choices.json
  _do:
    td>: staging/queries/${tbl}.sql
    create_table: stg_${sub}.${tbl}

+invalid_emails_report:
  td>: staging/queries/invalid_emails.sql
  create_table: stg_${sub}.invalid_emails_report
```

**Get table list from profiling-choices.json:**
```bash
# Extract table names where include_in_staging: true
staging_tables: ['customers', 'orders', 'consents', 'pageviews']
```

---

#### 5. Update config/src_params.yml

Add staging parameters:

```yaml
# Source configuration
src: <source_database_name>
sub: <client_name>
stg: stg
gld: gldn
site: us01  # or eu01, ap02, etc.

# Staging configuration
staging_tables:
  - customers
  - orders
  - consents
  - pageviews
  - email_activity
  - sms_activity

# Time filtering (default: last 30 days for testing)
staging_time_range: "-30d"
```

---

### 6. Validate Generated SQL

Before pushing to TD, validate each staging query:

```bash
# Test query with LIMIT
tdx query "
SELECT * FROM (
  <paste staging query here>
) LIMIT 10
"
```

**Check**:
- All original columns preserved ✓
- `trfmd_*` columns created correctly ✓
- No syntax errors ✓
- Data types are correct ✓

---

### 7. Push Workflow to TD Console

```bash
# Navigate to project directory
cd <sub>_workflow/

# Push workflow to TD
tdx wf push <project_name>

# Verify workflow uploaded
tdx wf list <project_name>
```

**Expected output:**
```
wf2_stage
staging/queries/customers.sql
staging/queries/orders.sql
...
```

---

### 8. Run Staging Workflow

```bash
# Run staging workflow
tdx wf run <project_name> wf2_stage

# Get session ID
SESSION_ID=<returned_session_id>

# Monitor workflow
tdx wf sessions <project_name> wf2_stage

# Check status
tdx wf session <SESSION_ID>
```

---

### 9. Monitor and Handle Errors

**Check logs if workflow fails:**
```bash
# Get attempt ID from session
tdx wf session <SESSION_ID>

# Get task logs
tdx query "SELECT * FROM _job_log WHERE session_id = '<SESSION_ID>'"
```

**Common Errors & Fixes:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Column not found: email` | Source table doesn't have email column | Check profiling-choices.json, verify column exists |
| `Type mismatch: cannot cast X to Y` | Wrong CAST in transformation | Update staging query, add explicit CAST |
| `Table not found: <source_database>.<table>` | Source database/table name wrong | Verify source database name in config |
| `Syntax error near 'REGEXP_REPLACE'` | Invalid regex pattern | Escape special characters in regex |
| `Division by zero` | Null handling issue | Add NULLIF or COALESCE |

**If error occurs:**
1. Get error message from logs
2. Identify failing table/query
3. Fix staging/queries/<table>.sql
4. Push updated workflow: `tdx wf push <project_name>`
5. Re-run: `tdx wf run <project_name> wf2_stage`

---

### 10. Validate Staging Results

After successful run, verify:

```bash
# Check staging tables created
tdx tables stg_<sub>

# Check row counts
tdx query "SELECT COUNT(*) FROM stg_<sub>.customers"

# Check transformed columns
tdx query "
SELECT
  email,
  trfmd_email,
  trfmd_email_status,
  COUNT(*) as cnt
FROM stg_<sub>.customers
GROUP BY 1, 2, 3
LIMIT 20
"

# Check invalid emails
tdx query "SELECT * FROM stg_<sub>.invalid_emails_report LIMIT 100"
```

**Validation Checklist:**
- [ ] All expected staging tables exist in `stg_<sub>` database
- [ ] Row counts match source tables (within expected time range)
- [ ] `trfmd_*` columns populated correctly
- [ ] Email validation shows reasonable valid/invalid split
- [ ] No massive NULL spikes in transformed columns
- [ ] Date transformations produce valid unix timestamps

---

### 11. Generate Staging Summary Report

Create `staging-report.md`:

```markdown
# Staging Report - stg_<sub>

Generated: <timestamp>

## Summary
- Source Database: <source_database>
- Target Database: stg_<sub>
- Tables Staged: <count>
- Total Rows: <sum>
- Workflow Session: <SESSION_ID>
- Status: SUCCESS

## Tables Processed

### Table: customers
- Source Rows: <count>
- Staged Rows: <count>
- Transformations Applied:
  - trfmd_email ← LOWER(TRIM(email))
  - trfmd_phone ← REGEXP_REPLACE(phone, '[^0-9]', '')
  - trfmd_first_name ← Title Case (Trino ARRAY_JOIN + TRANSFORM)
  - trfmd_last_name ← Title Case (Trino ARRAY_JOIN + TRANSFORM)
  - trfmd_dob_unix ← TD_TIME_PARSE(date_of_birth)

**Email Quality:**
- Valid: <count> (<pct>%)
- Invalid: <count> (<pct>%)
- Missing: <count> (<pct>%)

**Phone Quality:**
- Valid: <count> (<pct>%)
- Invalid: <count> (<pct>%)
- Missing: <count> (<pct>%)

### Table: orders
...

## Invalid Emails Summary
Top 10 invalid email patterns:
1. <pattern> - <count> occurrences
2. ...

## Next Steps
✓ Staging complete
→ Ready for unification (Step 2)

Recommended unification keys:
- Primary: customer_id (coverage: <pct>%)
- Secondary: email (coverage: <pct>%)
- Tertiary: phone (coverage: <pct>%)
```

---

### 12. User Confirmation

Present to user:
```
✓ Staging workflow completed successfully!

Created staging tables in: stg_<sub>
- <table1>: <row_count> rows
- <table2>: <row_count> rows
- <table3>: <row_count> rows

Transformations applied:
- <N> email columns → trfmd_email
- <N> phone columns → trfmd_phone
- <N> name columns → trfmd_first_name, trfmd_last_name
- <N> date columns → trfmd_*_unix

Email quality: <valid_pct>% valid, <invalid_pct>% invalid

Staging report saved to: staging-report.md

Ready to proceed to unification? (yes/no)
```

---

## Error Handling Best Practices

1. **Always validate queries before pushing** - Use `LIMIT 10` first
2. **Check source table existence** - Verify all tables from profiling exist
3. **Handle NULL values** - Use COALESCE or CASE for nullable columns
4. **Escape special characters** - In regex patterns and string literals
5. **Test transformations** - Manually verify transformed values make sense
6. **Monitor row counts** - Ensure no unexpected data loss
7. **Keep original columns** - Never drop source columns, only add `trfmd_*`

---

## Output for Next Steps

Pass to unification_skill:
- Staging database: `stg_<sub>`
- Tables with unification keys
- Transformed key columns (trfmd_email, trfmd_phone, trfmd_customer_id)
- Staging report summary
