Purpose: Profile source data to detect PII, join keys, duplicates, data quality issues, and recommend transformations.

Template reference: ps_poc_automation/
Helper skills:
  - tdx-skills/tdx-basic — Database/table exploration
  - sql-skills/trino — TD SQL patterns

---

## Step 0: Data Profiling (Source Database Discovery)

**Goal**: Understand the source data structure, quality, and relationships before any transformations.

### Prerequisites
Before profiling, ensure:
1. Source database name is known (ask user if not provided)
2. TD API credentials are configured
3. Access to source database is verified: `tdx tables <source_database>`

---

### Profiling Workflow

#### 1. List All Tables
```bash
tdx tables <source_database>
```

**Output**: List of all table names in source database.
**Store**: Table names for parallel processing.

---

#### 2. Get Schema for Each Table (Parallel - 3 at a time)
For each table, run:
```bash
tdx tables <source_database> <table_name>
```

**Capture**:
- Column names
- Column types (string, long, int, double, timestamp, array, map)
- Table row counts (if available)

**Store**: `profiles/<table_name>_schema.json`

---

#### 3. Run Data Quality Queries (Per Table)

For each table, execute these profiling queries:

**A. Row Count & Basic Stats**
```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT td_client_id) as unique_client_ids,
  MIN(time) as earliest_time,
  MAX(time) as latest_time
FROM <source_database>.<table_name>
```

**B. Column Completeness (Null Rate)**
```sql
SELECT
  '<column_name>' as column_name,
  COUNT(*) as total_rows,
  COUNT(<column_name>) as non_null_count,
  ROUND(100.0 * COUNT(<column_name>) / COUNT(*), 2) as completeness_pct
FROM <source_database>.<table_name>
```
Run for each column. Identify columns with <50% completeness.

**C. PII Detection Patterns**

**Email Detection:**
```sql
SELECT
  '<column_name>' as column_name,
  COUNT(DISTINCT <column_name>) as unique_values,
  COUNT(CASE WHEN REGEXP_LIKE(<column_name>, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 1 END) as valid_email_count,
  ROUND(100.0 * COUNT(CASE WHEN REGEXP_LIKE(<column_name>, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 1 END) / COUNT(*), 2) as email_match_pct
FROM <source_database>.<table_name>
WHERE <column_name> IS NOT NULL
```
**Rule**: If email_match_pct > 80%, classify as EMAIL.

**Phone Detection:**
```sql
SELECT
  '<column_name>' as column_name,
  COUNT(DISTINCT <column_name>) as unique_values,
  COUNT(CASE WHEN REGEXP_LIKE(<column_name>, '^[+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,9}$') THEN 1 END) as phone_pattern_count,
  ROUND(100.0 * COUNT(CASE WHEN REGEXP_LIKE(<column_name>, '^[+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,9}$') THEN 1 END) / COUNT(*), 2) as phone_match_pct
FROM <source_database>.<table_name>
WHERE <column_name> IS NOT NULL
```
**Rule**: If phone_match_pct > 70%, classify as PHONE.

**Name Detection (First Name, Last Name):**
```sql
SELECT
  '<column_name>' as column_name,
  COUNT(DISTINCT <column_name>) as unique_values,
  AVG(LENGTH(<column_name>)) as avg_length,
  MAX(LENGTH(<column_name>)) as max_length,
  COUNT(CASE WHEN REGEXP_LIKE(<column_name>, '^[A-Za-z\s\-'']+$') THEN 1 END) as alpha_only_count
FROM <source_database>.<table_name>
WHERE <column_name> IS NOT NULL
```
**Rule**: If column name contains 'first_name', 'last_name', 'name' AND alpha_only_count > 80%, classify as NAME.

**Date/Timestamp Detection:**
```sql
SELECT
  '<column_name>' as column_name,
  MIN(<column_name>) as min_value,
  MAX(<column_name>) as max_value,
  COUNT(DISTINCT <column_name>) as unique_values
FROM <source_database>.<table_name>
WHERE <column_name> IS NOT NULL
```
**Rule**: If column type is timestamp, bigint (unix), or string matching date patterns, classify as DATE.

**ID Detection (Customer ID, User ID, etc.):**
```sql
SELECT
  '<column_name>' as column_name,
  COUNT(DISTINCT <column_name>) as unique_values,
  COUNT(*) as total_rows,
  ROUND(100.0 * COUNT(DISTINCT <column_name>) / COUNT(*), 2) as uniqueness_pct
FROM <source_database>.<table_name>
WHERE <column_name> IS NOT NULL
```
**Rule**: If uniqueness_pct > 90% AND column name contains 'id', 'key', classify as ID.

**D. Duplicate Detection**
```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT email) as unique_emails,
  COUNT(*) - COUNT(DISTINCT email) as duplicate_count
FROM <source_database>.<table_name>
WHERE email IS NOT NULL
```
Run for each identifier column (email, phone, customer_id).

**E. Join Key Discovery**
For each ID-like column across all tables, find common keys:
```sql
-- Table A
SELECT DISTINCT <id_column> FROM <source_database>.<table_a> LIMIT 10000

-- Table B
SELECT DISTINCT <id_column> FROM <source_database>.<table_b> LIMIT 10000

-- Compare overlap
SELECT
  COUNT(DISTINCT a.<id_column>) as table_a_unique,
  COUNT(DISTINCT b.<id_column>) as table_b_unique,
  COUNT(DISTINCT CASE WHEN b.<id_column> IS NOT NULL THEN a.<id_column> END) as overlap_count,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN b.<id_column> IS NOT NULL THEN a.<id_column> END) / COUNT(DISTINCT a.<id_column>), 2) as join_coverage_pct
FROM <source_database>.<table_a> a
LEFT JOIN <source_database>.<table_b> b ON a.<id_column> = b.<id_column>
```
**Rule**: If join_coverage_pct > 30%, recommend as JOIN KEY.

**F. TD Time Column Classification**
```sql
SELECT
  MIN(time) as min_time,
  MAX(time) as max_time,
  COUNT(DISTINCT DATE(time)) as unique_dates,
  COUNT(*) as total_rows
FROM <source_database>.<table_name>
```
**Rule**: If all rows have same time → IMPORT time. If time varies significantly → EVENT time.

**G. JSON/ARRAY Field Assessment**
```sql
SELECT
  '<column_name>' as column_name,
  JSON_EXTRACT_SCALAR(<column_name>, '$[0]') as sample_value,
  COUNT(*) as total_rows
FROM <source_database>.<table_name>
WHERE <column_name> IS NOT NULL
LIMIT 10
```
**Rule**: If column type is MAP or ARRAY, assess if it needs JSON_EXTRACT in staging.

---

### 4. Generate Profiling Report

Create `profiling-report.md` with:

```markdown
# Data Profiling Report - <source_database>

Generated: <timestamp>

## Summary
- Total Tables: <count>
- Total Rows: <sum across tables>
- Date Range: <min_time> to <max_time>

## Tables Overview

### Table: <table_name_1>
- Rows: <count>
- Columns: <count>
- Primary Identifiers: <list>
- Date Range: <min_time> to <max_time>

**Detected PII:**
- Email columns: <list>
- Phone columns: <list>
- Name columns: <list>

**Identifier Coverage:**
- customer_id: <completeness_pct>%
- email: <completeness_pct>%
- phone: <completeness_pct>%

**Data Quality Issues:**
- Columns with >20% nulls: <list>
- Duplicate rows: <count> (<pct>%)

**Recommended Transformations:**
- trfmd_email ← LOWER(TRIM(email))
- trfmd_phone ← REGEXP_REPLACE(phone, '[^0-9]', '')
- trfmd_first_name ← INITCAP(TRIM(first_name))

### Table: <table_name_2>
...

## Cross-Table Analysis

**Recommended Join Keys:**
1. customer_id (appears in: <table_list>, coverage: <pct>%)
2. email (appears in: <table_list>, coverage: <pct>%)
3. phone (appears in: <table_list>, coverage: <pct>%)

**Unification Strategy:**
- Primary key: <customer_id OR email OR phone>
- Secondary keys: <list>
- Match rules: <email exact match, phone fuzzy match, etc.>

## Next Steps
1. Review and approve transformations
2. Identify tables for staging (exclude reference/catalog tables)
3. Configure unification keys
4. Define golden layer attributes
```

**Store**: `profiling-report.md`

---

### 5. Generate Profiling Choices (Interactive)

Present findings to user and get approval:

```
Found the following transformations for <table_name>:

Email columns (2 found):
  - email → trfmd_email (lowercase, trim)
  - contact_email → trfmd_contact_email (lowercase, trim)

Phone columns (1 found):
  - phone_number → trfmd_phone (digits only)

Name columns (2 found):
  - first_name → trfmd_first_name (title case)
  - last_name → trfmd_last_name (title case)

Date columns (1 found):
  - date_of_birth → trfmd_dob_unix (unix timestamp)

Do you approve these transformations? (yes/no)
```

**Store approved choices**: `profiling-choices.json`

```json
{
  "tables": {
    "customers": {
      "transformations": {
        "email": {"type": "email", "target": "trfmd_email"},
        "phone_number": {"type": "phone", "target": "trfmd_phone"},
        "first_name": {"type": "name", "target": "trfmd_first_name"},
        "last_name": {"type": "name", "target": "trfmd_last_name"},
        "date_of_birth": {"type": "date_to_unix", "target": "trfmd_dob_unix"}
      },
      "include_in_staging": true
    },
    "orders": {
      "transformations": {
        "customer_email": {"type": "email", "target": "trfmd_customer_email"}
      },
      "include_in_staging": true
    },
    "products": {
      "transformations": {},
      "include_in_staging": false,
      "reason": "Reference table - no customer data"
    }
  },
  "unification_keys": {
    "primary": "customer_id",
    "secondary": ["email", "phone"],
    "tables_with_keys": ["customers", "orders", "consents"]
  },
  "time_classification": {
    "customers": "import_time",
    "orders": "event_time",
    "pageviews": "event_time"
  }
}
```

---

### 6. Output Artifacts

After profiling, you should have:

1. **profiling-report.md** - Human-readable summary
2. **profiling-choices.json** - Machine-readable approved transformations
3. **profiles/** - Per-table JSON profiles with schema and stats
4. **Recommended configuration** for:
   - Staging SQL queries
   - Unification config (unify.yml)
   - Golden layer attributes

---

### 7. User Confirmation

Present to user:
```
✓ Profiling complete!

Found:
- <N> tables with customer data
- <N> PII columns (email, phone, name)
- <N> recommended transformations
- <N> potential join keys

Profiling report saved to: profiling-report.md
Transformation choices saved to: profiling-choices.json

Ready to proceed to staging? (yes/no)
```

---

## Error Handling

**Issue**: Table has no rows
**Action**: Skip profiling, mark as empty in report

**Issue**: Column appears to be PII but regex match is < 50%
**Action**: Flag for manual review, ask user to confirm

**Issue**: No common join keys found across tables
**Action**: Warn user, suggest creating synthetic key or using pre-unified ID

**Issue**: Time column all same timestamp
**Action**: Classify as IMPORT time, recommend using event timestamp if available

---

## Best Practices

1. **Sampling**: For large tables (>10M rows), use LIMIT or TABLESAMPLE for profiling queries
2. **Parallel Processing**: Profile max 3 tables at a time to avoid overwhelming TD
3. **Manual Review**: Always get user approval before proceeding to staging
4. **Documentation**: Keep profiling-report.md updated as source changes
5. **Versioning**: Store profiling artifacts with timestamp for historical reference

---

## Output for Next Steps

Pass to staging_skill:
- List of tables to stage
- Column transformations per table
- Target database: `stg_<sub>`

Pass to unification_skill:
- Unification keys (primary + secondary)
- Tables with unification keys
- Match rules

Pass to golden_skill:
- Table relationships (join keys)
- Attribute categories (transactions, engagement, web, etc.)
