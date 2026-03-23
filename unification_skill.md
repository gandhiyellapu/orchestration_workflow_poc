Purpose: Configure and run TD ID Unification to produce unified IDs and lookup tables.

Template reference: workflow-template/unification/
Helper skills:
  - ps_poc_automation/ — Unification key discovery & matching strategy

---

## Step 2: Unification (Identity Resolution)

**Goal**: Match customer records across staging tables to create a single unified customer ID.

### Prerequisites
Before unification, ensure:
1. Staging is complete (`stg_<sub>` database exists with clean data)
2. Staging report confirms transformed key columns (trfmd_email, trfmd_phone, trfmd_customer_id)
3. Unification ID name decided (e.g., `canonical_id`, `td_unified_id`, `master_id`)

---

### Step 0: Set TD API Key Secret

**CRITICAL**: Unification requires your TD API key to authenticate with the Unification API.

**Ask user for:**
1. **Project name** (e.g., `b2b-dev`)
2. **TD API key** (starts with `TD1`, format: `TD1 1/abc123...`)

**Set the secret:**
```bash
# Set API key as workflow secret
td wf secret set <project-name> td.apikey

# When prompted, paste your TD API key
# Example: TD1 1/abc123def456...
```

**How to get TD API key:**
1. Log in to TD Console: https://console.treasuredata.com
2. Go to: **Profile** → **API Keys**
3. Copy your API key

**Security Note:** Never commit API keys to git or share them publicly!

---

### Input from Staging
Read `staging-report.md`:
- Staging database: `stg_<sub>`
- Tables with unification keys
- Key column coverage percentages
- Data quality metrics

---

### Unification Workflow

#### 1. Analyze Unification Strategy

**Check if data is pre-unified:**
```bash
# Check if all staging tables share a common ID
tdx query "
SELECT
  COUNT(DISTINCT customer_id) as unique_customers
FROM stg_<sub>.customers
"

tdx query "
SELECT
  COUNT(DISTINCT customer_id) as unique_customers
FROM stg_<sub>.orders
"

# If same count and 100% coverage → data is pre-unified
```

**Decision:**
- **If pre-unified** → Use existing customer_id as primary key, skip matching
- **If NOT pre-unified** → Use email/phone matching with TD Unification API

---

#### 2. Define Unification Keys

Based on staging profiling:

**Primary Key Options (choose best coverage):**
1. `customer_id` - If exists and well-populated (>80% coverage)
2. `email` - If unique and valid (>70% coverage)
3. `phone` - If standardized (>60% coverage)

**Secondary Keys:**
- Use remaining identifiers as secondary match keys
- Examples: device_id, session_id, loyalty_id

**Recommended Priority:**
```
1st: trfmd_customer_id (exact match)
2nd: trfmd_email (exact match, case-insensitive)
3rd: trfmd_phone (exact match, digits only)
```

---

#### 3. Configure unification/unify.yml

Edit `unification/unify.yml`:

name: ${sub}

#####################################################
##
##Declare Validation logic for unification keys
##
#####################################################

keys:
  - name: email
    valid_regexp: "^[a-zA-Z0-9][a-zA-Z0-9._+-]*@(?!(?:example\\.com|test\\.com|demo\\.com|sample\\.com|temp\\.com|fake\\.com|dummy\\.com))[a-zA-Z0-9][a-zA-Z0-9.-]*\\.[a-zA-Z]{2,}$"
    invalid_texts: ['', null, 'anonymous@example.com']
  - name: td_client_id
    invalid_texts: ['', null]
  - name: phone_number
    invalid_texts: ['', null]
  - name: order_no
    invalid_texts: ['', null]
  - name: credit_card_token
    invalid_texts: ['', null]
  # - name: customer_id
  #   invalid_texts: ['', null, '71288389702', '126847236', '126795972']
  - name: device_id
    invalid_texts: ['', null]
  - name: adid
    invalid_texts: ['', null]
  - name: idfa
    invalid_texts: ['', null]

#####################################################
##
## Declare databases, tables, and keys to use during unification
##
#####################################################

tables:
  - database: ${stg}_${sub}
    table: loyalty_profile
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}
      # - {column: customer_id, key: customer_id}

  - database: ${stg}_${sub}
    table: pageviews
    key_columns:
      - {column: td_client_id, key: td_client_id}

  - database: ${stg}_${sub}
    table: email_activity
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}

  - database: ${stg}_${sub}
    table: order_digital_transactions
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}
      - {column: token, key: credit_card_token}
      - {column: order_no, key: order_no}
      # - {column: customer_id, key: customer_id}

  - database: ${stg}_${sub}
    table: order_offline_transactions
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}
      - {column: token, key: credit_card_token}
      - {column: order_no, key: order_no}
      # - {column: customer_id, key: customer_id}

  - database: ${stg}_${sub}
    table: order_details
    key_columns:
      - {column: order_no, key: order_no}

  - database: ${stg}_${sub}
    table: formfills
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}
      - {column: td_client_id, key: td_client_id}

  - database: ${stg}_${sub}
    table: consents_email
    key_columns:
      - {column: trfmd_email, key: email}
      
  - database: ${stg}_${sub}
    table: consents_phone
    key_columns:
      - {column: trfmd_phone_number, key: phone_number}

  - database: ${stg}_${sub}
    table: survey_responses
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}
      # - {column: customer_id, key: customer_id}

  - database: ${stg}_${sub}
    table: app_analytics
    key_columns:
      - {column: device_id, key: device_id}
      - {column: adid, key: adid}
      - {column: idfa, key: idfa}

  - database: ${stg}_${sub}
    table: sms_activity
    key_columns:
      - {column: trfmd_email, key: email}
      - {column: trfmd_phone_number, key: phone_number}

#####################################################
##
##Declare hierarchy for unification. Define keys to use for each level.
##
#####################################################

canonical_ids:
  - name: ${unification_id}
    merge_by_keys: [email, phone_number, credit_card_token, td_client_id, order_no, device_id, adid, idfa]
    merge_iterations: 5
    incremental_merge_iterations: 2


#####################################################
##
##Declare Survivorship Rules to Determine Best Value for Each Attribute
## 
#####################################################

master_tables:
  - name: unification_master
    canonical_id: ${unification_id}
    attributes:
      - name: email
        source_columns:
          - {table: loyalty_profile, column: trfmd_email, priority: 1}
          - {table: email_activity, column: trfmd_email, priority: 2}
          - {table: order_digital_transactions, column: trfmd_email, priority: 2}
          - {table: order_offline_transactions, column: trfmd_email, priority: 2}
          - {table: formfills, column: trfmd_email, priority: 2}
          - {table: consents_email, column: trfmd_email, priority: 2}
          - {table: survey_responses, column: trfmd_email, priority: 3}
          - {table: app_analytics, column: trfmd_email, priority: 3}
          - {table: sms_activity, column: trfmd_email, priority: 3}

      - name: phone_number
        source_columns:
          - {table: loyalty_profile, column: trfmd_phone_number, priority: 1}
          - {table: order_digital_transactions, column: trfmd_phone_number, priority: 2}
          - {table: order_offline_transactions, column: trfmd_phone_number, priority: 2}
          - {table: formfills, column: trfmd_phone_number, priority: 2}
          - {table: consents_phone, column: trfmd_phone_number, priority: 2}
          - {table: survey_responses, column: trfmd_phone_number, priority: 3}
          - {table: app_analytics, column: trfmd_phone_number, priority: 3}
          - {table: sms_activity, column: trfmd_phone_number, priority: 3}

      - name: td_client_id
        source_columns:
          - {table: pageviews, column: td_client_id, priority: 1}
          - {table: formfills, column: td_client_id, priority: 2}

      - name: device_id
        source_columns:
          - {table: app_analytics, column: device_id, priority: 1}
      - name: adid
        source_columns:
          - {table: app_analytics, column: adid, priority: 1}
      - name: idfa
        source_columns:
          - {table: app_analytics, column: idfa, priority: 1}

      # Primary Address
      - name: address
        source_columns:
          - {table: loyalty_profile, column: trfmd_address, priority: 1}
          - {table: order_digital_transactions, column: trfmd_location_address, priority: 2}
          - {table: order_offline_transactions, column: trfmd_location_address, priority: 3}

      # City
      - name: city
        source_columns:
          - {table: loyalty_profile, column: trfmd_city, priority: 1}
          - {table: order_digital_transactions, column: trfmd_location_city, priority: 2}
          - {table: order_offline_transactions, column: trfmd_location_city, priority: 3}

      # State/Province
      - name: state
        source_columns:
          - {table: loyalty_profile, column: trfmd_state, priority: 1}
          - {table: order_digital_transactions, column: trfmd_location_state, priority: 2}
          - {table: order_offline_transactions, column: trfmd_location_state, priority: 3}

      # ZIP/Postal Code
      - name: postal_code
        source_columns:
          - {table: loyalty_profile, column: trfmd_postal_code, priority: 1}
          - {table: order_digital_transactions, column: trfmd_location_postal_code, priority: 2}
          - {table: order_offline_transactions, column: trfmd_location_postal_code, priority: 3}

      # Country
      - name: country
        source_columns:
          - {table: loyalty_profile, column: trfmd_country, priority: 1}
          - {table: order_offline_transactions, column: trfmd_location_country, priority: 2}

          


**Key Configuration Decisions:**

**Match Types:**
- `exact` - Requires exact string match (use for customer_id, email, phone)
- `fuzzy` - Allows similarity matching (use for names if needed)
- `probabilistic` - ML-based matching (for complex cases)

**Survivorship Rules:**
- `most_recent` - Latest record wins (based on time column)
- `most_complete` - Record with most non-null fields wins
- `source_priority` - Specific table takes precedence
- `manual` - Custom logic

---

#### 4. Update wf3_unify.dig Workflow

Verify `wf3_unify.dig`:

```yaml
_export:
  !include : 'config/src_params.yml'
  td:
    database: stg_${sub}

+create_unification_database:
  td_ddl>:
  create_databases: ["cdp_unif_${sub}"]

+unification:
  http_call>: https://api-cdp.treasuredata.com/unifications/workflow_call
  headers:
    - authorization: ${secret:td.apikey}
  method: POST
  retry: true
  content_format: json
  content:
    early_access: true
    full_refresh: true
    unification:
      !include : unification/unify.yml
```

---

#### 5. Add TD API Key Secret

**CRITICAL**: Ask user for their TD API key before proceeding.

**Required from user:**
- **Project name** (e.g., `b2b-dev`)
- **TD API key** (starts with `TD1`, format: `TD1 1/abc123...`)

**Set the secret:**
```bash
# Use the project name and API key provided by user
td wf secret set <project_name> td.apikey

# When prompted, paste the user's TD API key directly
# Example: TD1 1/abc123def456...
```

**How user gets their TD API key:**
1. Log in to TD Console: https://console.treasuredata.com
2. Go to: **Profile** → **API Keys**
3. Copy the API key (starts with `TD1`)

---

#### 6. Push Workflow to TD Console

```bash
cd <sub>_workflow/

# Push unification workflow
tdx wf push <project_name>

# Verify files uploaded
tdx wf list <project_name> | grep -E "wf3_unify|unification"
```

**Expected output:**
```
wf3_unify.dig
unification/unify.yml
```

---

#### 7. Run Unification Workflow

```bash
# Run unification
tdx wf run <project_name> wf3_unify

# Get session ID
SESSION_ID=<returned_session_id>

# Monitor (unification can take 30min - 2hrs depending on data size)
tdx wf session <SESSION_ID>
```

**Expected status progression:**
```
1. queued → running
2. running → success (or error)
```

---

#### 8. Monitor and Handle Errors

**Check unification progress:**
```bash
# Check session status
tdx wf session <SESSION_ID>

# If running, check progress
tdx query "
SELECT
  workflow_name,
  status,
  started_at,
  finished_at
FROM _job_log
WHERE session_id = '<SESSION_ID>'
ORDER BY started_at DESC
"
```

**Common Errors & Fixes:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Unauthorized: Invalid API key` | API key secret not set | Ask user for API key, run `td wf secret set <project> td.apikey` |
| `Table not found: stg_<sub>.<table>` | Staging table missing | Verify staging completed successfully |
| `Invalid unificationIdName` | Special characters in ID name | Use alphanumeric + underscore only |
| `Source table has no ID columns` | unify.yml missing idColumns | Add idColumns for all source tables |
| `Match rule error: unsupported type` | Wrong idType in matchRules | Use: customer_id, email, phone, device_id |
| `Timeout: Unification exceeded 2h` | Too much data | Reduce data size or contact TD support |

**If error occurs:**
1. Get error from session logs
2. Fix unification/unify.yml
3. Push updated workflow
4. Re-run workflow

---

#### 9. Validate Unification Results

After successful run:

```bash
# Check unification database created
tdx tables cdp_unif_<sub>

# Expected tables:
# - <unification_id>_master (enriched customer master)
# - <unification_id>_lookup (source_id → unified_id mapping)
# - customers_enriched, orders_enriched (original tables with unified_id added)
```

**Verify unified ID count:**
```bash
tdx query "
SELECT
  COUNT(DISTINCT ${unification_id}) as total_unified_customers
FROM cdp_unif_<sub>.${unification_id}_master
"
```

**Check unification coverage:**
```bash
# How many source customers got unified?
tdx query "
SELECT
  source_table,
  COUNT(*) as total_source_records,
  COUNT(${unification_id}) as unified_records,
  ROUND(100.0 * COUNT(${unification_id}) / COUNT(*), 2) as unification_pct
FROM cdp_unif_<sub>.${unification_id}_lookup
GROUP BY 1
"
```

**Expected coverage:**
- >95% for primary key (customer_id)
- >80% for email/phone
- 100% overall (all records should get unified ID)

**Check merge quality:**
```bash
# How many source IDs per unified ID? (merge rate)
tdx query "
SELECT
  ${unification_id},
  COUNT(DISTINCT source_id) as num_source_ids,
  COUNT(DISTINCT source_table) as num_tables
FROM cdp_unif_<sub>.${unification_id}_lookup
GROUP BY 1
ORDER BY num_source_ids DESC
LIMIT 20
"
```

**Healthy merge distribution:**
- Most unified IDs (70-80%) should have 1-3 source IDs
- Some (10-20%) will have 4-10 (legitimate multi-touch customers)
- Very few (< 5%) should have >10 (potential over-merging)

**Check for over-merging:**
```bash
# Unified IDs with suspiciously high source count
tdx query "
SELECT
  ${unification_id},
  COUNT(DISTINCT source_id) as num_merged,
  ARRAY_AGG(DISTINCT source_table) as tables,
  ARRAY_AGG(DISTINCT source_id LIMIT 5) as sample_ids
FROM cdp_unif_<sub>.${unification_id}_lookup
GROUP BY 1
HAVING COUNT(DISTINCT source_id) > 20
ORDER BY num_merged DESC
LIMIT 10
"
```

**If over-merging detected:**
- Review match rules (may be too loose)
- Check for bad data (e.g., shared email like "info@company.com")
- Adjust survivorship rules
- Re-run with tighter matching

---

#### 10. Generate Unification Summary Report

Create `unification-report.md`:

```markdown
# Unification Report - cdp_unif_<sub>

Generated: <timestamp>

## Summary
- Unification ID: ${unification_id}
- Source Database: stg_<sub>
- Output Database: cdp_unif_<sub>
- Total Unified Customers: <count>
- Source Records: <count>
- Workflow Session: <SESSION_ID>
- Status: SUCCESS
- Duration: <duration>

## Match Statistics

### Unification Coverage
| Source Table | Source Records | Unified Records | Coverage % |
|--------------|----------------|-----------------|------------|
| customers    | 1,250,000      | 1,248,500       | 99.88%     |
| orders       | 3,456,789      | 3,450,000       | 99.80%     |
| consents     | 890,000        | 885,600         | 99.51%     |

### Merge Distribution
| Unified IDs | Count | Percentage |
|-------------|-------|------------|
| 1 source ID | 950,000 | 76.0% |
| 2-3 source IDs | 250,000 | 20.0% |
| 4-10 source IDs | 45,000 | 3.6% |
| >10 source IDs | 5,000 | 0.4% |

### Match Rule Performance
| Match Rule | Matches Created | Percentage |
|------------|-----------------|------------|
| customer_id (exact) | 800,000 | 64.0% |
| email (exact) | 350,000 | 28.0% |
| phone (exact) | 100,000 | 8.0% |

## Quality Checks

### Over-Merge Detection
- Unified IDs with >20 source IDs: <count>
- Largest cluster: <max_count> source IDs
- Action: <review/acceptable>

### Under-Merge Detection
- Expected merges missed: <count>
- Common cause: <email typos / missing data>

## Output Tables
- `${unification_id}_master` - Master customer table (1 row per unified ID)
- `${unification_id}_lookup` - Source ID to unified ID mapping
- `customers_enriched` - Customers with ${unification_id} added
- `orders_enriched` - Orders with ${unification_id} added
- `consents_enriched` - Consents with ${unification_id} added

## Next Steps
✓ Unification complete
→ Ready for golden layer (Step 3)

Recommended golden layer structure:
- Master identity table: profile_identifiers
- Attribute tables: transactions, email_activity, web_engagement
- Row-level tables: orders, consents, page events
```

---

#### 11. User Confirmation

Present to user:
```
✓ Unification workflow completed successfully!

Created unified IDs in: cdp_unif_<sub>
- Total unified customers: <count>
- Unification coverage: <pct>%
- Average merge rate: <avg> source IDs per customer

Unification ID: ${unification_id}

Output tables:
- ${unification_id}_master (<row_count> rows)
- ${unification_id}_lookup (<row_count> mappings)
- customers_enriched, orders_enriched, consents_enriched

Quality check:
- Over-merging: <status>
- Coverage: <status>

Unification report saved to: unification-report.md

Ready to proceed to golden layer? (yes/no)
```

---

## Error Handling Best Practices

1. **Validate staging data first** - Ensure clean trfmd_* columns
2. **Test with small sample** - Use WHERE time >= TD_TIME_ADD(..., '-7d') for first run
3. **Monitor API quotas** - Unification counts toward API limits
4. **Check survivorship** - Verify correct fields are preserved
5. **Review merge quality** - Always check for over/under-merging
6. **Incremental mode** - After first successful run, use incrementalMode: true

---

## Output for Next Steps

Pass to golden_skill:
- Unification database: `cdp_unif_<sub>`
- Unification ID name: `${unification_id}`
- Master table: `${unification_id}_master`
- Enriched tables: `customers_enriched`, `orders_enriched`, etc.
- Unification report summary
