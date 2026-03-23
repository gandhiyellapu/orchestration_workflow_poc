Purpose: Build the golden layer (single customer view + attribute tables + row-level drill-down tables).

Template reference: workflow-template/golden/
Helper skills:
  - ps_poc_automation/ — Golden construction patterns
  - sql-skills/trino-optimizer — CTAS, bucketing, approx functions
  - sql-skills/trino — Aggregation patterns

---

## Step 3: Golden Layer (Single Customer View)

**Goal**: Create a unified golden database with 1 master identity table + aggregated attribute tables + row-level detail tables.

### Prerequisites
Before golden layer, ensure:
1. Unification is complete (`cdp_unif_<sub>` database exists)
2. Unification report confirms ${unification_id} and master table
3. All enriched tables created
4. Target golden database: `gldn_<sub>` (will be auto-created)

---

### Input from Unification
Read `unification-report.md`:
- Unification database: `cdp_unif_<sub>`
- Unification ID name: `${unification_id}`
- Master table: `${unification_id}_master`
- Enriched tables available
- Source staging tables: `stg_<sub>`

---

### Golden Layer Structure

```
gldn_<sub>/
├── profile_identifiers         ← Master identity (1 row per customer)
├── ${unification_id}_master    ← Copy of unification master
│
├── Attribute Tables (aggregated, 1 row per customer):
│   ├── attr_transactions       ← Purchase behavior
│   ├── attr_email_activity     ← Email engagement
│   ├── attr_sms_activity       ← SMS engagement
│   ├── attr_pageviews          ← Web behavior
│   ├── attr_order_details      ← Order summary
│   └── attr_survey_responses   ← Survey feedback
│
└── Row-Level Tables (many rows per customer):
    ├── orders                  ← Order details with ${unification_id}
    ├── consents                ← Consent records
    ├── order_details           ← Line items
    ├── order_digital_transactions
    └── order_offline_transactions
```

---

### Golden Workflow

#### 1. Create Golden Database

```bash
tdx query "CREATE DATABASE IF NOT EXISTS gldn_<sub>"
```

---

#### 2. Create Master Identity Table

Create `golden/queries/all_profile_identifiers.sql`:

```sql
-- Master identity table - 1 row per unified customer
-- Combines all customer identifiers from unification master

SELECT
  ${unification_id},

  -- Core identifiers
  trfmd_customer_id,
  trfmd_email,
  trfmd_phone,

  -- Personal information
  trfmd_first_name,
  trfmd_last_name,
  trfmd_full_name,

  -- Demographics
  trfmd_dob_unix,
  CASE
    WHEN trfmd_dob_unix IS NOT NULL
    THEN YEAR(CURRENT_DATE) - YEAR(FROM_UNIXTIME(trfmd_dob_unix))
    ELSE NULL
  END as age,

  gender,
  city,
  state,
  zip_code,
  country,

  -- Account metadata
  account_status,
  loyalty_tier,
  vip_flag,

  -- Temporal
  MIN(time) as first_seen_time,
  MAX(time) as last_seen_time,
  TD_TIME_FORMAT(MIN(time), 'yyyy-MM-dd', 'UTC') as first_seen_date,
  TD_TIME_FORMAT(MAX(time), 'yyyy-MM-dd', 'UTC') as last_seen_date,

  -- Record count
  COUNT(*) as num_source_records

FROM cdp_unif_<sub>.${unification_id}_master
GROUP BY
  ${unification_id},
  trfmd_customer_id,
  trfmd_email,
  trfmd_phone,
  trfmd_first_name,
  trfmd_last_name,
  trfmd_full_name,
  trfmd_dob_unix,
  gender,
  city,
  state,
  zip_code,
  country,
  account_status,
  loyalty_tier,
  vip_flag
```

**Add to wf5_golden.dig:**
```yaml
+create_profile_identifiers:
  td>: golden/queries/all_profile_identifiers.sql
  database: gldn_${sub}
  create_table: gldn_${sub}.profile_identifiers
```

---

#### 3. Copy Unification Master Table

Create `golden/queries/copy_enriched_table.sql`:

```sql
-- Copy unification master to golden
SELECT *
FROM cdp_unif_<sub>.${table}
```

**Add to wf5_golden.dig:**
```yaml
+copy_unification_master:
  td>: golden/queries/copy_enriched_table.sql
  database: gldn_${sub}
  create_table: gldn_${sub}.${unification_id}_master
  table: ${unification_id}_master
```

---

#### 4. Create Row-Level Tables (with Unification ID)

For each transactional table, join with unification lookup:

Create `golden/queries/orders.sql`:

```sql
-- Orders with unified customer ID
SELECT
  o.*,
  l.${unification_id}
FROM stg_<sub>.orders o
LEFT JOIN cdp_unif_<sub>.${unification_id}_lookup l
  ON l.source_table = 'orders'
  AND l.source_id = o.trfmd_customer_id
```

Create `golden/queries/consents.sql`:

```sql
-- Consents with unified customer ID
SELECT
  c.*,
  l.${unification_id}
FROM stg_<sub>.consents c
LEFT JOIN cdp_unif_<sub>.${unification_id}_lookup l
  ON l.source_table = 'consents'
  AND (
    l.source_id = c.trfmd_email
    OR l.source_id = c.trfmd_phone
  )
```

**Add to wf5_golden.dig:**
```yaml
+copy_row_level_tables:
  _parallel: true
  for_each>:
    tbl: ['orders', 'consents', 'order_details', 'order_digital_transactions', 'order_offline_transactions']
  _do:
    td>: golden/queries/${tbl}.sql
    database: gldn_${sub}
    create_table: gldn_${sub}.${tbl}
```

---

#### 5. Create Attribute Tables (Aggregated)

**A. Transaction Attributes**

Create `golden/queries/attributes/transactions.sql`:

```sql
-- Customer transaction summary (1 row per customer)
SELECT
  ${unification_id},

  -- Purchase counts
  COUNT(DISTINCT order_id) as total_orders,
  COUNT(DISTINCT CASE WHEN channel = 'online' THEN order_id END) as online_orders,
  COUNT(DISTINCT CASE WHEN channel = 'offline' THEN order_id END) as offline_orders,

  -- Revenue metrics
  SUM(order_amount) as lifetime_revenue,
  AVG(order_amount) as average_order_value,
  MIN(order_amount) as min_order_value,
  MAX(order_amount) as max_order_value,

  -- Product metrics
  COUNT(DISTINCT product_id) as unique_products_purchased,
  COUNT(DISTINCT category) as unique_categories_purchased,

  -- Temporal metrics
  MIN(order_date_unix) as first_purchase_date_unix,
  MAX(order_date_unix) as last_purchase_date_unix,
  TD_TIME_FORMAT(MIN(order_date_unix), 'yyyy-MM-dd', 'UTC') as first_purchase_date,
  TD_TIME_FORMAT(MAX(order_date_unix), 'yyyy-MM-dd', 'UTC') as last_purchase_date,
  TD_TIME_PARSE(CURRENT_DATE, 'yyyy-MM-dd') - MAX(order_date_unix) as days_since_last_purchase,

  -- Recency, Frequency, Monetary (RFM)
  TD_TIME_PARSE(CURRENT_DATE, 'yyyy-MM-dd') - MAX(order_date_unix) as recency_days,
  COUNT(DISTINCT order_id) as frequency,
  SUM(order_amount) as monetary,

  -- Cohort
  TD_TIME_FORMAT(MIN(order_date_unix), 'yyyy-MM', 'UTC') as cohort_month

FROM gldn_<sub>.orders
WHERE ${unification_id} IS NOT NULL
GROUP BY ${unification_id}
```

**B. Email Activity Attributes**

Create `golden/queries/attributes/email_activity.sql`:

```sql
-- Customer email engagement summary
SELECT
  ${unification_id},

  -- Email counts
  COUNT(*) as total_emails_sent,
  COUNT(CASE WHEN opened = 'True' THEN 1 END) as emails_opened,
  COUNT(CASE WHEN clicked = 'True' THEN 1 END) as emails_clicked,
  COUNT(CASE WHEN bounced = 'True' THEN 1 END) as emails_bounced,
  COUNT(CASE WHEN unsubscribed = 'True' THEN 1 END) as emails_unsubscribed,

  -- Engagement rates
  ROUND(100.0 * COUNT(CASE WHEN opened = 'True' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as open_rate,
  ROUND(100.0 * COUNT(CASE WHEN clicked = 'True' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as click_rate,

  -- Temporal
  MAX(sent_time_unix) as last_email_sent_time,
  TD_TIME_FORMAT(MAX(sent_time_unix), 'yyyy-MM-dd', 'UTC') as last_email_sent_date,
  TD_TIME_PARSE(CURRENT_DATE, 'yyyy-MM-dd') - MAX(sent_time_unix) as days_since_last_email,

  -- Campaign types
  COUNT(DISTINCT campaign_id) as unique_campaigns_received,
  ARRAY_AGG(DISTINCT campaign_type) as campaign_types

FROM gldn_<sub>.email_activity
WHERE ${unification_id} IS NOT NULL
GROUP BY ${unification_id}
```

**C. Web/Pageview Attributes**

Create `golden/queries/attributes/pageviews.sql`:

```sql
-- Customer web engagement summary
SELECT
  ${unification_id},

  -- Page metrics
  COUNT(*) as total_pageviews,
  COUNT(DISTINCT session_id) as total_sessions,
  COUNT(DISTINCT page_url) as unique_pages_viewed,
  COUNT(DISTINCT product_id) as unique_products_viewed,

  -- Engagement metrics
  ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT session_id), 0), 2) as avg_pages_per_session,
  SUM(time_on_page) as total_time_on_site,

  -- Temporal
  MAX(pageview_time_unix) as last_pageview_time,
  TD_TIME_FORMAT(MAX(pageview_time_unix), 'yyyy-MM-dd', 'UTC') as last_pageview_date,
  TD_TIME_PARSE(CURRENT_DATE, 'yyyy-MM-dd') - MAX(pageview_time_unix) as days_since_last_visit,

  -- Top pages/products
  ELEMENT_AT(ARRAY_AGG(page_url ORDER BY cnt DESC), 1) as most_viewed_page,
  ELEMENT_AT(ARRAY_AGG(product_id ORDER BY cnt DESC), 1) as most_viewed_product

FROM (
  SELECT
    ${unification_id},
    session_id,
    page_url,
    product_id,
    time_on_page,
    pageview_time_unix,
    COUNT(*) OVER (PARTITION BY ${unification_id}, page_url) as cnt
  FROM gldn_<sub>.pageviews
)
WHERE ${unification_id} IS NOT NULL
GROUP BY ${unification_id}
```

**D. Order Details Attributes**

Create `golden/queries/attributes/order_details.sql`:

```sql
-- Customer order details summary
SELECT
  ${unification_id},

  -- Line item counts
  COUNT(*) as total_line_items,
  COUNT(DISTINCT product_id) as unique_products,
  COUNT(DISTINCT category) as unique_categories,

  -- Quantity metrics
  SUM(quantity) as total_quantity_purchased,
  AVG(quantity) as avg_quantity_per_line,

  -- Favorite products/categories
  ELEMENT_AT(ARRAY_AGG(product_id ORDER BY qty DESC), 1) as favorite_product,
  ELEMENT_AT(ARRAY_AGG(category ORDER BY qty DESC), 1) as favorite_category

FROM (
  SELECT
    ${unification_id},
    product_id,
    category,
    quantity,
    SUM(quantity) OVER (PARTITION BY ${unification_id}, product_id) as qty
  FROM gldn_<sub>.order_details
)
WHERE ${unification_id} IS NOT NULL
GROUP BY ${unification_id}
```

**E. SMS Activity Attributes**

Create `golden/queries/attributes/sms_activity.sql`:

```sql
-- Customer SMS engagement summary
SELECT
  ${unification_id},

  -- SMS counts
  COUNT(*) as total_sms_sent,
  COUNT(CASE WHEN delivered = 'True' THEN 1 END) as sms_delivered,
  COUNT(CASE WHEN clicked = 'True' THEN 1 END) as sms_clicked,
  COUNT(CASE WHEN failed = 'True' THEN 1 END) as sms_failed,

  -- Engagement rate
  ROUND(100.0 * COUNT(CASE WHEN clicked = 'True' THEN 1 END) / NULLIF(COUNT(*), 0), 2) as sms_click_rate,

  -- Temporal
  MAX(sent_time_unix) as last_sms_sent_time,
  TD_TIME_FORMAT(MAX(sent_time_unix), 'yyyy-MM-dd', 'UTC') as last_sms_sent_date

FROM gldn_<sub>.sms_activity
WHERE ${unification_id} IS NOT NULL
GROUP BY ${unification_id}
```

**F. Survey Responses Attributes**

Create `golden/queries/attributes/survey_responses.sql`:

```sql
-- Customer survey summary
SELECT
  ${unification_id},

  -- Survey counts
  COUNT(*) as total_surveys_completed,
  COUNT(DISTINCT survey_id) as unique_surveys,

  -- Satisfaction scores
  AVG(CASE WHEN question_type = 'nps' THEN score END) as avg_nps_score,
  AVG(CASE WHEN question_type = 'csat' THEN score END) as avg_csat_score,

  -- Latest response
  MAX(response_time_unix) as last_survey_time,
  TD_TIME_FORMAT(MAX(response_time_unix), 'yyyy-MM-dd', 'UTC') as last_survey_date

FROM gldn_<sub>.survey_responses
WHERE ${unification_id} IS NOT NULL
GROUP BY ${unification_id}
```

**Add to wf5_golden.dig:**
```yaml
+create_attributes:
  _parallel: true
  for_each>:
    attr: ['transactions', 'email_activity', 'pageviews', 'order_details', 'sms_activity', 'survey_responses']
  _do:
    td>: golden/queries/attributes/${attr}.sql
    database: gldn_${sub}
    create_table: gldn_${sub}.attr_${attr}
```

---

#### 6. Update wf5_golden.dig (Complete)

Final `wf5_golden.dig`:

```yaml
_export:
  !include : 'config/src_params.yml'
  td:
    database: cdp_unif_${sub}

+create_golden_database:
  td_ddl>:
  create_databases: ["gldn_${sub}"]

+create_profile_identifiers:
  td>: golden/queries/all_profile_identifiers.sql
  database: gldn_${sub}
  create_table: gldn_${sub}.profile_identifiers

+copy_unification_master:
  td>: golden/queries/copy_enriched_table.sql
  database: gldn_${sub}
  create_table: gldn_${sub}.${unification_id}_master
  table: ${unification_id}_master

+copy_row_level_tables:
  _parallel: true
  for_each>:
    tbl: ['orders', 'consents', 'order_details', 'order_digital_transactions', 'order_offline_transactions']
  _do:
    td>: golden/queries/${tbl}.sql
    database: gldn_${sub}
    create_table: gldn_${sub}.${tbl}

+create_attributes:
  _parallel: true
  for_each>:
    attr: ['transactions', 'email_activity', 'pageviews', 'order_details', 'sms_activity', 'survey_responses']
  _do:
    td>: golden/queries/attributes/${attr}.sql
    database: gldn_${sub}
    create_table: gldn_${sub}.attr_${attr}
```

---

#### 7. Push Workflow to TD Console

```bash
cd <sub>_workflow/

# Push golden workflow
tdx wf push <project_name>

# Verify files
tdx wf list <project_name> | grep -E "wf5_golden|golden"
```

---

#### 8. Run Golden Workflow

```bash
# Run golden layer
tdx wf run <project_name> wf5_golden

# Get session ID
SESSION_ID=<returned_session_id>

# Monitor
tdx wf session <SESSION_ID>
```

---

#### 9. Monitor and Handle Errors

**Common Errors & Fixes:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Column not found: ${unification_id}` | Wrong unification ID name | Check unification-report.md for correct name |
| `Table not found: cdp_unif_<sub>.<table>` | Unification didn't create table | Verify unification completed successfully |
| `Division by zero` | NULLIF missing in rate calculations | Add NULLIF(COUNT(*), 0) |
| `Aggregation error: non-aggregated column` | Missing column in GROUP BY | Add all non-aggregated columns to GROUP BY |

---

#### 10. Validate Golden Results

```bash
# Check golden tables created
tdx tables gldn_<sub>

# Check master identity count
tdx query "
SELECT COUNT(*) as total_customers
FROM gldn_<sub>.profile_identifiers
"

# Check attribute tables
tdx query "
SELECT
  COUNT(*) as customers_with_transactions
FROM gldn_<sub>.attr_transactions
"

# Verify all customers in attributes
tdx query "
SELECT
  p.${unification_id},
  p.trfmd_email,
  t.total_orders,
  t.lifetime_revenue,
  e.emails_sent,
  e.open_rate,
  w.total_pageviews
FROM gldn_<sub>.profile_identifiers p
LEFT JOIN gldn_<sub>.attr_transactions t USING (${unification_id})
LEFT JOIN gldn_<sub>.attr_email_activity e USING (${unification_id})
LEFT JOIN gldn_<sub>.attr_pageviews w USING (${unification_id})
LIMIT 20
"
```

**Validation Checklist:**
- [ ] profile_identifiers has 1 row per ${unification_id}
- [ ] All attribute tables have matching ${unification_id} count
- [ ] Row-level tables have ${unification_id} column populated
- [ ] No massive NULL spikes in attribute calculations
- [ ] Attribute values make sense (no negative counts, reasonable averages)

---

#### 11. Generate Golden Summary Report

Create `golden-report.md`:

```markdown
# Golden Layer Report - gldn_<sub>

Generated: <timestamp>

## Summary
- Golden Database: gldn_<sub>
- Total Customers: <count>
- Unification ID: ${unification_id}
- Workflow Session: <SESSION_ID>
- Status: SUCCESS

## Tables Created

### Master Identity
- `profile_identifiers` - <count> customers

### Attribute Tables (1 row per customer)
- `attr_transactions` - <count> customers with purchases
- `attr_email_activity` - <count> customers with email engagement
- `attr_pageviews` - <count> customers with web activity
- `attr_order_details` - <count> customers with order details
- `attr_sms_activity` - <count> customers with SMS engagement
- `attr_survey_responses` - <count> customers with survey responses

### Row-Level Tables (many rows per customer)
- `orders` - <count> rows
- `consents` - <count> rows
- `order_details` - <count> rows
- `order_digital_transactions` - <count> rows
- `order_offline_transactions` - <count> rows

## Attribute Coverage

| Attribute Table | Customers | Coverage % |
|-----------------|-----------|------------|
| Transactions    | 850,000   | 68.0%      |
| Email Activity  | 1,100,000 | 88.0%      |
| Pageviews       | 950,000   | 76.0%      |
| Order Details   | 850,000   | 68.0%      |
| SMS Activity    | 450,000   | 36.0%      |
| Survey Responses| 120,000   | 9.6%       |

## Next Steps
✓ Golden layer complete
→ Ready for segmentation (Step 4)

Parent segment will use:
- Master table: profile_identifiers
- Attributes: All attr_* tables
- Behaviors: orders, consents, pageviews
```

---

#### 12. User Confirmation

```
✓ Golden layer workflow completed successfully!

Created golden tables in: gldn_<sub>
- profile_identifiers: <count> customers
- attr_transactions: <count> customers
- attr_email_activity: <count> customers
- attr_pageviews: <count> customers
- attr_order_details: <count> customers
- attr_sms_activity: <count> customers
- attr_survey_responses: <count> customers

Row-level tables:
- orders, consents, order_details, etc.

Golden report saved to: golden-report.md

Ready to proceed to segmentation? (yes/no)
```

---

## Output for Next Steps

Pass to segmentation_skill:
- Golden database: `gldn_<sub>`
- Master table: `profile_identifiers`
- Unification ID: `${unification_id}`
- Attribute tables list
- Row-level tables list
- Golden report summary
