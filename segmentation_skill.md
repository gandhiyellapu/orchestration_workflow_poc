Purpose: Configure parent segment from the golden layer to enable audience creation and activation.

Template reference: workflow-template/segment/
Helper skills:
  - tdx-skills/parent-segment — tdx ps validation/preview
  - tdx-skills/segment — tdx sg child segment creation/activation

---

## Step 4: Segmentation (Parent Segment Configuration)

**Goal**: Create a parent segment (master segment) from golden layer data to enable audience building and activation.

### Prerequisites
Before segmentation, ensure:
1. Golden layer is complete (`gldn_<sub>` database exists)
2. Golden report confirms all attribute and behavior tables
3. Parent segment template available
4. TDX CLI installed and configured

---

### Input from Golden Layer
Read `golden-report.md`:
- Golden database: `gldn_<sub>`
- Master table: `profile_identifiers`
- Unification ID: `${unification_id}`
- Attribute tables: `attr_transactions`, `attr_email_activity`, etc.
- Row-level tables: `orders`, `consents`, etc.

---

### Parent Segment Structure

A parent segment consists of:
1. **Master Table** - Core customer identity (1 row per customer)
2. **Attributes** - Aggregated customer properties (1 row per customer)
3. **Behaviors** - Transactional/event data (many rows per customer)

```
Parent Segment
├── Master: profile_identifiers
│   └── Key: ${unification_id}
│
├── Attributes (joined to master by ${unification_id}):
│   ├── Identity (email, phone, name)
│   ├── Demographics (age, gender, location)
│   ├── Purchase (transactions, orders, revenue)
│   ├── Email Engagement (sends, opens, clicks)
│   ├── Web Behavior (pageviews, sessions)
│   ├── SMS Engagement (sends, clicks)
│   └── Survey Feedback (NPS, CSAT)
│
└── Behaviors (linked to master by ${unification_id}):
    ├── Orders (order history)
    ├── Consents (consent records)
    ├── Order Details (line items)
    └── Pageviews (web events)
```

---

### Segmentation Workflow

#### 1. Read Golden Table Schemas

For each golden table, get actual column names:

```bash
# Get profile_identifiers schema
tdx tables gldn_<sub> profile_identifiers

# Get attribute table schemas
tdx tables gldn_<sub> attr_transactions
tdx tables gldn_<sub> attr_email_activity
tdx tables gldn_<sub> attr_pageviews
tdx tables gldn_<sub> attr_order_details
tdx tables gldn_<sub> attr_sms_activity
tdx tables gldn_<sub> attr_survey_responses

# Get behavior table schemas
tdx tables gldn_<sub> orders
tdx tables gldn_<sub> consents
tdx tables gldn_<sub> order_details
```

**Store schemas** for mapping to parent segment config.

---

#### 2. Configure Parent Segment Template

Edit `segment/config/parent_segment_templates/retail_parent_segment_template.yml`:

```yaml
# Parent Segment Configuration
# Auto-generated from golden layer

master:
  parentDatabaseName: gldn_${sub}
  parentTableName: profile_identifiers
  parentKey: ${unification_id}
  description: "Master customer segment for ${sub}"

# Attribute tables (1 row per customer)
attributes:
  # Identity Group
  - groupName: "Identity"
    attributes:
      - attributeName: "customer_id"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: trfmd_customer_id
        dataType: string
        description: "Customer ID"

      - attributeName: "email"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: trfmd_email
        dataType: string
        description: "Customer email"

      - attributeName: "phone"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: trfmd_phone
        dataType: string
        description: "Customer phone"

      - attributeName: "full_name"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: trfmd_full_name
        dataType: string
        description: "Customer full name"

  # Demographics Group
  - groupName: "Demographics"
    attributes:
      - attributeName: "age"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: age
        dataType: long
        description: "Customer age"

      - attributeName: "gender"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: gender
        dataType: string
        description: "Customer gender"

      - attributeName: "city"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: city
        dataType: string
        description: "Customer city"

      - attributeName: "state"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: state
        dataType: string
        description: "Customer state"

      - attributeName: "country"
        parentDatabaseName: gldn_${sub}
        parentTableName: profile_identifiers
        parentKey: ${unification_id}
        columnName: country
        dataType: string
        description: "Customer country"

  # Purchase Behavior Group
  - groupName: "Purchase Behavior"
    attributes:
      - attributeName: "total_orders"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: total_orders
        dataType: long
        description: "Total number of orders"

      - attributeName: "lifetime_revenue"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: lifetime_revenue
        dataType: double
        description: "Total lifetime revenue"

      - attributeName: "average_order_value"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: average_order_value
        dataType: double
        description: "Average order value"

      - attributeName: "first_purchase_date"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: first_purchase_date
        dataType: string
        description: "First purchase date"

      - attributeName: "last_purchase_date"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: last_purchase_date
        dataType: string
        description: "Last purchase date"

      - attributeName: "days_since_last_purchase"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: days_since_last_purchase
        dataType: long
        description: "Days since last purchase"

      - attributeName: "recency_days"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: recency_days
        dataType: long
        description: "Recency (days since last purchase)"

      - attributeName: "frequency"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: frequency
        dataType: long
        description: "Frequency (total orders)"

      - attributeName: "monetary"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_transactions
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: monetary
        dataType: double
        description: "Monetary (lifetime revenue)"

  # Email Engagement Group
  - groupName: "Email Engagement"
    attributes:
      - attributeName: "total_emails_sent"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_email_activity
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: total_emails_sent
        dataType: long
        description: "Total emails sent"

      - attributeName: "emails_opened"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_email_activity
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: emails_opened
        dataType: long
        description: "Emails opened"

      - attributeName: "emails_clicked"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_email_activity
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: emails_clicked
        dataType: long
        description: "Emails clicked"

      - attributeName: "email_open_rate"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_email_activity
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: open_rate
        dataType: double
        description: "Email open rate %"

      - attributeName: "email_click_rate"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_email_activity
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: click_rate
        dataType: double
        description: "Email click rate %"

  # Web Behavior Group
  - groupName: "Web Behavior"
    attributes:
      - attributeName: "total_pageviews"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_pageviews
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: total_pageviews
        dataType: long
        description: "Total pageviews"

      - attributeName: "total_sessions"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_pageviews
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: total_sessions
        dataType: long
        description: "Total sessions"

      - attributeName: "unique_pages_viewed"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_pageviews
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: unique_pages_viewed
        dataType: long
        description: "Unique pages viewed"

      - attributeName: "days_since_last_visit"
        parentDatabaseName: gldn_${sub}
        parentTableName: attr_pageviews
        parentKey: ${unification_id}
        foreignKey: ${unification_id}
        columnName: days_since_last_visit
        dataType: long
        description: "Days since last website visit"

# Behavior tables (many rows per customer)
behaviors:
  - behaviorName: "Orders"
    parentDatabaseName: gldn_${sub}
    parentTableName: orders
    parentKey: ${unification_id}
    foreignKey: ${unification_id}
    description: "Customer order history"
    columns:
      - columnName: order_id
        dataType: string
        description: "Order ID"
      - columnName: order_date_unix
        dataType: long
        description: "Order date (unix timestamp)"
      - columnName: order_amount
        dataType: double
        description: "Order total amount"
      - columnName: channel
        dataType: string
        description: "Order channel (online/offline)"
      - columnName: status
        dataType: string
        description: "Order status"

  - behaviorName: "Consents"
    parentDatabaseName: gldn_${sub}
    parentTableName: consents
    parentKey: ${unification_id}
    foreignKey: ${unification_id}
    description: "Customer consent records"
    columns:
      - columnName: consent_type
        dataType: string
        description: "Type of consent"
      - columnName: consent_status
        dataType: string
        description: "Consent status (opt-in/opt-out)"
      - columnName: consent_date_unix
        dataType: long
        description: "Consent date (unix timestamp)"
      - columnName: channel
        dataType: string
        description: "Consent channel (email/sms/phone)"

  - behaviorName: "Order Details"
    parentDatabaseName: gldn_${sub}
    parentTableName: order_details
    parentKey: ${unification_id}
    foreignKey: ${unification_id}
    description: "Order line items"
    columns:
      - columnName: order_id
        dataType: string
        description: "Order ID"
      - columnName: product_id
        dataType: string
        description: "Product ID"
      - columnName: product_name
        dataType: string
        description: "Product name"
      - columnName: category
        dataType: string
        description: "Product category"
      - columnName: quantity
        dataType: long
        description: "Quantity purchased"
      - columnName: price
        dataType: double
        description: "Unit price"
```

**Key Configuration Rules:**
1. **parentKey** - Always the ${unification_id} in master table
2. **foreignKey** - Always the ${unification_id} in attribute/behavior tables
3. **dataType** - Must match actual column type: string, long, double, timestamp
4. **Remove non-existent** - Delete any template attributes referencing tables you don't have

---

#### 3. Validate Parent Segment Configuration

Before creating, validate the config:

```bash
# Validate parent segment YAML
tdx ps validate segment/config/parent_segment_templates/retail_parent_segment_template.yml
```

**Check for:**
- [ ] All referenced tables exist in `gldn_<sub>`
- [ ] All column names match actual schema
- [ ] Data types are correct
- [ ] parentKey/foreignKey are consistent
- [ ] No duplicate attributeName or behaviorName

---

#### 4. Update wf7_segment.dig Workflow

Verify `wf7_segment.dig`:

```yaml
_export:
  !include : 'config/src_params.yml'
  td:
    database: va_config_${sub}

+create_config_database:
  td_ddl>:
  create_databases: ["va_config_${sub}"]

+create_tmp_tables:
  td_ddl>:
  empty_tables: ["${segment.tables.parent_segment_templates}"]

+create_tables:
  td_ddl>:
  create_tables: ["${segment.tables.parent_segment_creation}"]

+list_all_templates:
  _export:
    folder_path: segment/config/parent_segment_templates
    db: va_config_${sub}
    table: ${segment.tables.parent_segment_templates}
  docker:
    image: "digdag/digdag-python:3.9"
  py>: segment.python.list_templates.main
  _env:
    TD_SITE: ${site}
    TD_API_KEY: ${secret:td_apikey}

+create_parent_segments:
  _export:
    !include : 'config/src_params.yml'
  _parallel: true
  td_for_each>: segment/queries/lookup_templates.sql
  _do:
    +create:
      _export:
        folder: ${td.each.folder}
        file_name: ${td.each.file}
        database: va_config_${sub}
        table: ${segment.tables.parent_segment_creation}
        parent_db: gldn_${sub}
        unif_id: ${unification_id}
        run_type: ${segment.run_type}
      docker:
        image: "digdag/digdag-python:3.9"
      py>: segment.python.create_parent_segments.main
      _env:
        TD_SITE: ${site}
        TD_API_KEY: ${secret:td_apikey}

+clean_log:
  td>: segment/queries/cleanup_log.sql

+active_audience:
  td>: segment/queries/active_audience.sql
  create_table: ${segment.tables.active_audience}

+refresh_parent_segments:
  _parallel: true
  td_for_each>: segment/queries/get_active_audience.sql
  _do:
      +parent_segment_refresh:
          require>: audience
          project_name: cdp_audience_${td.each.audience_id}
          session_time: ${moment(session_time).format()}
```

---

#### 5. Update config/src_params.yml

Add segment configuration:

```yaml
# Segment configuration
segment:
  run_type: "create"  # "create" for first run, "update" for subsequent
  tables:
    parent_segment_templates: ps_templates
    parent_segment_creation: ps_creation_log
    active_audience: active_audiences
```

---

#### 6. Push Workflow to TD Console

```bash
cd <sub>_workflow/

# Push segment workflow
tdx wf push <project_name>

# Verify files
tdx wf list <project_name> | grep -E "wf7_segment|segment"
```

---

#### 7. Run Segmentation Workflow

```bash
# Run parent segment creation
tdx wf run <project_name> wf7_segment

# Get session ID
SESSION_ID=<returned_session_id>

# Monitor
tdx wf session <SESSION_ID>
```

---

#### 8. Monitor and Handle Errors

**Common Errors & Fixes:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Table not found: gldn_<sub>.<table>` | Golden table missing | Verify golden layer completed |
| `Column not found: <column>` | Wrong column name in YAML | Check table schema, update YAML |
| `Data type mismatch` | Wrong dataType in YAML | Update dataType to match actual schema |
| `Parent segment already exists` | Duplicate creation | Change run_type to "update" |
| `API authentication failed` | TD API key not set | Add secret: `td wf secret set <project> td_apikey` |

---

#### 9. Verify Parent Segment Created

After successful run:

```bash
# List parent segments
tdx ps list

# Get parent segment details
tdx ps get <parent_segment_id>

# Preview parent segment data
tdx ps preview <parent_segment_id> --limit 100
```

**Expected output:**
```
Parent Segment: <parent_segment_id>
Master Table: gldn_<sub>.profile_identifiers
Total Profiles: <count>
Attributes: <count>
Behaviors: <count>
```

---

#### 10. Test Segment Creation

Create a test child segment:

```bash
# Example: High-value customers
tdx sg create \
  --parent-segment <parent_segment_id> \
  --name "High Value Customers" \
  --description "Customers with lifetime revenue > $1000" \
  --filter "lifetime_revenue > 1000"

# Get segment ID
SEGMENT_ID=<returned_segment_id>

# Check segment size
tdx sg get <SEGMENT_ID>
```

**Expected:**
```
Segment: High Value Customers
Size: <count> customers
Parent Segment: <parent_segment_id>
Filter: lifetime_revenue > 1000
```

---

#### 11. Generate Segmentation Summary Report

Create `segmentation-report.md`:

```markdown
# Segmentation Report - Parent Segment

Generated: <timestamp>

## Summary
- Parent Segment ID: <parent_segment_id>
- Master Table: gldn_<sub>.profile_identifiers
- Total Profiles: <count>
- Workflow Session: <SESSION_ID>
- Status: SUCCESS

## Parent Segment Configuration

### Master Table
- Database: gldn_<sub>
- Table: profile_identifiers
- Key: ${unification_id}

### Attributes (<count> total)
| Group | Attributes | Source Table |
|-------|-----------|--------------|
| Identity | customer_id, email, phone, full_name | profile_identifiers |
| Demographics | age, gender, city, state, country | profile_identifiers |
| Purchase Behavior | total_orders, lifetime_revenue, AOV, etc. | attr_transactions |
| Email Engagement | emails_sent, open_rate, click_rate | attr_email_activity |
| Web Behavior | pageviews, sessions, days_since_visit | attr_pageviews |
| SMS Engagement | sms_sent, click_rate | attr_sms_activity |
| Survey Feedback | NPS, CSAT | attr_survey_responses |

### Behaviors (<count> total)
| Behavior | Columns | Source Table |
|----------|---------|--------------|
| Orders | order_id, order_date, amount, channel | orders |
| Consents | consent_type, status, date, channel | consents |
| Order Details | product_id, category, quantity, price | order_details |

## Test Segments Created
1. High Value Customers - <count> customers (lifetime_revenue > 1000)
2. Recent Purchasers - <count> customers (days_since_last_purchase < 30)
3. Email Engaged - <count> customers (open_rate > 20)

## Next Steps
✓ Parent segment created
→ Ready for audience activation

You can now:
1. Create child segments in TD Console
2. Activate segments to destinations (Facebook, Google Ads, etc.)
3. Build journeys using segment triggers
4. Monitor segment performance
```

---

#### 12. User Confirmation

```
✓ Segmentation workflow completed successfully!

Parent Segment Created:
- ID: <parent_segment_id>
- Total Profiles: <count>
- Attributes: <count>
- Behaviors: <count>

Parent segment includes:
- Identity (email, phone, customer_id)
- Demographics (age, gender, location)
- Purchase Behavior (orders, revenue, RFM)
- Email Engagement (sends, opens, clicks)
- Web Behavior (pageviews, sessions)

Test segments created:
- High Value Customers: <count>
- Recent Purchasers: <count>

Segmentation report saved to: segmentation-report.md

Your CDP pipeline is now complete! ✅

Next steps:
1. Create custom audience segments
2. Activate segments to marketing channels
3. Build customer journeys
4. Monitor and optimize
```

---

## Best Practices

1. **Start with core attributes** - Add identity, demographics, purchase first
2. **Test with small segments** - Validate parent segment works before scaling
3. **Use descriptive names** - Make attributeName clear and searchable
4. **Group logically** - Organize attributes by category for easy navigation
5. **Include timestamps** - Always include date fields for recency/frequency
6. **Document calculations** - Add descriptions explaining derived metrics
7. **Monitor performance** - Check parent segment refresh times

---

## Output for Orchestration

Pass to orchestration workflow:
- Parent segment ID: `<parent_segment_id>`
- Configuration file: `retail_parent_segment_template.yml`
- Segmentation report summary
- Ready for activation
