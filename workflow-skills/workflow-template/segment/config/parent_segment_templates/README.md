# Parent Segment Templates

## Automatic Parent Segment Creation

When the **segmentation workflow** (`wf7_create_refresh_master_segment`) runs, it **automatically creates parent segments** from all `.yml` template files in this directory.

---

## How It Works

### Step 1: List Templates
The workflow scans this directory for all `.yml` files:
```python
# Only .yml files are processed
# .yml.sample, .yml.bak, etc. are ignored
```

### Step 2: Variable Substitution
Template variables are replaced with actual values from `config/src_params.yml`:
- `${sub}` → Subscriber name (e.g., `b2b_dev`)
- `${unification_id}` → Unification ID (e.g., `canonical_id`)
- `${gld}` → Golden database prefix (e.g., `gldn`)

### Step 3: Create Parent Segments
Each template is used to create a parent segment via TD CDP API.

---

## Available Templates

### ✅ **b2b_parent_segment_template.yml** (Active)
B2B-focused parent segment with:
- **32 Attributes:**
  - Identity: Account ID, Email, Phone
  - Demographics: Name, Job Title, Department, Seniority
  - Company: Account Name, Industry, Revenue, Size
  - Subscription: Status, Plan, MRR
  - Support: Cases, Priority
  - Sales: Opportunities, Pipeline Value
  - Engagement: Pageviews, Sessions, Product Usage

- **7 Behaviors:**
  - Subscriptions, Support Cases, Sales Opportunities
  - Pageviews, Partner Activities, Product Usage, Contacts

**Data Source:** `gldn_${sub}` database
- Master Table: `profile_identifiers`
- Attribute Tables: `unification_master`, `attr_*`
- Behavior Tables: `enriched_*`

---

### 📄 **retail_parent_segment_template.yml.sample** (Reference Only)
Retail-focused template (not automatically created).
- To use: Rename to `.yml` to activate

---

## Creating Custom Templates

### Add a New Template

1. **Create YAML file:**
   ```bash
   touch custom_parent_segment_template.yml
   ```

2. **Configure attributes:**
   ```yaml
   id: null
   name: Custom Parent Segment
   description: Custom parent segment for ${sub}
   master:
     parentDatabaseName: gldn_${sub}
     parentTableName: profile_identifiers
     parentKey: ${unification_id}

   attributes:
   - audienceId: null
     id: null
     name: Custom Attribute
     type: string
     parentDatabaseName: gldn_${sub}
     parentTableName: unification_master
     parentColumn: custom_column
     parentKey: ${unification_id}
     foreignKey: ${unification_id}
     matrixColumnName: custom_column
     groupingName: Custom Group
   ```

3. **Deploy workflow:**
   ```bash
   td wf push <project_name>
   td wf run <project_name> wf7_create_refresh_master_segment
   ```

---

## Template Variables

All templates support these Digdag variables:

| Variable | Example Value | Description |
|----------|---------------|-------------|
| `${sub}` | `b2b_dev` | Subscriber/client name |
| `${unification_id}` | `canonical_id` | Unification ID column name |
| `${gld}` | `gldn` | Golden database prefix |
| `${site}` | `us` | TD site (us/eu/ap) |

**Example:**
```yaml
parentDatabaseName: gldn_${sub}  # Becomes: gldn_b2b_dev
parentKey: ${unification_id}      # Becomes: canonical_id
```

---

## Disabling Templates

To prevent a template from being created automatically:

**Option 1: Rename with .sample extension**
```bash
mv template.yml template.yml.sample
```

**Option 2: Move to backup folder**
```bash
mkdir -p ../backups
mv template.yml ../backups/
```

**Option 3: Delete the file**
```bash
rm template.yml
```

---

## Testing Templates

Before deploying to production:

1. **Validate YAML syntax:**
   ```bash
   python -c "import yaml; yaml.safe_load(open('b2b_parent_segment_template.yml'))"
   ```

2. **Check table/column names:**
   ```bash
   td tables gldn_b2b_dev profile_identifiers
   td tables gldn_b2b_dev unification_master
   td tables gldn_b2b_dev attr_subscriptions
   ```

3. **Test with small data:**
   - Add `WHERE LIMIT 1000` to test queries first
   - Verify attribute values populate correctly

4. **Run workflow:**
   ```bash
   td wf run <project_name> wf7_create_refresh_master_segment --session now
   ```

---

## Troubleshooting

### ❌ Parent segment not created

**Check:**
1. File has `.yml` extension (not `.yml.sample`)
2. YAML syntax is valid
3. All referenced tables exist in golden database
4. Column names match actual schema
5. Workflow completed successfully

### ❌ Missing attributes in parent segment

**Check:**
1. Attribute table exists: `td tables gldn_${sub} <table_name>`
2. Column exists in table: `td query "SELECT <column_name> FROM gldn_${sub}.<table_name> LIMIT 1"`
3. Data type matches (string/number/timestamp)
4. parentKey and foreignKey are correct

### ❌ Variables not substituted

**Check:**
1. Using `${variable}` syntax (not `{variable}`)
2. Variable defined in `config/src_params.yml`
3. Workflow includes `config/src_params.yml` in `_export`

---

## Best Practices

✅ **Use descriptive attribute names** - `Latest Opportunity Stage` not `opp_stage`
✅ **Group related attributes** - Use `groupingName` for organization
✅ **Include timestamps** - Always add date fields for recency filters
✅ **Test incrementally** - Start with 5-10 attributes, then expand
✅ **Document custom fields** - Add descriptions for business users
✅ **Version control** - Keep templates in git for change tracking

---

**Last Updated:** 2026-03-21
**Active Templates:** 1 (b2b_parent_segment_template.yml)
**Reference Templates:** 1 (retail_parent_segment_template.yml.sample)
