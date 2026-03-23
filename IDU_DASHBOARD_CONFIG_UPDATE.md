# IDU Dashboard Configuration Update

**Date:** 2026-03-21
**Change:** Automatic config.json update with data model name and notification emails

---

## What Changed

The IDU Dashboard workflow now **automatically updates** `idu_dashboard/config.json` with:
1. **Data model name** (user-provided during setup)
2. **Shared user list** (from notification_emails in src_params.yml)

---

## Files Updated

### 1. **idu_dashboard/python_files/update_config.py** (NEW)
Python script that updates config.json before dashboard deployment.

```python
def main():
    """
    Update IDU dashboard config.json with:
    1. Data model name (from workflow parameter)
    2. Shared user list (from notification_emails)
    """
    params = digdag.env.params

    data_model_name = params.get('idu_data_model_name', 'IDU Dashboard')
    notification_emails = params.get('notification_emails', [])

    # Update config.json
    config['model_name'] = data_model_name
    config['shared_user_list'] = notification_emails
```

---

### 2. **idu_dashboard/idu_dashboard_launch.dig**
Added `+update_config` step before existing workflow steps.

**Before:**
```yaml
+set_params:
  _export:
    wf_project_name: ${project_name}
    unif_id: ${unification_id}
  py>: python_files.set_params.main
```

**After:**
```yaml
+update_config:
  _export:
    idu_data_model_name: ${idu_data_model_name}
    notification_emails: ${notification_emails}
  py>: python_files.update_config.main
  docker:
    image: "digdag/digdag-python:3.9"

+set_params:
  _export:
    wf_project_name: ${project_name}
    unif_id: ${unification_id}
  py>: python_files.set_params.main
```

---

### 3. **config/src_params.yml**
Added IDU dashboard configuration fields.

**Added:**
```yaml
## IDU DASHBOARD CONFIG ##
idu_data_model_name: "Retail Value Accelerator - IDU Dashboard"  # Data model name for IDU Dashboard

## NOTIFICATION CONFIG ##
notification_emails:
  - global-presales-demo+dev@treasure-data.com
```

---

## How It Works

### **Step 1: User Provides Data Model Name During Setup**

When setting up a new POC, users are asked:

```bash
# Setup Question (Step 0.2)
"Do you want to include IDU (ID Unification) dashboard?"
  → If YES:
    "What should the IDU Dashboard data model be named?"
    Example: "Acme B2B - ID Unification Dashboard"
```

This value is stored in `config/src_params.yml`:
```yaml
idu_data_model_name: "Acme B2B - ID Unification Dashboard"
```

---

### **Step 2: Workflow Automatically Updates config.json**

When `wf4_idu_dash` runs (if `include_idu_dashboard: true`):

1. **Reads src_params.yml:**
   - `idu_data_model_name` → "Acme B2B - ID Unification Dashboard"
   - `notification_emails` → ["user@acme.com"]

2. **Updates idu_dashboard/config.json:**
   ```json
   {
     "model_name": "Acme B2B - ID Unification Dashboard",
     "shared_user_list": ["user@acme.com"],
     "model_tables": [ ... ]
   }
   ```

3. **Dashboard Created with Correct Name:**
   - Data Model Name: "Acme B2B - ID Unification Dashboard"
   - Shared with: user@acme.com

---

## Setup.md Updates Required

Add to **Step 0.2: Gather POC Requirements** (around line 198):

```markdown
5. **IDU Dashboard** (optional quality dashboard)
   - Ask: "Do you want to include IDU (ID Unification) dashboard for quality monitoring?"
   - Options: yes/no
   - **Store choice**: `include_idu_dashboard: true/false`
   - If no: Will skip IDU dashboard deployment entirely

5a. **IDU Dashboard Data Model Name** (only if IDU Dashboard = yes)
   - Ask: "What should the IDU Dashboard data model be named?"
   - Example: `"Acme Retail - ID Unification Dashboard"`
   - Example: `"B2B Corp - IDU Quality Metrics"`
   - **Store in**: `idu_data_model_name: "<name>"`
   - Used for: Data model and dashboard naming in TD Console
```

Update **Step 0.3: Create src_params.yml** (around line 280):

```yaml
## IDU DASHBOARD CONFIG ##
idu_data_model_name: <user_provided_name>  # Only if include_idu_dashboard: true

## NOTIFICATION CONFIG ##
notification_emails:
  - <notification_email>
```

---

## Example POC Configuration

### **Scenario: B2B SaaS Company**

**User Inputs:**
- Client Name: `acme_saas`
- Include IDU Dashboard: `yes`
- IDU Data Model Name: `"Acme SaaS - ID Unification Quality Dashboard"`
- Notification Emails: `["ops@acme.com", "analytics@acme.com"]`

**Generated src_params.yml:**
```yaml
sub: acme_saas
project_name: acme-saas-cdp
unification_id: canonical_id

optional_workflows:
  include_idu_dashboard: true
  include_analytics_dashboard: false

idu_data_model_name: "Acme SaaS - ID Unification Quality Dashboard"

notification_emails:
  - ops@acme.com
  - analytics@acme.com
```

**When wf4_idu_dash runs:**

Automatically updates `idu_dashboard/config.json`:
```json
{
  "model_name": "Acme SaaS - ID Unification Quality Dashboard",
  "shared_user_list": ["ops@acme.com", "analytics@acme.com"],
  "model_tables": [ ... ]
}
```

**Result in TD Console:**
- Data Model Name: "Acme SaaS - ID Unification Quality Dashboard"
- Dashboard shared with: ops@acme.com, analytics@acme.com

---

## Benefits

✅ **No manual config.json editing** - Fully automated
✅ **Consistent naming** - Data model name matches client requirements
✅ **Automatic user sharing** - Dashboard shared with notification emails
✅ **POC-specific** - Each POC gets custom configuration
✅ **Less error-prone** - No forgotten manual steps

---

## Backward Compatibility

**For existing POCs without idu_data_model_name:**

The workflow has a default fallback:
```python
data_model_name = params.get('idu_data_model_name', 'IDU Dashboard')
```

If `idu_data_model_name` is not in src_params.yml:
- Uses default: "IDU Dashboard"
- Workflow continues without error

**To update existing POC:**
```yaml
# Add to config/src_params.yml
idu_data_model_name: "Client Name - IDU Dashboard"
```

---

## Testing

### **Test the Update Script:**

```bash
cd <poc>_workflow/

# Run update config step manually
td wf run <project_name> wf4_idu_dash --session now
```

**Verify:**
1. Check `idu_dashboard/config.json` was updated:
   ```bash
   cat idu_dashboard/config.json | grep "model_name"
   cat idu_dashboard/config.json | grep "shared_user_list"
   ```

2. Confirm data model created in TD Console:
   - Navigate to **Data Workbench > Data Models**
   - Look for data model with correct name

---

## Troubleshooting

### ❌ config.json not updated

**Check:**
1. `idu_data_model_name` exists in `config/src_params.yml`
2. `notification_emails` exists in `config/src_params.yml`
3. `update_config.py` file exists in `idu_dashboard/python_files/`
4. Workflow includes `+update_config` step before `+set_params`

### ❌ Dashboard has wrong name

**Check:**
1. Verify `idu_data_model_name` value in `config/src_params.yml`
2. Check workflow logs for `+update_config` step output
3. Verify `config.json` shows correct `model_name` value

### ❌ Dashboard not shared with users

**Check:**
1. `notification_emails` is a valid list in `config/src_params.yml`:
   ```yaml
   notification_emails:
     - user1@company.com
     - user2@company.com
   ```
2. `config.json` shows correct `shared_user_list`
3. Users have valid TD Console accounts

---

**Status:** ✅ Complete - IDU Dashboard config.json automatically updated

**Files Modified:**
- ✅ `idu_dashboard/python_files/update_config.py` (created)
- ✅ `idu_dashboard/idu_dashboard_launch.dig` (updated)
- ✅ `config/src_params.yml` (template updated)

**Documentation:**
- ⚠️ `setup.md` - Needs update to add data model name question
- ✅ `IDU_DASHBOARD_CONFIG_UPDATE.md` - This file
