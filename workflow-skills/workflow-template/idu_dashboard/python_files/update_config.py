import json
import os
import digdag

def main():
    """
    Update IDU dashboard config.json with:
    1. Data model name (from workflow parameter)
    2. Shared user list (from notification_emails)
    """
    # Get parameters from Digdag
    params = digdag.env.params

    data_model_name = params.get('idu_data_model_name', 'IDU Dashboard')
    notification_emails = params.get('notification_emails', [])

    # Path to config.json
    config_path = 'idu_dashboard/config.json'

    # Read existing config
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Update config
    config['model_name'] = data_model_name
    config['shared_user_list'] = notification_emails if isinstance(notification_emails, list) else [notification_emails]

    # Write updated config
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"✅ Updated IDU dashboard config.json:")
    print(f"   - Data Model Name: {data_model_name}")
    print(f"   - Shared Users: {config['shared_user_list']}")

    return {
        'data_model_name': data_model_name,
        'shared_users': config['shared_user_list']
    }
