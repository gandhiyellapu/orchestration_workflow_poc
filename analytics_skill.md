Purpose: Build analytics datamodels and dashboards (sales, web, IDU, others) using golden/unification data.

Template reference: workflow-template/analytics/
Helper skills:

sql-skills/hive — Market basket, lateral view, MAPJOIN.
sql-skills/trino — Web analytics, funnels.
sql-skills/time-filtering — Time-based aggregations.

Behavior:

Configure wf6_analytics.dig to:
Optionally call IDU dashboard sub-workflow (if not skipped).
Launch analytics dashboards.
Customize analytics queries based on available data:
analytics/queries/sales/ — sales trends, RFM, market basket (Hive).
analytics/queries/web_analytics/ — web analytics, conversion funnels, Sankey.
analytics/queries/create_model_config.sql — reporting datamodel config.
Configure:
analytics/python/create_datamodel.py — datamodel creation via API.
analytics/config/datamodels/*.json — analytics datamodel definitions.
analytics/config/web_analytics.yml — web analytics configuration.
analytics/dashboard/* — dashboard launch, datamodel create/build, templates, params.

If the user chooses to skip analytics, comment out analytics-specific sections in wf06_analytics.dig and related sub-workflows.
