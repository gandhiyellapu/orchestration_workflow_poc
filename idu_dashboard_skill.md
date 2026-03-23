Purpose: Build ID Unification quality dashboard and QA data flows.

Template reference: workflow-template/idu_dashboard/
Helper skills:

None beyond SQL and existing IDU dashboard template.

Behavior (only if user says IDU dashboard is needed):

Configure and orchestrate:
idu_dashboard/idu_dashboard_launch.dig
idu_dashboard_data_prep.dig — column mappings, known/unknown stats, ID histograms, match rates.
idu_qa.dig — over-merge detection, frequent ID analysis.
idu_datamodel_create.dig / idu_datamodel_build.dig — reporting datamodel.
idu_cleanup_runner.dig — cleanup of temp tables.
Customize:
idu_dashboard/config/params.yml with:
Unification ID.
Databases (cdp_unif_<sub>, gldn_<sub> if needed).
Dashboard user emails.
If user says skip IDU dashboard:
Comment out or bypass all idu_dashboard/* workflow calls.
