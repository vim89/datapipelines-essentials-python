SELECT 
nct_id, 
agency_class, 
lead_or_collaborator, 
name, 
CAST(ins_ts AS TIMESTAMP) AS ins_ts,
{prcs_run_id} AS prcs_run_id,
{job_run_id} AS job_run_id,
{rec_crt_user} AS rec_crt_user
FROM
(
	SELECT
	xmltable_sponsors_lead_sponsor.pk_nct_id AS nct_id,
	agency_class agency_class,
	'lead' AS lead_or_collaborator,
	agency AS name,
	xmltable_sponsors_lead_sponsor.spark_ts AS ins_ts
	FROM xmltable_sponsors_lead_sponsor
	LEFT JOIN xmltable_sponsors ON xmltable_sponsors_lead_sponsor.surrogate_id_xmltable_sponsors = xmltable_sponsors_lead_sponsor.surrogate_id_xmltable_sponsors
	AND xmltable_sponsors.pk_nct_id = xmltable_sponsors_lead_sponsor.pk_nct_id

	UNION ALL

	SELECT
	xmltable_sponsors_collaborator.pk_nct_id AS nct_id,
	agency_class AS agency_class,
	'collaborator' AS lead_or_collaborator,
	agency AS name,
	xmltable_sponsors_collaborator.spark_ts AS ins_ts
	FROM xmltable_sponsors_collaborator
	LEFT JOIN xmltable_sponsors ON xmltable_sponsors.surrogate_id_xmltable_sponsors = xmltable_sponsors_collaborator.surrogate_id_xmltable_sponsors
	AND xmltable_sponsors.pk_nct_id = xmltable_sponsors_collaborator.pk_nct_id
) sponsors
