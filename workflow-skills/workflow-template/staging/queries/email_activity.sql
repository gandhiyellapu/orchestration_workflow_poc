CREATE TABLE IF NOT EXISTS ${stg}_${sub}.${tbl} (
  time bigint
);

INSERT INTO ${stg}_${sub}.${tbl}

with max_time as (
  select COALESCE(max(time),0) as max_time from ${stg}_${sub}.${tbl}
)

SELECT
*,
--
TD_TIME_PARSE(activity_date) as trfmd_activity_date_unix,
--
cast(COALESCE(regexp_like( "email", '^(?=.{1,256})(?=.{1,64}@.{1,255}$)[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'), false) as varchar)  AS  "valid_email_flag",
--
case
  when nullif(lower(ltrim(rtrim("activity_type"))), 'null') is null then null
  when nullif(lower(ltrim(rtrim("activity_type"))), '') is null then null
  else array_join((transform((split(regexp_replace(lower(trim("activity_type")), '[ .]', ' '),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
end   AS  "trfmd_activity_type",
--
case
  when nullif(lower(ltrim(rtrim("campaign_name"))), 'null') is null then null
  when nullif(lower(ltrim(rtrim("campaign_name"))), '') is null then null
  else  array_join((transform((split(lower(trim("campaign_name")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','')
end   AS  "trfmd_campaign_name",
--
case
  when nullif(lower(ltrim(rtrim("email"))), 'null') is null then null
  when nullif(lower(ltrim(rtrim("email"))), '') is null then null
  when nullif(lower(trim("email")), '') in (select lower(trim(invalid_email)) from ${stg}_${sub}.invalid_emails ) then null
  else lower(ltrim(rtrim(regexp_replace("email", '[^a-zA-Z0-9.@_+-]', ''))))
end   AS  "trfmd_email",

case
  when nullif(lower(ltrim(rtrim("phone_number"))), 'null') is null then null
  when nullif(lower(ltrim(rtrim("phone_number"))), '') is null then null
  else ARRAY_JOIN(REGEXP_EXTRACT_ALL(replace(lower(ltrim(rtrim("phone_number"))), ' ', ''), '([0-9]+)?'), '')
end AS "trfmd_phone_number"

FROM

email_activity
WHERE
  time > (SELECT max_time FROM max_time)