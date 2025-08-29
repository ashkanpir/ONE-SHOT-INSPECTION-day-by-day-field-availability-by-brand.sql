WITH base AS (
  SELECT
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    event_date,
    event_name,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ssguid' LIMIT 1) AS user_guid,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'sscampaigns' LIMIT 1) AS sscampaigns_raw,
    (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS ga_session_id,
    (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) AS ga_session_number,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer' LIMIT 1) AS page_referrer,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_hostname' LIMIT 1) AS page_hostname,

    user_first_touch_timestamp,
    device.category AS device_category
  FROM `nabuminds.analytics_479167499.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250801' AND '20250814'
),
scoped AS (
  SELECT
    user_pseudo_id,
    event_ts,
    event_date,
    event_name,
    (SELECT SAFE_CAST(SPLIT(sscampaigns_raw, ',')[SAFE_OFFSET(0)] AS INT64)) AS variationgroup_id,
    user_guid,
    ga_session_id,
    ga_session_number,
    page_location,
    page_referrer,
    page_hostname,
    user_first_touch_timestamp,
    device_category
  FROM base
  WHERE --user_guid IS NOT NULL
    page_hostname = 'vidavegas.com'
    AND SAFE_CAST(SPLIT(sscampaigns_raw, ',')[SAFE_OFFSET(0)] AS INT64) IN (1410, 1411)
),
sessions AS (
  SELECT
    user_guid                                         AS User_GUID,
    variationgroup_id                                 AS VariationGroup_ID,
    ga_session_id                                     AS Session_ID,

    MIN(event_ts)                                     AS StartTime_Excel,
    UNIX_SECONDS(MIN(event_ts))                       AS StartTime_SSE,
    TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS VisitLength,

    ARRAY_AGG(page_location ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS EntryPageUrl,
    ARRAY_AGG(page_referrer ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS RefererUrl,

    COUNTIF(event_name = 'page_view')                           AS Pageviews,
    COUNTIF(event_name = 'registration_submitted')              AS Registration_Submitted_Total,
    COUNT(DISTINCT IF(event_name = 'registration_submitted', user_pseudo_id, NULL))
                                                               AS Registration_Submitted_Unique,
    COUNTIF(event_name = 'registration_completed')              AS Registration_Completed_Total,
    COUNT(DISTINCT IF(event_name = 'registration_completed', user_pseudo_id, NULL))
                                                               AS Registration_Completed_Unique,
    COUNTIF(event_name = 'registration_failed')                 AS Registration_Failed_Total,
    COUNT(DISTINCT IF(event_name = 'registration_failed', user_pseudo_id, NULL))
                                                               AS Registration_Failed_Unique,
    COUNTIF(event_name = 'deposit_success')                     AS First_Deposit_Completed_Total,
    COUNT(DISTINCT IF(event_name = 'deposit_success', user_pseudo_id, NULL))
                                                               AS First_Deposit_Completed_Unique,
    COUNTIF(event_name = 'js_error')                            AS JS_Error_Total,
    COUNT(DISTINCT IF(event_name = 'js_error', user_pseudo_id, NULL))
                                                               AS JS_Error_Unique,

    ANY_VALUE(ga_session_number)                                AS AsmtVisitCount,
    MAX(IF(device_category = 'mobile', 1, 0))                   AS Smartphone,

    -- aggregate first-touch timestamp within the group
    MIN(user_first_touch_timestamp)                              AS user_first_touch_ts_agg
  FROM scoped
  GROUP BY User_GUID, VariationGroup_ID, Session_ID
)
SELECT
  *,
  SAFE_DIVIDE(Registration_Completed_Total, Registration_Submitted_Total) AS NRC_Conversion_Rate,
  -- compute days since first touch using the aggregated value
  SAFE_DIVIDE(
    TIMESTAMP_DIFF(StartTime_Excel, TIMESTAMP_MICROS(user_first_touch_ts_agg), SECOND),
    86400.0
  ) AS TimeSinceFirstVisit_days,

  -- GA4 export doesn't include full UA/IP → keep as NULLs (or derive browser info if desired)
  CAST(NULL AS STRING) AS UserAgent,
  CAST(NULL AS STRING) AS Users_IP_Address
FROM sessions
ORDER BY StartTime_Excel;
