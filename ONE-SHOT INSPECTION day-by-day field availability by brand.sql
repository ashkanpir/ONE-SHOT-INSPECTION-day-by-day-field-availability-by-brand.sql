-- ONE-SHOT INSPECTION (no DECLARE): day-by-day field availability by brand
-- Edit these two dates:
WITH params AS (
  SELECT '20250701' AS start_sfx, '20250707' AS end_sfx
),

/* ---------- helper block to avoid repeating the big CASEs ----------
   Replace BRAND / DATASET in each call below.
---------------------------------------------------------------------*/
jetbahis AS (
  SELECT
    'Jetbahis' AS brand,
    'analytics_271530837' AS dataset,
    DATE(TIMESTAMP_MICROS(e.event_timestamp), 'Europe/Athens') AS day,
    e.event_name,
    COUNT(*) AS events_total,

    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('error_code','errorCode','status_code','statusCode','http_status','http_status_code','http_code','code','error-number','error_number','err_code')
        AND (
          p.value.int_value IS NOT NULL OR p.value.double_value IS NOT NULL OR p.value.float_value IS NOT NULL
          OR NULLIF(p.value.string_value,'') IS NOT NULL
          OR JSON_VALUE(SAFE.PARSE_JSON(p.value.string_value),'$.code') IS NOT NULL
        )
    ) THEN 1 ELSE 0 END) AS has_error_code,

    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('error_message','errorMessage','message','msg','error','err_message')
        AND (
          NULLIF(p.value.string_value,'') IS NOT NULL
          OR JSON_VALUE(SAFE.PARSE_JSON(p.value.string_value),'$.message') IS NOT NULL
          OR JSON_VALUE(SAFE.PARSE_JSON(p.value.string_value),'$.msg') IS NOT NULL
        )
    ) THEN 1 ELSE 0 END) AS has_error_message,

    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('provider','deposit_provider','payment_provider','depositProvider')
        AND NULLIF(p.value.string_value,'') IS NOT NULL
    ) THEN 1 ELSE 0 END) AS has_provider,

    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('amount','deposit_amount','value')
        AND (
          p.value.int_value IS NOT NULL OR p.value.double_value IS NOT NULL OR p.value.float_value IS NOT NULL
          OR SAFE_CAST(p.value.string_value AS NUMERIC) IS NOT NULL
        )
    ) THEN 1 ELSE 0 END) AS has_amount,

    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('currency','deposit_currency','currency_code')
        AND NULLIF(p.value.string_value,'') IS NOT NULL
    ) THEN 1 ELSE 0 END) AS has_currency,

    SUM(CASE WHEN (
      NULLIF(e.user_id,'') IS NOT NULL
      OR EXISTS(SELECT 1 FROM UNNEST(e.user_properties) up
                WHERE up.key IN ('user_id','userId','uid','player_id','customer_id')
                  AND NULLIF(up.value.string_value,'') IS NOT NULL)
      OR EXISTS(SELECT 1 FROM UNNEST(e.event_params) p
                WHERE p.key IN ('user_id','userId','uid','player_id','customer_id')
                  AND NULLIF(p.value.string_value,'') IS NOT NULL)
    ) THEN 1 ELSE 0 END) AS has_user_id

  FROM `nabuminds.analytics_271530837.events_*` e, params
  WHERE _TABLE_SUFFIX BETWEEN params.start_sfx AND params.end_sfx
    AND e.event_name IN ('deposit_failed','deposit_success','login_error','registration_error')
  GROUP BY day, e.event_name
),

betchip AS (
  SELECT * REPLACE('Betchip' AS brand, 'analytics_271547113' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
betelli AS (
  SELECT * REPLACE('Betelli' AS brand, 'analytics_271552729' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
betroad AS (
  SELECT * REPLACE('Betroad' AS brand, 'analytics_298828340' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
bluffbet AS (
  SELECT * REPLACE('Bluffbet' AS brand, 'analytics_298834809' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
davegas AS (
  SELECT * REPLACE('Davegas' AS brand, 'analytics_314402899' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
discount_casino AS (
  SELECT * REPLACE('Discount Casino' AS brand, 'analytics_353463445' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
genzobet AS (
  SELECT * REPLACE('Genzobet' AS brand, 'analytics_374934400' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
hovarda AS (
  SELECT * REPLACE('Hovarda' AS brand, 'analytics_411328708' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
intobet AS (
  SELECT * REPLACE('Intobet' AS brand, 'analytics_415075867' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
milyar AS (
  SELECT * REPLACE('Milyar' AS brand, 'analytics_420555141' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
rexbet AS (
  SELECT * REPLACE('Rexbet' AS brand, 'analytics_423856871' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
slotbon AS (
  SELECT * REPLACE('Slotbon' AS brand, 'analytics_424995907' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
winnit AS (
  SELECT * REPLACE('Winnit' AS brand, 'analytics_476027509' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
vidavegas_br AS (
  SELECT * REPLACE('VidaVegas - Brazil' AS brand, 'analytics_479167499' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
vidavegas_latam AS (
  SELECT * REPLACE('VidaVegas - LATAM' AS brand, 'analytics_481292744' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
hitpot AS (
  SELECT * REPLACE('Hitpot' AS brand, 'analytics_490858540' AS dataset)
  FROM jetbahis
  WHERE FALSE
),
jokera AS (
  SELECT * REPLACE('Jokera' AS brand, 'analytics_481292744' AS dataset)  -- adjust if different
  FROM jetbahis
  WHERE FALSE
),

/* Real unions against each dataset (copy of the jetbahis logic but pointed to each dataset) */
u_jetbahis AS ( SELECT * FROM jetbahis ),

u_betchip AS (
  SELECT
    'Betchip' AS brand, 'analytics_271547113' AS dataset,
    DATE(TIMESTAMP_MICROS(e.event_timestamp), 'Europe/Athens') AS day, e.event_name,
    COUNT(*) AS events_total,
    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('error_code','errorCode','status_code','statusCode','http_status','http_status_code','http_code','code','error-number','error_number','err_code')
        AND (p.value.int_value IS NOT NULL OR p.value.double_value IS NOT NULL OR p.value.float_value IS NOT NULL
             OR NULLIF(p.value.string_value,'') IS NOT NULL
             OR JSON_VALUE(SAFE.PARSE_JSON(p.value.string_value),'$.code') IS NOT NULL)
    ) THEN 1 ELSE 0 END) AS has_error_code,
    SUM(CASE WHEN EXISTS (
      SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('error_message','errorMessage','message','msg','error','err_message')
        AND (NULLIF(p.value.string_value,'') IS NOT NULL
             OR JSON_VALUE(SAFE.PARSE_JSON(p.value.string_value),'$.message') IS NOT NULL
             OR JSON_VALUE(SAFE.PARSE_JSON(p.value.string_value),'$.msg') IS NOT NULL)
    ) THEN 1 ELSE 0 END) AS has_error_message,
    SUM(CASE WHEN EXISTS (SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('provider','deposit_provider','payment_provider','depositProvider')
        AND NULLIF(p.value.string_value,'') IS NOT NULL) THEN 1 ELSE 0 END) AS has_provider,
    SUM(CASE WHEN EXISTS (SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('amount','deposit_amount','value')
        AND (p.value.int_value IS NOT NULL OR p.value.double_value IS NOT NULL OR p.value.float_value IS NOT NULL
             OR SAFE_CAST(p.value.string_value AS NUMERIC) IS NOT NULL)) THEN 1 ELSE 0 END) AS has_amount,
    SUM(CASE WHEN EXISTS (SELECT 1 FROM UNNEST(e.event_params) p
      WHERE p.key IN ('currency','deposit_currency','currency_code')
        AND NULLIF(p.value.string_value,'') IS NOT NULL) THEN 1 ELSE 0 END) AS has_currency,
    SUM(CASE WHEN (
      NULLIF(e.user_id,'') IS NOT NULL
      OR EXISTS(SELECT 1 FROM UNNEST(e.user_properties) up
                WHERE up.key IN ('user_id','userId','uid','player_id','customer_id')
                  AND NULLIF(up.value.string_value,'') IS NOT NULL)
      OR EXISTS(SELECT 1 FROM UNNEST(e.event_params) p
                WHERE p.key IN ('user_id','userId','uid','player_id','customer_id')
                  AND NULLIF(p.value.string_value,'') IS NOT NULL)
    ) THEN 1 ELSE 0 END) AS has_user_id
  FROM `nabuminds.analytics_271547113.events_*` e, params
  WHERE _TABLE_SUFFIX BETWEEN params.start_sfx AND params.end_sfx
    AND e.event_name IN ('deposit_failed','deposit_success','login_error','registration_error')
  GROUP BY day, e.event_name
),

/* Repeat this block for each dataset (same as u_betchip, just change brand & dataset + table path) */
u_betelli AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_betroad AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_bluffbet AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_davegas AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_discount_casino AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_genzobet AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_hovarda AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_intobet AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_milyar AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_rexbet AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_slotbon AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_winnit AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_vidavegas_br AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_vidavegas_latam AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_hitpot AS (
  SELECT * FROM u_betchip
  WHERE FALSE
),
u_jokera AS (
  SELECT * FROM u_betchip
  WHERE FALSE
)

SELECT * FROM u_jetbahis
UNION ALL SELECT * FROM u_betchip
UNION ALL SELECT * FROM u_betelli
UNION ALL SELECT * FROM u_betroad
UNION ALL SELECT * FROM u_bluffbet
UNION ALL SELECT * FROM u_davegas
UNION ALL SELECT * FROM u_discount_casino
UNION ALL SELECT * FROM u_genzobet
UNION ALL SELECT * FROM u_hovarda
UNION ALL SELECT * FROM u_intobet
UNION ALL SELECT * FROM u_milyar
UNION ALL SELECT * FROM u_rexbet
UNION ALL SELECT * FROM u_slotbon
UNION ALL SELECT * FROM u_winnit
UNION ALL SELECT * FROM u_vidavegas_br
UNION ALL SELECT * FROM u_vidavegas_latam
UNION ALL SELECT * FROM u_hitpot
UNION ALL SELECT * FROM u_jokera
ORDER BY brand, day, event_name;
