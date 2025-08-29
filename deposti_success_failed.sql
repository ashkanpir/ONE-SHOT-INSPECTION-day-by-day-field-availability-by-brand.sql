-- Adjust the date range once and paste all blocks
-- Counts deposit events per dataset in the window
SELECT 'analytics_271530837' AS dataset, COUNT(*) AS cnt FROM `nabuminds.analytics_271530837.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_271547113', COUNT(*) FROM `nabuminds.analytics_271547113.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_374934400', COUNT(*) FROM `nabuminds.analytics_374934400.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_298828340', COUNT(*) FROM `nabuminds.analytics_298828340.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_423856871', COUNT(*) FROM `nabuminds.analytics_423856871.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_411328708', COUNT(*) FROM `nabuminds.analytics_411328708.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_353463445', COUNT(*) FROM `nabuminds.analytics_353463445.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_298834809', COUNT(*) FROM `nabuminds.analytics_298834809.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_314402899', COUNT(*) FROM `nabuminds.analytics_314402899.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_424995907', COUNT(*) FROM `nabuminds.analytics_424995907.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_271530837', COUNT(*) FROM `nabuminds.analytics_271530837.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_448461531', COUNT(*) FROM `nabuminds.analytics_448461531.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_476027509', COUNT(*) FROM `nabuminds.analytics_476027509.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_415075867', COUNT(*) FROM `nabuminds.analytics_415075867.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_479167499', COUNT(*) FROM `nabuminds.analytics_479167499.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_490858540', COUNT(*) FROM `nabuminds.analytics_490858540.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
UNION ALL SELECT 'analytics_481292744', COUNT(*) FROM `nabuminds.analytics_481292744.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20250701' AND '20250825'
  AND event_name IN ('deposit_failed','deposit_success')
ORDER BY cnt DESC;
