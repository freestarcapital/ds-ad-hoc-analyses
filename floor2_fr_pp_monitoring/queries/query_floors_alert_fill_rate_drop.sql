DECLARE test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

WITH base AS (
  SELECT REGEXP_REPLACE(net.host(page_url), 'undefined$', '') host,
  TIMESTAMP_MILLIS(server_time) > TIMESTAMP_SUB(test_time, INTERVAL 1 HOUR) last_hour,
  SAFE_DIVIDE(SUM(CASE WHEN REGEXP_CONTAINS(co.Name, '(?i)^((fspb_.*)|(Google Ad Exchange)|(Amazon)|(freestar_prebid)|(Adexchange)|(Ad Exchange))$') THEN 1 ElSE 0 END), count(1)) prog_fill_rate,
  count(1) auctions
  FROM `freestar-157323.prod_eventstream.auction_end_raw` auc
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableCompany_15184186` co ON co._PARTITIONDATE = DATE_SUB(DATE(test_time), INTERVAL 1 DAY) AND co.ID = advertiser_id
  WHERE auc._PARTITIONDATE >= DATE_SUB(DATE(test_time), INTERVAL 1 DAY)
  AND auc._PARTITIONDATE <= DATE(test_time)
  AND TIMESTAMP_MILLIS(server_time) > TIMESTAMP_SUB(test_time, INTERVAL 4 HOUR)
  AND EXISTS ( SELECT COUNT(1) FROM UNNEST(auc.kvps) kvpss
        WHERE kvpss LIKE "floors_id=%"
        )
  AND net.host(page_url) IS NOT NULL
  GROUP BY 1,2
  HAVING auctions > 1000
), agg AS (
  SELECT host,
  MAX(CASE WHEN NOT last_hour THEN prog_fill_rate ELSE 0 END) previous_hours,
  MAX(CASE WHEN last_hour THEN prog_fill_rate ELSE 0 END) last_hour,
  MAX(CASE WHEN last_hour THEN prog_fill_rate ELSE 0 END) - MAX(CASE WHEN NOT last_hour THEN prog_fill_rate ELSE 0 END) prog_fill_rate_diff, SUM(auctions) auctions
  FROM base
  GROUP BY host
  HAVING auctions > 100000 AND last_hour > 0 AND previous_hours > 0
  ORDER BY prog_fill_rate_diff ASC
)
SELECT COUNT(1) count
FROM agg
WHERE prog_fill_rate_diff < -0.1
HAVING IF(count < 10, true, ERROR(CONCAT('Floors: Fill rate dropped by over 10% on 10 or more sites. ',
  (SELECT STRING_AGG(host LIMIT 10) FROM agg WHERE prog_fill_rate_diff < -0.1))))