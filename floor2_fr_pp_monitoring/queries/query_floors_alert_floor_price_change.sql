DECLARE test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

WITH base AS (
  SELECT REGEXP_REPLACE(net.host(page_url), 'undefined$', '') host,
  TIMESTAMP_MILLIS(server_time) > TIMESTAMP_SUB(test_time, INTERVAL 1 HOUR) last_hour,
  AVG(upr.floor_price) avg_floor_price,
  count(1) auctions
  FROM `freestar-157323.prod_eventstream.auction_end_raw` auc, auc.kvps kvpss
  JOIN `sublime-elixir-273810.ds_experiments_us.upr_map2` upr
  ON upr.upr_id = split(kvpss, 'floors_id=')[OFFSET(1)]
  WHERE auc._PARTITIONDATE >= DATE_SUB(DATE(test_time), INTERVAL 1 DAY)
  AND auc._PARTITIONDATE <= DATE(test_time)
  AND TIMESTAMP_MILLIS(server_time) > TIMESTAMP_SUB(test_time, INTERVAL 4 HOUR)
  AND kvpss LIKE "floors_id=%"
  AND net.host(page_url) IS NOT NULL
  GROUP BY 1,2
  HAVING auctions > 1000
), agg AS (
  SELECT host,
  MAX(CASE WHEN NOT last_hour THEN avg_floor_price ELSE 0 END) previous_hours,
  MAX(CASE WHEN last_hour THEN avg_floor_price ELSE 0 END) last_hour,
  SAFE_DIVIDE(MAX(CASE WHEN last_hour THEN avg_floor_price ELSE 0 END) - MAX(CASE WHEN NOT last_hour THEN avg_floor_price ELSE 0 END), MAX(CASE WHEN NOT last_hour THEN avg_floor_price ELSE 0 END)) avg_floor_price_diff_perc, SUM(auctions) auctions
  FROM base
  GROUP BY host
  HAVING auctions > 100000 AND last_hour > 0 AND previous_hours > 0
  ORDER BY avg_floor_price_diff_perc DESC
)
SELECT COUNT(1) count
FROM agg
WHERE avg_floor_price_diff_perc > 0.9
HAVING IF(count < 5, true, ERROR(CONCAT('Floors: At least five sites have had average floor price increase by over 90%. ',
  (SELECT STRING_AGG(host LIMIT 10) FROM agg WHERE avg_floor_price_diff_perc > 0.9))))