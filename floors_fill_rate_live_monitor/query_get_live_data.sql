select net.host(page_url) domain, placement_id,
TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), HOUR) AS date_hour,
  AVG(upr.floor_price) AS avg_floor_price,
  COUNT(1) AS auctions
FROM `freestar-157323.prod_eventstream.auction_end_raw` auc, auc.kvps kvpss
JOIN `sublime-elixir-273810.ds_experiments_us.upr_map2` upr
ON upr.upr_id = SPLIT(kvpss, 'floors_id=')[OFFSET(1)]
WHERE auc._PARTITIONDATE >= '2025-6-1'
AND TIMESTAMP_MILLIS(server_time) >= '2025-6-1'
AND kvpss LIKE "floors_id=%"

and lower(placement_id) in ('flightaware_live_airport_leaderboard_atf',
   'tagged_160x600_300x250_320x50_320x100_right', 'aljazeera_incontent_5', 'netronline_pubrecs_728x90_atf_desktop_header_1')
and net.host(page_url) in ('www.flightaware.com', 'www.aljazeera.com', 'www.tagged.com', 'publicrecords.netronline.com')
group by 1, 2, 3
order by 1, 2, 3