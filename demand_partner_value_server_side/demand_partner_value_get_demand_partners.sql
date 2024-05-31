DECLARE start_timestamp TIMESTAMP DEFAULT TIMESTAMP("<START_TIMESTAMP_STR>");
DECLARE end_timestamp TIMESTAMP DEFAULT TIMESTAMP("<END_TIMESTAMP_STR>");

with winner as
(
  select auction_id, geo_country, device, host, max(raw_cpm) as raw_cpm_winner
  from `freestar-prod.prebid_server_raw.auction`
  where start_timestamp < time and time <= end_timestamp
  and (geo_country is not null) and (device is not null) and (host is not null)
  and (auction_id is not null) and (bidder_id is not null) and (raw_cpm is not null)
  and top_bid
  group by 1, 2, 3, 4
  having count(*) = 1 and max(raw_cpm) > 0
)
select distinct bidder_id
from `freestar-prod.prebid_server_raw.auction`
join winner using (auction_id, geo_country, device, host)
where start_timestamp < time and time <= end_timestamp
and (geo_country is not null) and (device is not null) and (host is not null)
and (auction_id is not null) and (bidder_id is not null) and (raw_cpm is not null)
order by 1