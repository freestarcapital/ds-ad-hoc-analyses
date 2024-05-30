DECLARE start_timestamp TIMESTAMP DEFAULT TIMESTAMP("<START_TIMESTAMP_STR>");
DECLARE end_timestamp TIMESTAMP DEFAULT TIMESTAMP("<END_TIMESTAMP_STR>");

select distinct bidder_id
from `freestar-prod.prebid_server_raw.auction`
where start_timestamp < time and time <= end_timestamp
and (geo_country is not null) and (device is not null) and (host is not null)
and (auction_id is not null) and (bidder_id is not null) and (raw_cpm is not null)
order by 1