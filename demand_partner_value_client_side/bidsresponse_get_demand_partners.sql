select distinct bidder_code
from `freestar-157323.prod_eventstream.bidsresponse_raw`
where <START_UNIX_TIME_MS> < server_time and server_time < <END_UNIX_TIME_MS>
and auction_id is not null and bidder_code is not null and cpm is not null
order by 1
