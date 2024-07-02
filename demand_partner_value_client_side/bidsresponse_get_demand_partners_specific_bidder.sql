
CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.bidder_inv_auction_ids_<SPECIFIC_BIDDER>_<START_UNIX_TIME_MS>_<END_UNIX_TIME_MS>` AS
select distinct auction_id
from `freestar-157323.prod_eventstream.bidsresponse_raw`
where <START_UNIX_TIME_MS> < server_time and server_time < <END_UNIX_TIME_MS>
    and auction_id is not null and bidder_code is not null and cpm is not null and source = 'client'
    and site_id in (
        select distinct site_id
        from `freestar-157323.prod_eventstream.bidsresponse_raw`
        where <START_UNIX_TIME_MS> < server_time and server_time < <END_UNIX_TIME_MS>
            and bidder_code = '<SPECIFIC_BIDDER>'
            and auction_id is not null and cpm is not null and source = 'client'
);

select distinct bidder_code
from `freestar-157323.prod_eventstream.bidsresponse_raw`
join `sublime-elixir-273810.ds_experiments_us.bidder_inv_auction_ids_<SPECIFIC_BIDDER>_<START_UNIX_TIME_MS>_<END_UNIX_TIME_MS>` using (auction_id)
where <START_UNIX_TIME_MS> < server_time and server_time < <END_UNIX_TIME_MS>
and auction_id is not null and bidder_code is not null and cpm is not null
and source = 'client'
order by 1
