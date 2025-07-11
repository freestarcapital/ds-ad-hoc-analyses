{create_or_insert_statement}

with aer as (
    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, max(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where {START_UNIX_TIME_MS} < server_time and server_time < {END_UNIX_TIME_MS}
        and fs_auction_id is not null
        and status_message = 'Bid available'
    group by 1, 2, 3, 4, 5, 6
),

aer_rn as (
    select *,
        row_number() over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code order by bid_cpm desc) bid_rank,
    from aer
),

combined as (
    select aer_rn.*, aer_rn_bid_rank_1.bid_cpm winning_bid_cpm,
        safe_divide(aer_rn.bid_cpm, aer_rn_bid_rank_1.bid_cpm) bid_price_pressure
    from aer_rn
    join (select * from aer_rn where bid_rank=1) aer_rn_bid_rank_1
    using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code)

)

select CAST("{date}" as date) date, bidder, source, bid_rank, count(*) auctions, avg(bid_price_pressure) bid_price_pressure
from combined
group by 1, 2, 3, 4

-- --For Looker, plot by bid_rank=2
-- --
--
-- with bid_rank_1 as (
--   select date, sum(auctions) auctions_bid_rank_1
--   from `streamamp-qa-239417.DAS_increment.concurrent_test1`
--   where bid_rank = 1
--   group by 1
-- )
--
-- select date, bid_rank,
--   sum(bid_price_pressure * auctions) / sum(auctions) bid_price_pressure_when_bid_present,
--   sum(auctions) / (select auctions_bid_rank_1 from bid_rank_1 where date=t1.date) bid_proportion,
--   sum(bid_price_pressure * auctions) / (select auctions_bid_rank_1 from bid_rank_1 where date=t1.date) bid_price_pressure
--
-- from `streamamp-qa-239417.DAS_increment.concurrent_test1` t1
-- where bid_rank = 2
-- group by 1, 2
-- order by 1, 2