

--     select  * --bidder_code, bidder, cpm, source, auction_id, fs_auction_id, placement_id
--         from `freestar-157323.prod_eventstream.bidsresponse_raw`
--         where 1751443200000 < server_time and server_time < 1751446800000
--         and fs_auction_id is not null
--         --and not is_native_render
-- order by fs_auction_id
-- limit 100

with bwr as (

    select * --bidder_code, bidder, cpm, source, auction_id, fs_auction_id, placement_id
        from `freestar-157323.prod_eventstream.bidswon_raw`
        where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and not is_native_render
),

t1 as (
    select fs_auction_id,placement_id, fs_refresh_count, ad_unit_code, count(*) cnt
    from bwr
    group by 1, 2, 3, 4
),

t2 as (
    select cnt, count(*) cnt2
    from t1
    group by 1
)

select cnt, cnt2 / sum(cnt2) over()
from t2
order by 1
--  from `freestar-157323.prod_eventstream._raw`

--  where 1751443200000 < server_time and server_time < 1751446800000



-- order by fs_auction_id, placement_id, cpm desc
-- limit 100


with aer as (
    select  fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, avg(bid_cpm)* 1000 bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and status_message = 'Bid available'
        --and not is_native_render
    group by 1, 2, 3, 4, 5, 6
),

bwr as (

    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder winning_bidder, source winning_source
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and not is_native_render
        and status_message = 'Bid available'
    qualify count(*) over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code) = 1
)

select *, source || '_' || bidder source_bidder
from aer
join bwr using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code)
order by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code
limit 1000




with aer as (
    select  fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, avg(bid_cpm)* 1000 bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and status_message = 'Bid available'
        --and not is_native_render
    group by 1, 2, 3, 4, 5, 6
),

bwr as (

    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder winning_bidder, source winning_source, cpm winning_cpm
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and not is_native_render
        and status_message = 'Bid available'
    qualify count(*) over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code) = 1
),

combined as (
    select *, source || '_' || bidder source_bidder
    from aer
    join bwr using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code)
),

total_auction_count as (
    select count(*) total_auctions from (
        select 1
        from combined
        group by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code
    )
)

select source_bidder, count(*) cnt, avg(safe_divide(bid_cpm, winning_cpm)) bid_pressure_when_participating,
    countif(bidder=winning_bidder) winning_bid_rate_when_participating, avg(total_auctions) total_auctions

from combined
join total_auction_count on True
group by source_bidder
order by 4 desc
--order by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code

limit 1000




with aer as (
    select  fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, avg(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and status_message = 'Bid available'
        --and not is_native_render
    group by 1, 2, 3, 4, 5, 6
),

bwr as (

    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder winning_bidder, source winning_source, cpm / 10000 winning_cpm
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and not is_native_render
        and status_message = 'Bid available'
    qualify count(*) over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code) = 1
),

combined as (
    select *, --source || '_' || bidder source_bidder
        row_number() over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code order by bid_cpm desc) bid_rank,

    from aer
    join bwr using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code)
),

t1 as (
select bid_rank,
        1-avg(safe_divide(bid_cpm, winning_cpm)) winning_price_pressure_when_bid_made,
        count(*) total_bids_made
    from combined
    group by 1
)

select *, total_bids_made / (select total_bids_made from t1 where bid_rank=1) bid_proportion,
    winning_price_pressure_when_bid_made * total_bids_made / (select total_bids_made from t1 where bid_rank=1) winning_price_pressure
from t1
order by 1





with aer as (
    select  fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, avg(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and status_message = 'Bid available'
        --and not is_native_render
    group by 1, 2, 3, 4, 5, 6
),

bwr as (

    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder winning_bidder, source winning_source, cpm / 10000 winning_cpm
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where 1751443200000 < server_time and server_time < 1751446800000
        and fs_auction_id is not null
        and not is_native_render
        and status_message = 'Bid available'
    qualify count(*) over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code) = 1
),

combined as (
    select *, --source || '_' || bidder source_bidder
        row_number() over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code order by bid_cpm desc) bid_rank,

    from aer
    join bwr using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code)
),

t1 as (
select bid_rank,
        1-avg(safe_divide(bid_cpm, winning_cpm)) winning_price_pressure_when_bid_made,
        count(*) total_bids_made
    from combined
    group by 1
),

t2 as (
    select total_bids_made from t1 where bid_rank=1
)

select *, t1.total_bids_made / t2.total_bids_made bid_proportion,
    winning_price_pressure_when_bid_made * t1.total_bids_made / t2.total_bids_made winning_price_pressure
from t1
join t2 on True
order by 1



