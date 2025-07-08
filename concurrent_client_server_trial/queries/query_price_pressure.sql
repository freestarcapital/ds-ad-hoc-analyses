
with aer as (
    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, max(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where {START_UNIX_TIME_MS} < server_time and server_time < {END_UNIX_TIME_MS}
        and fs_auction_id is not null
        and status_message = 'Bid available'
    group by 1, 2, 3, 4, 5, 6
),

bwr as (
    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder winning_bidder, source winning_source, cpm / 10000 winning_cpm
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where {START_UNIX_TIME_MS} < server_time and server_time < {END_UNIX_TIME_MS}
        and fs_auction_id is not null
        and not is_native_render
        and status_message = 'Bid available'
    qualify count(*) over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code) = 1
),

combined as (
    select *, row_number() over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code order by bid_cpm desc) bid_rank,

    from aer
    join bwr using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code)
),

bid_stats as (
    select bid_rank,
        avg(safe_divide(bid_cpm, winning_cpm)) price_pressure_when_bid_made_avg,
        approx_quantiles(least(1.0, safe_divide(bid_cpm, winning_cpm)), 100) price_pressure_when_bid_made_quantiles,
        count(*) total_bids_made
    from combined
    where bid_rank <= 5
    group by 1
),

total_bids as (
    select total_bids_made total_bids_made_bid_rank_1
    from bid_stats where bid_rank=1
)

select *, total_bids_made / total_bids_made_bid_rank_1 bid_participation
from bid_stats
join total_bids on True
order by 1
