create temp table raw_data as

with t1 as (
    select fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, max(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    --where 1751065200000 < server_time and server_time < 1751068800000
    where {START_UNIX_TIME_MS} < server_time and server_time < {END_UNIX_TIME_MS}
        and fs_auction_id is not null
        and status_message = 'Bid available'
    group by 1, 2, 3, 4, 5, 6
),

t2 as (
    select *,
        row_number() over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code order by bid_cpm desc) bid_rank,
    from t1
),

t3 as (
    select *,
        safe_divide(bid_cpm,
            (
                select bid_cpm
                from t2
                where fs_auction_id = o.fs_auction_id and placement_id = o.placement_id
                    and fs_refresh_count = o.fs_refresh_count and ad_unit_code = o.ad_unit_code
                    and bid_rank = 1
            )
        ) bid_price_pressure
    from t2 o
),

totals as (
  select APPROX_COUNT_DISTINCT(fs_auction_id || placement_id || fs_refresh_count || ad_unit_code) total
  from t3
  where bid_rank = 1
),

results as (
    select
        bid_rank,
        source,
        bidder,
        sum(bid_price_pressure) / (select total from totals) price_pressure,
        count(*) / (select total from totals) bid_made,
        countif(bid_rank = 1) / (select total from totals) highest_bid,
        countif(bid_price_pressure >= 0.8) / (select total from totals) bid_within_20_perc_of_highest_bid,
        countif(bid_price_pressure >= 0.5) / (select total from totals) bid_within_50_perc_of_highest_bid
    from t3
    group by 1, 2, 3
)

select *
from results;


{create_or_insert_statement_1}_second_highest_bid_dyanmics{create_or_insert_statement_2}
select
    CAST("{date}" as date) date,
    sum(price_pressure) price_pressure,
    sum(bid_made) bid_made,
    sum(bid_within_20_perc_of_highest_bid) bid_within_20_perc_of_highest_bid,
    sum(bid_within_50_perc_of_highest_bid) bid_within_50_perc_of_highest_bid
from raw_data
where bid_rank = 2
group by 1;


{create_or_insert_statement_1}_highest_bid_source{create_or_insert_statement_2}
select
    CAST("{date}" as date) date,
    source,
    sum(bid_made) bid_made
from raw_data
where bid_rank = 1
group by 1, 2;

{create_or_insert_statement_1}_bid_source_bidder{create_or_insert_statement_2}
select
    CAST("{date}" as date) date,
    bidder, source,
    sum(price_pressure) price_pressure,
    sum(bid_made) bid_made,
    sum(highest_bid) highest_bid,
    sum(bid_within_20_perc_of_highest_bid) bid_within_20_perc_of_highest_bid,
    sum(bid_within_50_perc_of_highest_bid) bid_within_50_perc_of_highest_bid
from raw_data
group by 1, 2, 3;