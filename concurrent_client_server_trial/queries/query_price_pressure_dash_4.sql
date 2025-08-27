create temp table raw_data as

with brr as (
    select  coalesce(NET.REG_DOMAIN(page_url), '__unknown') AS domain,
        fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, max(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    --where 1751065200000 < server_time and server_time < 1751068800000
    where {START_UNIX_TIME_MS} < server_time and server_time < {END_UNIX_TIME_MS}
        and fs_auction_id is not null
        and status_message = 'Bid available'
    group by 1, 2, 3, 4, 5, 6, 7
),

brr_rn as (
    select *,
        row_number() over(partition by domain, fs_auction_id, placement_id, fs_refresh_count, ad_unit_code order by bid_cpm desc) bid_rank,
    from brr
),

all_auction_stats as (
    select *,
        safe_divide(bid_cpm,
            (
                select bid_cpm
                from brr_rn
                where fs_auction_id = o.fs_auction_id and placement_id = o.placement_id
                    and fs_refresh_count = o.fs_refresh_count and ad_unit_code = o.ad_unit_code
                    and domain = o.domain and bid_rank = 1
            )
        ) bid_price_pressure
    from brr_rn o
),

domain_totals as (
    select domain, APPROX_COUNT_DISTINCT(fs_auction_id || placement_id || fs_refresh_count || ad_unit_code) domain_total_auctions
    from all_auction_stats
    where bid_rank = 1
    group by 1
),

agg_auction_stats as (
    select
        domain,
        bid_rank,
        source,
        bidder,
        sum(bid_price_pressure) sum_bid_price_pressure,
        count(*) count_bid_made,
        countif(bid_rank = 1) count_highest_bid,
        countif(bid_price_pressure >= 0.8) count_bid_within_20_perc_of_highest_bid,
        countif(bid_price_pressure >= 0.5) count_bid_within_50_perc_of_highest_bid
    from all_auction_stats
    group by 1, 2, 3, 4
),

all_bidder_source_domain_combinations as (
    select bidder, source, domain, domain_total_auctions
    from domain_totals
    cross join (select distinct bidder, source from agg_auction_stats)
),

results as (
    select all_bidder_source_domain_combinations.*,
        bid_rank,
        coalesce(sum_bid_price_pressure, 0) / domain_total_auctions price_pressure,
        coalesce(count_bid_made, 0) / domain_total_auctions bid_made,
        coalesce(count_highest_bid, 0) / domain_total_auctions highest_bid,
        coalesce(count_bid_within_20_perc_of_highest_bid, 0) / domain_total_auctions bid_within_20_perc_of_highest_bid,
        coalesce(count_bid_within_50_perc_of_highest_bid, 0) / domain_total_auctions bid_within_50_perc_of_highest_bid
    from all_bidder_source_domain_combinations
    left join agg_auction_stats
    using (domain, bidder, source)
)

select *
from results;

{create_or_insert_statement_1}_second_highest_bid_dyanmics{create_or_insert_statement_2}
select
    CAST("{date}" as date) date,
    domain,
    avg(domain_total_auctions) domain_total_auctions,
    sum(price_pressure) price_pressure,
    sum(bid_made) bid_made,
    sum(bid_within_20_perc_of_highest_bid) bid_within_20_perc_of_highest_bid,
    sum(bid_within_50_perc_of_highest_bid) bid_within_50_perc_of_highest_bid
from raw_data
where bid_rank = 2
group by 1, 2;


{create_or_insert_statement_1}_highest_bid_source{create_or_insert_statement_2}
select
    CAST("{date}" as date) date,
    source,
    domain,
    avg(domain_total_auctions) domain_total_auctions,
    sum(bid_made) bid_made
from raw_data
where bid_rank = 1
group by 1, 2, 3;

{create_or_insert_statement_1}_bid_source_bidder{create_or_insert_statement_2}
select
    CAST("{date}" as date) date,
    bidder, source,
    domain,
    avg(domain_total_auctions) domain_total_auctions,
    sum(price_pressure) price_pressure,
    sum(bid_made) bid_made,
    sum(highest_bid) highest_bid,
    sum(bid_within_20_perc_of_highest_bid) bid_within_20_perc_of_highest_bid,
    sum(bid_within_50_perc_of_highest_bid) bid_within_50_perc_of_highest_bid
from raw_data
group by 1, 2, 3, 4;
