{create_or_insert_statement}

with brr as (
    select
        NET.REG_DOMAIN(page_url) AS domain,
        fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, bidder, source, max(bid_cpm) bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where 1751065200000 < server_time and server_time < 1751068800000
        and fs_auction_id is not null
        and status_message = 'Bid available'
    group by 1, 2, 3, 4, 5, 6, 7
),

brr_rn as (
    select *,
        row_number() over(partition by fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, domain order by bid_cpm desc) bid_rank,
    from brr
),

total_auctions_table as (
    select domain, APPROX_COUNT_DISTINCT(fs_auction_id || placement_id || fs_refresh_count || ad_unit_code) total_auctions
    from brr_rn
    where bid_rank = 1
    group by 1
),

results as (
    select br_1.domain domain,
        br_1.bidder bidder_1, br_1.source source_1,
        br_2.bidder bidder_2, br_2.source source_2,
        avg(total_auctions) total_auctions_in_cohort,
        count(br_1.bid_cpm) total_bids_1,
        count(br_2.bid_cpm) total_bids_2,
        avg(safe_divide(coalesce(br_1.bid_cpm, 0), br_1.bid_cpm)) bid_price_pressure_1,
        countif(coalesce(br_2.bid_cpm, 0) >= 0.8 * br_1.bid_cpm) bid_price_pressure_2_within_20_perc,
        countif(coalesce(br_2.bid_cpm, 0) >= 0.5 * br_1.bid_cpm) bid_price_pressure_2_within_50_perc--,

    from total_auctions_table
    join (select * from brr_rn where bid_rank=1) br_1
        using (domain)
    left join (select * from brr_rn where bid_rank=2) br_2
        using (fs_auction_id, placement_id, fs_refresh_count, ad_unit_code, domain)

    group by 1, 2, 3, 4, 5
)

select CAST("{date}" as date) date, *
from results