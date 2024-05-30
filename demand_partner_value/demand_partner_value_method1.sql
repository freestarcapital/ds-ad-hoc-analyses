
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
  having count(*) = 1
), demand_partner as
(
  select auction_id, max(ifnull(raw_cpm, 0)) as raw_cpm_demand_partner, max(top_bid) as top_bid_demand_partner
  from `freestar-prod.prebid_server_raw.auction`
  where start_timestamp < time and time <= end_timestamp
  and (geo_country is not null) and (device is not null) and (host is not null)
  and (auction_id is not null) and (bidder_id is not null)
  and bidder_id = "<DEMAND_PARTNER>"
  group by 1
  having count(*) = 1
), join_data as
(
  select *, b.auction_id is not null demand_partner_included, ifnull(raw_cpm_demand_partner, 0) > 0 demand_partner_bid,
  from winner a left join demand_partner b using (auction_id)
), agg_data as
(
  select geo_country, device, host,
    count(*) count,
    countif(demand_partner_bid) count_demand_partner_bid,
    countif(not demand_partner_bid) count_demand_partner_not_bid,
    countif(demand_partner_included) count_demand_partner_included,
    countif(not demand_partner_included) count_demand_partner_not_included,
    safe_divide(sum(if(demand_partner_included, raw_cpm_winner, 0)), countif(demand_partner_included)) as avg_raw_cpm_winner_demand_partner_included,
    safe_divide(sum(if(not demand_partner_included, raw_cpm_winner, 0)), countif(not demand_partner_included)) as avg_raw_cpm_winner_demand_partner_not_included,
    safe_divide(sum(if(demand_partner_bid, raw_cpm_winner, 0)), countif(demand_partner_bid)) as avg_raw_cpm_winner_demand_partner_bid,
    safe_divide(sum(if(not demand_partner_bid, raw_cpm_winner, 0)), countif(not demand_partner_bid)) as avg_raw_cpm_winner_demand_partner_not_bid,
    safe_divide(sum(if(demand_partner_included, raw_cpm_demand_partner, 0)), countif(demand_partner_included)) as avg_raw_cpm_demand_partner_demand_partner_included,
    safe_divide(sum(if(demand_partner_bid, raw_cpm_demand_partner, 0)), countif(demand_partner_bid)) as avg_raw_cpm_demand_partner_demand_partner_bid,
    safe_divide(sum(if(top_bid_demand_partner, raw_cpm_demand_partner, 0)), countif(top_bid_demand_partner)) as avg_raw_cpm_demand_partner_demand_partner_won
  from join_data
  group by 1, 2, 3
)
select
  avg(avg_raw_cpm_winner_demand_partner_included - avg_raw_cpm_winner_demand_partner_not_included) as avg_raw_cpm_winner_demand_partner_included_uplift,
  avg(avg_raw_cpm_winner_demand_partner_included) avg_raw_cpm_winner_demand_partner_included,
  avg(avg_raw_cpm_winner_demand_partner_not_included) avg_raw_cpm_winner_demand_partner_not_included,
  avg(avg_raw_cpm_winner_demand_partner_bid - avg_raw_cpm_winner_demand_partner_not_bid) as avg_raw_cpm_winner_demand_partner_bid_uplift,
  avg(avg_raw_cpm_winner_demand_partner_bid) avg_raw_cpm_winner_demand_partner_bid,
  avg(avg_raw_cpm_winner_demand_partner_not_bid) avg_raw_cpm_winner_demand_partner_not_bid,
  avg(avg_raw_cpm_demand_partner_demand_partner_included) avg_raw_cpm_demand_partner_demand_partner_included,
  avg(avg_raw_cpm_demand_partner_demand_partner_bid) avg_raw_cpm_demand_partner_demand_partner_bid,
  avg(avg_raw_cpm_demand_partner_demand_partner_won) avg_raw_cpm_demand_partner_demand_partner_won
from agg_data
where count_demand_partner_included >= 100 and count_demand_partner_not_included >= 100