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
  having count(*) = 1 and max(raw_cpm) > 0
), demand_partner as
(
  select auction_id, max(ifnull(raw_cpm, 0)) as raw_cpm_demand_partner, max(ifnull(top_bid, False)) as top_bid_demand_partner
  from `freestar-prod.prebid_server_raw.auction`
  where start_timestamp < time and time <= end_timestamp
  and (geo_country is not null) and (device is not null) and (host is not null)
  and (auction_id is not null) and (bidder_id is not null)
  and bidder_id = "<DEMAND_PARTNER>"
  group by 1
  having count(*) = 1
), join_data as
(
  select a.*, ifnull(raw_cpm_demand_partner, 0) raw_cpm_demand_partner,
  ifnull(raw_cpm_demand_partner, 0) / raw_cpm_winner prop_of_winning_bid_demand_partner,
    ifnull(top_bid_demand_partner, False) top_bid_demand_partner,
  b.auction_id is not null demand_partner_included, ifnull(raw_cpm_demand_partner, 0) > 0 demand_partner_bid,
  from winner a left join demand_partner b using (auction_id)
)
select <SELECT_COLS> from join_data
where demand_partner_included
