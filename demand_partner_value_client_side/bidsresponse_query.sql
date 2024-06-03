with winner as
(
    select auction_id, bidder_code bidder_winner, cpm cpm_winner
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where <START_UNIX_TIME_MS> < server_time and server_time < <END_UNIX_TIME_MS>
    and auction_id is not null and bidder_code is not null and cpm is not null
    qualify (row_number() over (partition by auction_id order by cpm desc) = 1)
), demand_partner as
(
    select auction_id, max(cpm) cpm_demand_partner
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    where <START_UNIX_TIME_MS> < server_time and server_time < <END_UNIX_TIME_MS>
    and auction_id is not null and bidder_code is not null and cpm is not null
    and bidder_code = "<DEMAND_PARTNER>"
    group by 1
), join_data as
(
    select a.*, ifnull(cpm_demand_partner, 0) cpm_demand_partner,
    ifnull(cpm_demand_partner, 0) / cpm_winner prop_of_winning_bid_demand_partner
    from winner a left join demand_partner b using (auction_id)
)
select <SELECT_COLS> from join_data
