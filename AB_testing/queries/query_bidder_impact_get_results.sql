select ab_test_name, domain, date, bidder, test_group,
        bidder_participation_rate,
        bidder_win_rate,
        bidder_prebid_win_rate,
        count_of_bidder_responses,
        bidder_cpm_when_bids,
        bidder_cpm_when_wins,
        bidder_price_pressure_include_non_bids,
        bidder_price_pressure_bids,
        bidder_price_pressure_wins,
        bidder_within_20perc_include_non_bids,
        bidder_within_20perc_bids,
        bidder_within_50perc_include_non_bids,
        bidder_within_50perc_bids

from `{tablename}`
qualify (countif(sessions_day_domain_test_group >= 5000) over (partition by ab_test_name, date, domain, bidder, test_name_str) = 2) and (test_name_str != 'null')
order by ab_test_name, domain, date, bidder, test_group