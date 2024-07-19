
DECLARE processing_date DATE DEFAULT "{processing_date}";

CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.DAS_bidder_sessions_{processing_date}` AS

    SELECT bidder, country_code, rtt_category rtt_category_raw,
    `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
    status,
    sum(session_count) session_count, sum(revenue) revenue, sum(impressions) impressions, sum(unfilled) unfilled

    FROM freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_v1
    WHERE DATE>= DATE_SUB(processing_date, INTERVAL {day_interval} DAY) AND DATE<= DATE_SUB(processing_date, INTERVAL 1 DAY)
    and fs_testgroup = 'optimised'
    and country_code is not null
    and status != 'disabled'
    and rtt_category is not null
    group by 1, 2, 3, 4, 5;

CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.DAS_bidder_summary_{processing_date}` AS
select DATE_SUB(processing_date, INTERVAL 1 DAY) date,
    bidder, device_category, status,
    sum(session_count) session_count, sum(revenue) revenue, sum(impressions) impressions,
    sum(unfilled) unfilled, sum(impressions+unfilled) ad_requests
from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}`
join `sublime-elixir-273810.ds_experiments_us.DAS_bidder_sessions_{processing_date}`
using (bidder, device_category, country_code, rtt_category_raw, status)
group by 1, 2, 3, 4;

select date, bidder, device_category, status,
    ad_requests / sum(ad_requests) over(partition by date, bidder, device_category) ad_req
from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_summary_{processing_date}`