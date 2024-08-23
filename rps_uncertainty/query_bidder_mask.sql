
with bidder_mask_values as (
  select *,
    if(bidder in ('ix', 'pubmatic', 'sharethrough', 'triplelift', 'rubicon', 'appnexus', 'gumgum', 'yieldmo'), '2',
      if(bidder in ('openx', 'rise', 'undertone', 'medianet', 'yahoossp'), '3', '.')) mask_value
  from `freestar-157323.ad_manager_dtf.lookup_bidders`
), bidder_mask_table as (
  select array_to_string(array(select mask_value from bidder_mask_values order by position), '') bidder_mask
)
select *
from `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_domain` t1
join bidder_mask_table t2 on REGEXP_CONTAINS(t1.fs_clientservermask, t2.bidder_mask)
WHERE '2024-06-01' <= date
        and fs_testgroup = 'experiment'
        and country_code is not null
        and domain is not null
        and `freestar-157323.ad_manager_dtf`.device_category(device_category) is not null


-- SELECT array_length(REGEXP_EXTRACT_ALL('12345678123123', '1')) AS example
