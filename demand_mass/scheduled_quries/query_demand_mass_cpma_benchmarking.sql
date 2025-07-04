DECLARE ddate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- create or replace table `sublime-elixir-273810.demand_mass.cpma_benchmarking`
-- PARTITION BY date
-- OPTIONS (
-- partition_expiration_days = 365,
-- require_partition_filter = TRUE) AS

BEGIN TRANSACTION;
DELETE FROM `sublime-elixir-273810.demand_mass.cpma_benchmarking`
WHERE date = ddate; --OR date = DATE_SUB(ddate, INTERVAL 1 DAY);
INSERT INTO `sublime-elixir-273810.demand_mass.cpma_benchmarking`
(

with with_cpma as (
   select
   date,domain,
   country_code,
   device_category, revenue,impressions,requests,
   sum(revenue) over(partition by date,device_category,country_code) revenue_geo_device,
   sum(impressions) over(partition by date,device_category,country_code) impressions_geo_device,
   sum(requests) over(partition by date,device_category,country_code) requests_geo_device,
   sum(requests) over() requests_all
   from (
      select date,domain,device_category,country_code,
      sum(gross_revenue) revenue,sum(impressions) impressions, sum(impressions+unfilled) requests
      from `sublime-elixir-273810.demand_mass.demand_mass_base_data` base
      where date=ddate and country_code is not null
      group by 1,2,3,4)
      where domain is not null and country_code is not null
   ),

intermediate as(
   select date,domain,country_code,device_category,
   requests,
   requests_geo_device, requests_all,
   safe_divide(requests,requests_geo_device) participation_rate,
   safe_divide(requests_geo_device,requests_all) participation_rate_benchmark,
   safe_divide(revenue,requests)*1000 cpma,
   safe_divide(revenue_geo_device,requests_geo_device)*1000 cpma_benchmark,
   from with_cpma
   where requests>1000

),


cpmas_cc_dc_domain as (
   select date, domain,country_code,device_category,
   avg(cpma) cpma_cc_dc_domain, sum(requests) requests
   from intermediate
   group by 1, 2, 3, 4
),
weighted_cpmas_cc_dc as (select date,country_code,device_category,
avg(cpma) cpma_cc_dc_benchmark,
SAFE_DIVIDE(SUM(cpma * participation_rate), SUM(participation_rate)) AS weighted_cpma_benchmark
from intermediate
group by 1, 2, 3
)

select a.date,a.country_code,a.device_category,a.domain, cpma_cc_dc_domain,cpma_cc_dc_benchmark,
weighted_cpma_benchmark, requests,onboarding
from cpmas_cc_dc_domain a
inner join weighted_cpmas_cc_dc b using (date,country_code,device_category)
left join `sublime-elixir-273810.ideal_ad_stack.domain_bidder_onboarding` using (domain)

);
COMMIT TRANSACTION;
