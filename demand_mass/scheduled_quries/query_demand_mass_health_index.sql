DECLARE ddate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- create or replace table `sublime-elixir-273810.demand_mass.health_index`
-- PARTITION BY date
-- OPTIONS (
-- partition_expiration_days = 365,
-- require_partition_filter = TRUE) AS

BEGIN TRANSACTION;
DELETE FROM `sublime-elixir-273810.demand_mass.health_index`
WHERE date = ddate; --OR date = DATE_SUB(ddate, INTERVAL 1 DAY);
INSERT INTO `sublime-elixir-273810.demand_mass.health_index`
(

with base as(
select * from `sublime-elixir-273810.demand_mass.demand_mass_base_data` where date=ddate
),

without_domain as (
    select date,device_category,country_code,  demand_partner, impressions,
    sum(impressions) over(partition by date,device_category,country_code) impressions_all,
    requests ,sum(requests) over(partition by date,device_category,country_code) requests_all
    from (
        select date,device_category,country_code, demand_partner,sum(impressions) impressions, sum(impressions+unfilled) requests
	    from base
        where country_code is not null
	    group by 1,2,3,4
        )
        ),

intermediate as (select date,device_category,country_code, demand_partner,
safe_divide(requests,requests_all)*100 participation_rate,requests_all
from without_domain
where impressions_all>1000
order by device_category,country_code, participation_rate desc),

with_domain as (
    select date,domain,device_category,country_code,   demand_partner, impressions,
    sum(impressions) over(partition by date,domain,device_category,country_code) impressions_all,
    requests ,sum(requests) over(partition by date,domain,device_category,country_code) requests_all
    from (
        select date,domain,device_category,country_code,  demand_partner,sum(impressions) impressions, sum(impressions+unfilled) requests
	    from base
        where country_code is not null
	    group by 1,2,3,4,5
        )
        ),

intermediate_2 as (select date,domain,device_category,country_code,  demand_partner,
safe_divide(requests,requests_all)*100 participation_rate,requests_all
from with_domain
order by date,domain,device_category,country_code,  participation_rate desc),


final as (select a.* except(participation_rate),
round(a.participation_rate,1) participation_rate,
round(sum(a.participation_rate) over(partition by a.date,a.domain,a.device_category,a.country_code order by a.participation_rate desc),1) cum_sum,
round(b.participation_rate,1) benchmark_participations,
b.requests_all benchmark_requests,
round(safe_divide((a.participation_rate-b.participation_rate),b.participation_rate)*100,1) percentage_change
from intermediate_2 a
join intermediate b
on a.date=b.date
and a.demand_partner=b.demand_partner
and a.device_category=b.device_category
and a.country_code=b.country_code
where a.requests_all>100
and domain is not null
and a.country_code='US'
),

health_check as (select *,
case when benchmark_participations>=3 and percentage_change>-50 then 'healthy'
when benchmark_participations<3 then 'unavailable'
else 'unhealthy' end health_flag
from final
),

health_counts as (select *,
countif(health_flag='healthy') over(partition by date,domain,country_code,device_category) healthy_cnt,
countif(health_flag='unhealthy') over(partition by date,domain,country_code,device_category) unhealthy_cnt,
 from health_check
 ),

health_indexes as (select *,
safe_divide(healthy_cnt,healthy_cnt+unhealthy_cnt)*100 health_index,
 from health_counts
)


select date,domain,sum(requests_all) requests_all,avg(health_index) health_index,
safe_divide(sum(health_index*requests_all),sum(requests_all)) health_index_weighted, onboarding
 from health_indexes
 left join `sublime-elixir-273810.ideal_ad_stack.domain_bidder_onboarding` using (domain)

 group by 1,2,onboarding

);
COMMIT TRANSACTION;



