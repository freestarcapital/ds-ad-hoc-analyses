
CREATE OR REPLACE TABLE `streamamp-qa-239417.floors_uplift_US.floors_uplift_domain_N_{N_countries}` AS

with aggregated_base_data_with_continent as (select * from `streamamp-qa-239417.floors_uplift_US.floors_uplift_base_data_1_day`),

qualifying_country_codes as (
    select date, country_code
    from aggregated_base_data_with_continent
    group by 1, 2
    qualify row_number() over (partition by date order by least(sum(if(control, ad_requests, 0)), sum(if(control, 0, ad_requests))) desc) <= {N_countries}
),

aggregated_base_data_country_continent as (
    select * except (country_code, geo_country, geo_continent), coalesce(qualifying_country_codes.country_code, 'continent_' || geo_continent) country_continent
    from aggregated_base_data_with_continent
    left join qualifying_country_codes using (date, country_code)
),

cpma_country_continent as (
    select date, country_continent,
        safe_divide(sum(if(control, 0, rev)), sum(if(control, 0, ad_requests))) * 1000 cpma_optimised,
        safe_divide(sum(if(control, rev, 0)), sum(if(control, ad_requests, 0))) * 1000 cpma_control,
        sum(if(control, 0, ad_requests)) ad_requests_optimised,
        sum(if(control, ad_requests, 0)) ad_requests_control
    from aggregated_base_data_country_continent
    where country_continent not like 'continent_%'
    group by 1, 2

    union all

    select date, 'continent_' || geo_continent country_continent,
        safe_divide(sum(if(control, 0, rev)), sum(if(control, 0, ad_requests))) * 1000 cpma_optimised,
        safe_divide(sum(if(control, rev, 0)), sum(if(control, ad_requests, 0))) * 1000 cpma_control,
        sum(if(control, 0, ad_requests)) ad_requests_optimised,
        sum(if(control, ad_requests, 0)) ad_requests_control
    from aggregated_base_data_with_continent
    group by 1, 2
)

select date, domain,
    sum(ad_requests * (cpma_optimised-cpma_control) / 1000) rev_uplift,
    sum(rev) revenue,
    sum(ad_requests) ad_requests,
    safe_divide(sum(cpma_optimised * rev), sum(rev)) cpma_optimised,
    safe_divide(sum(cpma_control * rev), sum(rev)) cpma_control
from aggregated_base_data_country_continent
join cpma_country_continent using (country_continent, date)
where not control
group by 1, 2

