
with aggregated_base_data_with_continent as (select * from `streamamp-qa-239417.floors_uplift_US.floors_uplift_base_data_25_days`),

qualifying_country_codes as (
    select date, country_code
    from aggregated_base_data_with_continent
    group by 1, 2
    qualify row_number() over (partition by date order by least(sum(if(control, ad_requests, 0)), sum(if(control, 0, ad_requests))) desc) <= 30
),

aggregated_base_data_country_continent as (
    select * except (geo_country, geo_continent), coalesce(qualifying_country_codes.country_code, 'continent_' || geo_continent) country_continent
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

select *, cpma_optimised-cpma_control cpma_uplift from cpma_country_continent
