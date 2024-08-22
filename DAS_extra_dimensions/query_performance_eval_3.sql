with base as (
    select date, domain, country_code, fs_testgroup,
        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
        sum(session_count) session_count,
        sum(revenue) revenue
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE '2024-06-10' <= date AND date <= '2024-08-18'
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'
        and ad_product is not null
        and domain is not null
        and fs_testgroup in ('experiment', 'optimised')
        and `freestar-157323.ad_manager_dtf`.device_category(device_category) is not null
    group by 1, 2, 3, 4, 5
), is_change_cohort as (
   select b.*, --t1.domain is not null j1, t2.domain is not null j2, t3.domain is not null j3, t4.domain is not null j4,
    coalesce(t1.domain, t2.domain, t3.domain, t4.domain) is not null change_cohort
   from base b
   left join (select distinct domain, country_code, device_category from `sublime-elixir-273810.ideal_ad_stack.domain_optimisations_manual` where country_code!='default' and device_category!='default') t1
   on b.domain=t1.domain and b.country_code=t1.country_code and b.device_category=t1.device_category
   left join (select distinct domain, device_category from `sublime-elixir-273810.ideal_ad_stack.domain_optimisations_manual` where country_code='default' and device_category!='default') t2
   on b.domain=t2.domain and b.device_category=t2.device_category
   left join (select distinct domain, country_code from `sublime-elixir-273810.ideal_ad_stack.domain_optimisations_manual` where country_code!='default' and device_category='default') t3
   on b.domain=t3.domain and b.country_code=t3.country_code
   left join (select distinct domain from `sublime-elixir-273810.ideal_ad_stack.domain_optimisations_manual` where country_code='default' and device_category='default') t4
   on b.domain=t4.domain
), results as (
   select date,
      sum(if(fs_testgroup='experiment', revenue, 0)) revenue_control,
      sum(if(fs_testgroup='experiment', session_count, 0)) session_count_control,
      sum(if(fs_testgroup='optimised', revenue, 0)) revenue_change,
      sum(if(fs_testgroup='optimised', session_count, 0)) session_count_change
    from is_change_cohort
    where change_cohort
    group by 1
)
select date, 
    --sum(revenue_control) over(order by date rows between {N_days} preceding and current row) revenue_rolling_control,
    safe_divide(sum(revenue_control) over(order by date rows between {N_days} preceding and current row),
        sum(session_count_control) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_control,
    --sum(revenue_change) over(order by date rows between {N_days} preceding and current row) revenue_rolling_change,
    safe_divide(sum(revenue_change) over(order by date rows between {N_days} preceding and current row),
        sum(session_count_change) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_change,
    from results
