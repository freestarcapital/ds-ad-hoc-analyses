with base as (
    select date, domain, country_code,
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
        and fs_testgroup = 'optimised'
        and `freestar-157323.ad_manager_dtf`.device_category(device_category) is not null
    group by 1, 2, 3, 4
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
      sum(if(change_cohort, 0, revenue)) revenue_no_change_cohorts,
      sum(if(change_cohort, 0, session_count)) session_count_no_change_cohorts,
      sum(if(change_cohort, revenue, 0)) revenue_change_cohorts,
      sum(if(change_cohort, session_count, 0)) session_count_change_cohorts
    from is_change_cohort
    group by 1
)
select date,
    --sum(revenue_no_change_cohorts) over(order by date rows between {N_days} preceding and current row) revenue_rolling_no_change_cohorts,
    safe_divide(sum(revenue_no_change_cohorts) over(order by date rows between {N_days} preceding and current row),
        sum(session_count_no_change_cohorts) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_no_change_cohorts,
    --sum(revenue_change_cohorts) over(order by date rows between {N_days} preceding and current row) revenue_rolling_change_cohorts,
    safe_divide(sum(revenue_change_cohorts) over(order by date rows between {N_days} preceding and current row),
        sum(session_count_change_cohorts) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_change_cohorts,
    from results

--with base as (
--    select date, domain, country_code,
--        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
--        sum(session_count) session_count,
--        sum(revenue) revenue
--    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
--    WHERE '2024-06-1' <= date AND date <= '2024-08-18'
--        and country_code is not null
--        and status != 'disabled'
--        and ad_product not like '%video%'
--        and ad_product is not null
--        and domain is not null
--        and fs_testgroup = 'optimised'
--        and `freestar-157323.ad_manager_dtf`.device_category(device_category) is not null
--    group by 1, 2, 3, 4
--), results_domain as (
--   select date,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, 0, t1.revenue)) revenue_no_change_cohorts_same_domain,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, 0, t1.session_count)) session_count_no_change_cohorts_same_domain,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, t1.revenue, 0)) revenue_change_cohorts_same_domain,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, t1.session_count, 0)) session_count_change_cohorts_same_domain
--    from base t1
--    join `sublime-elixir-273810.ideal_ad_stack.domain_optimisations_manual` t2
--    on t1.domain = t2.domain
--    group by 1
--), results_cc_dc as (
--   select date,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, 0, t1.revenue)) revenue_no_change_cohorts_same_cc_dc,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, 0, t1.session_count)) session_count_no_change_cohorts_same_cc_dc,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, t1.revenue, 0)) revenue_change_cohorts_same_cc_dc,
--      sum(if(t1.domain = t2.domain and t1.country_code = t2.country_code and t1.device_category = t2.device_category, t1.session_count, 0)) session_count_change_cohorts_same_cc_dc
--    from base t1
--    join `sublime-elixir-273810.ideal_ad_stack.domain_optimisations_manual` t2
--    on t1.country_code = t2.country_code and t1.device_category = t2.device_category
--    group by 1
--)
--select date,
--    --sum(revenue_no_change_cohorts_same_domain) over(order by date rows between {N_days} preceding and current row) revenue_rolling_no_change_cohorts_same_domain,
--    safe_divide(sum(revenue_no_change_cohorts_same_domain) over(order by date rows between {N_days} preceding and current row),
--        sum(session_count_no_change_cohorts_same_domain) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_no_change_cohorts_same_domain,
--    --sum(revenue_change_cohorts_same_domain) over(order by date rows between {N_days} preceding and current row) revenue_rolling_change_cohorts_same_domain,
--    --safe_divide(sum(revenue_change_cohorts_same_domain) over(order by date rows between {N_days} preceding and current row),
--      --  sum(session_count_change_cohorts_same_domain) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_change_cohorts_same_domain,
--    --sum(revenue_no_change_cohorts_same_cc_dc) over(order by date rows between {N_days} preceding and current row) revenue_rolling_no_change_cohorts_same_cc_dc,
--    safe_divide(sum(revenue_no_change_cohorts_same_cc_dc) over(order by date rows between {N_days} preceding and current row),
--        sum(session_count_no_change_cohorts_same_cc_dc) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_no_change_cohorts_same_cc_dc,
--    --sum(revenue_change_cohorts_same_cc_dc) over(order by date rows between {N_days} preceding and current row) revenue_rolling_change_cohorts_same_cc_dc,
--    safe_divide(sum(revenue_change_cohorts_same_cc_dc) over(order by date rows between {N_days} preceding and current row),
--        sum(session_count_change_cohorts_same_cc_dc) over(order by date rows between {N_days} preceding and current row)) * 1000 rps_rolling_change_cohorts
--
--from results_domain
--join results_cc_dc using (date)