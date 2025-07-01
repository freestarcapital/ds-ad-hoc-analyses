{create_table}

with t1 as (
    select {dims},
      date_diff(date(date), PARSE_DATE('%Y-%m-%d', fill_rate_model_enabled_date), day) days_past_fill_rate_model_enabled_date,
        sum(sum_ad_requests_fr) sum_ad_requests_fr,
        sum(sum_ad_requests_rm) sum_ad_requests_rm,
        sum(cpm_rm * sum_ad_requests_rm) / sum(sum_ad_requests_rm) as cpm_rm,
        sum(cpm_fr * sum_ad_requests_fr) / sum(sum_ad_requests_fr) as cpm_fr,
        sum(cpma_rm * sum_ad_requests_rm) / sum(sum_ad_requests_rm) as cpma_rm,
        sum(cpma_fr * sum_ad_requests_fr) / sum(sum_ad_requests_fr) as cpma_fr,
        sum(fill_rate_rm * sum_ad_requests_rm) / sum(sum_ad_requests_rm) as fill_rate_rm,
        sum(fill_rate_fr * sum_ad_requests_fr) / sum(sum_ad_requests_fr) as fill_rate_fr,
        sum(ad_request_weighted_floor_price_rm * sum_ad_requests_rm) / sum(sum_ad_requests_rm) as  ad_request_weighted_floor_price_rm,
        sum(ad_request_weighted_floor_price_fr * sum_ad_requests_fr) / sum(sum_ad_requests_fr) as  ad_request_weighted_floor_price_fr
    from `sublime-elixir-273810.training_fill_rate.fill-rate_results_for_performance_checking`
    group by {dims}, days_past_fill_rate_model_enabled_date
),

before as (
    select {dims},
        avg(sum_ad_requests_fr) sum_ad_requests_fr_before,
        avg(sum_ad_requests_rm) sum_ad_requests_rm_before,
        avg(cpm_rm) cpm_rm_before,
        avg(cpm_fr) cpm_fr_before,
        avg(cpma_rm) cpma_rm_before,
        avg(cpma_fr) cpma_fr_before,
        avg(fill_rate_rm) fill_rate_rm_before,
        avg(fill_rate_fr) fill_rate_fr_before,
        avg(ad_request_weighted_floor_price_rm) ad_request_weighted_floor_price_rm_before,
        avg(ad_request_weighted_floor_price_fr) ad_request_weighted_floor_price_fr_before,
        count(*) N_before
    from t1
    where (-{before_and_after_analysis_days} <= days_past_fill_rate_model_enabled_date) and (days_past_fill_rate_model_enabled_date <= -1)
    group by {dims}
),

after as (
    select {dims},
        avg(sum_ad_requests_fr) sum_ad_requests_fr_after,
        avg(sum_ad_requests_rm) sum_ad_requests_rm_after,
        avg(cpm_rm) cpm_rm_after,
        avg(cpm_fr) cpm_fr_after,
        avg(cpma_rm) cpma_rm_after,
        avg(cpma_fr) cpma_fr_after,
        avg(fill_rate_rm) fill_rate_rm_after,
        avg(fill_rate_fr) fill_rate_fr_after,
        avg(ad_request_weighted_floor_price_rm) ad_request_weighted_floor_price_rm_after,
        avg(ad_request_weighted_floor_price_fr) ad_request_weighted_floor_price_fr_after,
        count(*) N_after
    from t1
    where (1 <= days_past_fill_rate_model_enabled_date) and (days_past_fill_rate_model_enabled_date <= {before_and_after_analysis_days})
    group by {dims}
)

select *,
    least(sum_ad_requests_fr_before, sum_ad_requests_rm_before, sum_ad_requests_fr_after, sum_ad_requests_rm_after) min_daily_ad_requests
from before
join after using ({dims})
where least(sum_ad_requests_fr_before, sum_ad_requests_rm_before, sum_ad_requests_fr_after, sum_ad_requests_rm_after) > {min_daily_ad_requests}
    and N_before = {before_and_after_analysis_days}
    and N_after = {before_and_after_analysis_days}
