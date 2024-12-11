with adv as (
    select id,
      max(if(REGEXP_CONTAINS(name, '(?i)^((fspb_.*)|(Google Ad Exchange)|(Amazon)|(freestar_prebid)|(Adexchange)|(Ad Exchange))$'), 1, 0)) is_adv,
      max(if(REGEXP_CONTAINS(name, '(?i)^((fspb_.*)|(Google Ad Exchange)|(Amazon)|(freestar_prebid)|(Adexchange)|(Ad Exchange)|(Google AdSense))$'), 1, 0)) is_adv_incl_adsense
    from `freestar-157323.ad_manager_dtf.p_MatchTableCompany_15184186`
    where _PARTITIONDATE >= DATE_SUB('{first_date}', INTERVAL 3 DAY)
        and _PARTITIONDATE <= DATE_ADD('{last_date}', INTERVAL 1 DAY)
    group by 1
),

hourly_fill_rate as (
    SELECT
        {date_hour},
        count(1) requests,
        sum(if(unfilled, 0, 1)) impressions,
        safe_divide(sum(if(unfilled, 0, 1)), count(1)) fill_rate,
        SUM(is_adv) impressions_adv,
        safe_divide(SUM(is_adv), count(1)) fill_rate_adv,
        SUM(is_adv_incl_adsense) impressions_adv_incl_adsense,
        safe_divide(SUM(is_adv_incl_adsense), count(1)) fill_rate_adv_incl_adsense
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc
    LEFT JOIN adv ON adv.ID = advertiser_id
    where auc._PARTITIONDATE >= DATE_SUB('{first_date}', INTERVAL 3 DAY)
        and auc._PARTITIONDATE <= DATE_ADD('{last_date}', INTERVAL 1 DAY)
    and {placement_id_match}
    group by 1
)

select *,
    avg(requests) over(order by date_hour rows between {N} preceding and current row) {granularity}_requests_sm,
    sqrt((avg(power(requests, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(requests) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_requests_sm_err,

    avg(impressions) over(order by date_hour rows between {N} preceding and current row) {granularity}_impressions_sm,
    sqrt((avg(power(impressions, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(impressions) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_impressions_sm_err,

    avg(impressions_adv) over(order by date_hour rows between {N} preceding and current row) {granularity}_impressions_adv_sm,
    sqrt((avg(power(impressions_adv, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(impressions_adv) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_impressions_adv_sm_err,

    avg(impressions_adv_incl_adsense) over(order by date_hour rows between {N} preceding and current row) {granularity}_impressions_adv_incl_adsense_sm,
    sqrt((avg(power(impressions_adv_incl_adsense, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(impressions_adv_incl_adsense) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_impressions_adv_incl_adsense_sm_err,

    avg(fill_rate) over(order by date_hour rows between {N} preceding and current row) fill_rate_sm,
    sqrt((avg(power(fill_rate, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(fill_rate) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) fill_rate_sm_err,

    avg(fill_rate_adv) over(order by date_hour rows between {N} preceding and current row) fill_rate_adv_sm,
    sqrt((avg(power(fill_rate_adv, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(fill_rate_adv) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) fill_rate_adv_sm_err,

    avg(fill_rate_adv_incl_adsense) over(order by date_hour rows between {N} preceding and current row) fill_rate_adv_incl_adsense_sm,
    sqrt((avg(power(fill_rate_adv_incl_adsense, 2)) over(order by date_hour rows between {N} preceding and current row) -
        power(avg(fill_rate_adv_incl_adsense) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) fill_rate_adv_incl_adsense_sm_err

from hourly_fill_rate
where '{first_date}' <= date_hour and date_hour <= '{last_date}'
order by date_hour
