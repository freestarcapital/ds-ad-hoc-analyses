with hourly_fill_rate as (
    select {date_hour},
        SUM(coalesce(if(advertiser="House", 0, impressions), 0)) impressions,
        SUM(impressions + unfilled) requests,
        safe_divide(SUM(coalesce(if(advertiser="House", 0, impressions), 0)),
                    SUM(impressions + unfilled)) fill_rate,
    from `sublime-elixir-273810.floors.detailed_reporting`
    where date_hour >= TIMESTAMP(DATE_SUB('{first_date}', INTERVAL 3 DAY))
        and date_hour <= '{last_date}'
    and {ad_unit_name_match}
    group by 1
),

stats as (
    select *,
        avg(requests) over(order by date_hour rows between {N} preceding and current row) {granularity}_requests_sm,
        sqrt((avg(power(requests, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(requests) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_requests_sm_err,

        avg(impressions) over(order by date_hour rows between {N} preceding and current row) {granularity}_impressions_sm,
        sqrt((avg(power(impressions, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(impressions) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_impressions_sm_err,

        safe_divide(sum(impressions) over(order by date_hour rows between {N} preceding and current row),
            sum(requests) over(order by date_hour rows between {N} preceding and current row)) fill_rate_sm_ratio,

        avg(fill_rate) over(order by date_hour rows between {N} preceding and current row) fill_rate_sm,
        sqrt((avg(power(fill_rate, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(fill_rate) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) fill_rate_sm_err

    from hourly_fill_rate
)

select *,
        {granularity}_requests_sm_err / {granularity}_requests_sm * 100 perc_requests_sm,
        {granularity}_impressions_sm_err / {granularity}_impressions_sm * 100 perc_impressions_sm,
        sqrt(power({granularity}_requests_sm_err / {granularity}_requests_sm, 2) +
            power({granularity}_impressions_sm_err / {granularity}_impressions_sm, 2)) * 100 perc_joint_sm,
        sqrt(power({granularity}_requests_sm_err / {granularity}_requests_sm, 2) +
            power({granularity}_impressions_sm_err / {granularity}_impressions_sm, 2)) * fill_rate_sm_ratio fill_rate_sm_ratio_err

from stats
where date_hour >= '{first_date}' and date_hour <= '{last_date}'
order by date_hour