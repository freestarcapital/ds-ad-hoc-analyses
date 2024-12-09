with hourly_fill_rate as (
    select date_hour, sum(impressions + unfilled) requests,
        sum(impressions) impressions,
        safe_divide(sum(impressions), sum(impressions + unfilled)) fill_rate,
    from `sublime-elixir-273810.floors.detailed_reporting`
    where date_hour >= '{first_date}'
    and ad_unit_name = '{ad_unit_name}'
    group by 1
),

stats as (
    select *,
        avg(requests) over(order by date_hour rows between {N} preceding and current row) per_day_requests_sm,
        sqrt((avg(power(requests, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(requests) over(order by date_hour rows between {N} preceding and current row), 2))/{N}) per_day_requests_sm_err,

        avg(impressions) over(order by date_hour rows between {N} preceding and current row) per_day_impressions_sm,
        sqrt((avg(power(impressions, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(impressions) over(order by date_hour rows between {N} preceding and current row), 2))/{N}) per_day_impressions_sm_err,

        safe_divide(sum(impressions) over(order by date_hour rows between {N} preceding and current row),
            sum(requests) over(order by date_hour rows between {N} preceding and current row)) fill_rate_sm_ratio,

        avg(fill_rate) over(order by date_hour rows between {N} preceding and current row) fill_rate_sm,
        sqrt((avg(power(fill_rate, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(fill_rate) over(order by date_hour rows between {N} preceding and current row), 2))/{N}) fill_rate_sm_err

    from hourly_fill_rate
)

select *,
        per_day_requests_sm_err / per_day_requests_sm * 100 perc_requests_sm,
        per_day_impressions_sm_err / per_day_impressions_sm * 100 perc_impressions_sm,
        sqrt(power(per_day_requests_sm_err / per_day_requests_sm, 2) +
            power(per_day_impressions_sm_err / per_day_impressions_sm, 2)) * 100 perc_joint_sm,
        sqrt(power(per_day_requests_sm_err / per_day_requests_sm, 2) +
            power(per_day_impressions_sm_err / per_day_impressions_sm, 2)) * fill_rate_sm_ratio fill_rate_sm_ratio_err

from stats
order by date_hour