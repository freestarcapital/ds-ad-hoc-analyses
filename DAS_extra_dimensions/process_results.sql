with extra_dim as (
select e.bidder, e.country_code, e.device_category, e.rtt_category, e.fsrefresh, e.<EXTRA_DIM>,
  e.revenue revenue_expt,
  o.revenue revenue_opt,
  e.session_count session_count_expt,
  o.session_count session_count_opt
from `sublime-elixir-273810.ds_experiments_us.das_extra_dim_base_data_<EXTRA_DIM>_<COUNTRY_CODE_NAME>_experiment` e
join `sublime-elixir-273810.ds_experiments_us.das_extra_dim_base_data_<EXTRA_DIM>_<COUNTRY_CODE_NAME>_optimised` o
using (bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>)
),

aggregated as (
select bidder, country_code, device_category, rtt_category, fsrefresh,
  sum(revenue_expt) revenue_expt,
  sum(revenue_opt) revenue_opt,
  sum(session_count_expt) session_count_expt,
  sum(session_count_opt) session_count_opt
from extra_dim
group by bidder, country_code, device_category, rtt_category, fsrefresh
)

select 'without_<EXTRA_DIM>_<COUNTRY_CODE_NAME>' as scenario,
  avg(safe_divide(revenue_expt, session_count_expt)) * 1000 unweighted_avg_rps,
  sum(safe_divide(revenue_expt, session_count_expt) * session_count_opt) / sum(session_count_opt) * 1000 weighted_avg_rps,
  sum(safe_divide(revenue_expt, session_count_expt) * session_count_opt) as revenue,
  sum(session_count_opt) session_count
from aggregated

union all

select 'with_<EXTRA_DIM>_<COUNTRY_CODE_NAME>' as scenario,
  avg(safe_divide(revenue_expt, session_count_expt)) * 1000 unweighted_avg_rps,
  sum(safe_divide(revenue_expt, session_count_expt) * session_count_opt) / sum(session_count_opt) * 1000 weighted_avg_rps,
  sum(safe_divide(revenue_expt, session_count_expt) * session_count_opt) as revenue,
  sum(session_count_opt) session_count
from extra_dim
