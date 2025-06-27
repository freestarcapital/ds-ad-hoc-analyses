with t1 as (
{select_clause}
),

t2 as (
select domain, ad_unit, str, 'floors_ad_units' as tab
from floors_ad_units
join t1 on lower(ad_unit) like str
),

t3 as (
select distinct str, domain
from t2
),

t4_raw as (
select distinct ad_unit_name
from strategy_view_data
),

t4 as (
select str, ad_unit_name, 'strategy_view_data' as tab
from t4_raw
join t1 on lower(ad_unit_name) like str
),

t5 as (
select ad_unit_name, domain, t3.str, tab
from t3 join t4 on t3.str = t4.str
)

select t1.str, t2.domain, t2.tab, t2.ad_unit, t5.domain, t5.tab, t5.ad_unit_name
from t1
left join t2 on t1.str = t2.str
full join t5 on t2.str = t5.str
