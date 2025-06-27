--insert into fill_rate_ad_units

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
),

t6 as (
select t1.str, t2.domain domain_floors_ad_units, t2.tab, t2.ad_unit ad_unit_floors_ad_units,
	t5.domain domain_strategy_view_data, t5.tab, t5.ad_unit_name ad_unit_name_strategy_view_data
from t1
left join t2 on t1.str = t2.str
full join t5 on t2.str = t5.str
),

t7 as (
select *
from t6
where domain_floors_ad_units is not null
	and ad_unit_floors_ad_units is not null
	and domain_strategy_view_data is not null
	and ad_unit_name_strategy_view_data is not null
),

t8 as (
select str, ad_unit_floors_ad_units as ad_unit, domain_floors_ad_units as domain
from t7

union all

select str, ad_unit_name_strategy_view_data as ad_unit, domain_strategy_view_data as domain
from t7
)

select ad_unit, domain
from t8
order by str

