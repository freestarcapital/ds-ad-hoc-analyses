drop table if exists `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_1`;
create table `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_1` as
with t1 as (
  select split(device_category, '_')[0] device_category_raw, right(device_category, length(device_category)-strpos(device_category,'_')) user_category, *
  from `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025`
), t2 as (
  select *
  from t1
  where user_category != 'other'
  qualify count(*) over(partition by device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name) = 2
), t3 as (
  select lower(device_category) device_category_raw, * except (device_category)
  from `sublime-elixir-273810.training.floors_vertex_main_green_28042025`
)
select date, hour_x, hour_y, country_code, ad_unit_name, t2.device_category, t2.ad_requests_per_hour, t2.cpma, t2.fill_rate, t2.floor_price
from t2
join t3 using (device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name);
-- order by date, hour_x, hour_y, country_code, ad_unit_name, device_category
-- limit 1000


drop table if exists `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_2`;
create table `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_2` as
with t1 as (
  select split(device_category, '_')[0] device_category_raw, right(device_category, length(device_category)-strpos(device_category,'_')) user_category, *
  from `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025`
), t2 as (
  select *
  from t1
  where user_category != 'other'
  qualify count(*) over(partition by device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name) = 2
), t3 as (
  select lower(device_category) device_category_raw, * except (device_category)
  from `sublime-elixir-273810.training.floors_vertex_main_green_28042025`
)
select date, hour_x, hour_y, country_code, ad_unit_name, t2.device_category_raw device_category, t2.ad_requests_per_hour, t2.cpma, t2.fill_rate, t2.floor_price
from t2
join t3 using (device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name);
-- order by date, hour_x, hour_y, country_code, ad_unit_name, device_category
-- limit 1000



drop table if exists `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_3`;
create table `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_3` as
with t1 as (
  select split(device_category, '_')[0] device_category_raw, right(device_category, length(device_category)-strpos(device_category,'_')) user_category, *
  from `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025`
), t2 as (
  select *
  from t1
  where user_category != 'other'
  qualify count(*) over(partition by device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name) = 2
), t3 as (
  select lower(device_category) device_category_raw, * except (device_category)
  from `sublime-elixir-273810.training.floors_vertex_main_green_28042025`
)
select date, hour_x, hour_y, country_code, ad_unit_name, t2.device_category_raw device_category, t3.ad_requests_per_hour, t3.cpma, t3.fill_rate, t2.floor_price
from t2
join t3 using (device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name);
-- order by date, hour_x, hour_y, country_code, ad_unit_name, device_category
-- limit 1000

drop table if exists `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_4`;
create table `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025_expt_4` as
with t1 as (
  select split(device_category, '_')[0] device_category_raw, right(device_category, length(device_category)-strpos(device_category,'_')) user_category, *
  from `streamamp-qa-239417.training.floors_vertex_devbrowser-signal_green_28042025`
), t2 as (
  select *
  from t1
  where user_category != 'other'
  qualify count(*) over(partition by device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name) = 2
), t3 as (
  select lower(device_category) device_category_raw, * except (device_category)
  from `sublime-elixir-273810.training.floors_vertex_main_green_28042025`
)
select date, hour_x, hour_y, country_code, ad_unit_name, t2.device_category, t3.ad_requests_per_hour, t3.cpma, t3.fill_rate, t2.floor_price
from t2
join t3 using (device_category_raw, date, hour_x, hour_y, country_code, ad_unit_name);
-- order by date, hour_x, hour_y, country_code, ad_unit_name, device_category
-- limit 1000
