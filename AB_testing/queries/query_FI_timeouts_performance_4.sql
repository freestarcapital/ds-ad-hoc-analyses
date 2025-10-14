
with t1 as (
select distinct test_group, session_id
from freestar-157323.prod_eventstream.pagehits_raw
where _PARTITIONDATE = '2025-9-14'
       and country_code = 'US'
       and `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) = 'desktop'
       and NET.REG_DOMAIN(page_url) = 'tagged.com'
       and test_name = '8786f93d-34a7-4dfc-a89e-8d1970b753a9'
),
t2 as (
       select *
       from t1
       qualify count(*) over(partition by session_id) = 1
),

t3 as (

    select t2.*,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fs_clientservermask=%" LIMIT 1) AS fs_clientservermask,
       (SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fsrefresh=%" LIMIT 1) AS fsrefresh

    from `freestar-157323.prod_eventstream.auction_start_raw` t10
    join t2 using (session_id)
       where _PARTITIONDATE = '2025-9-14'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss in ("fs_testgroup=optimised", "fs_testgroup=optimised-static-timeout-3002")
        ) = 1
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fs_clientservermask=%"
        ) = 1
       AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fsrefresh=%"
        ) = 1

)


select test_group, fs_testgroup, count(*)

from t3
group by 1, 2

limit 100;




with t1 as (
select distinct test_group, session_id
from freestar-157323.prod_eventstream.pagehits_raw
where _PARTITIONDATE = '2025-9-14'
       and country_code = 'US'
       and `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) = 'desktop'
       and NET.REG_DOMAIN(page_url) = 'tagged.com'
       and test_name = '8786f93d-34a7-4dfc-a89e-8d1970b753a9'
),
t2 as (
       select *
       from t1
       qualify count(*) over(partition by session_id) = 1
),

t3 as (

    select t2.*,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fs_clientservermask=%" LIMIT 1) AS fs_clientservermask,
       (SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fsrefresh=%" LIMIT 1) AS fsrefresh

    from `freestar-157323.prod_eventstream.auction_start_raw` t10
    join t2 using (session_id)
       where _PARTITIONDATE = '2025-9-14'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss in ("fs_testgroup=optimised", "fs_testgroup=optimised-static-timeout-3002")
        ) = 1
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fs_clientservermask=%"
        ) = 1
       AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fsrefresh=%"
        ) = 1

)


--select test_group, fs_testgroup, count(*)

--select fsrefresh,
select fs_clientservermask,
       count(*),
       countif(fs_testgroup='optimised-static-timeout-3002'),
       countif(fs_testgroup='optimised'),
       safe_divide(countif(fs_testgroup='optimised-static-timeout-3002'), count(*)),
       safe_divide(countif(fs_testgroup='optimised'), count(*))
from t3
group by 1
order by 2 desc

limit 100;



with t1 as (
select distinct test_group, session_id
from freestar-157323.prod_eventstream.pagehits_raw
where _PARTITIONDATE = '2025-9-14'
       and country_code = 'US'
       and `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) = 'desktop'
       and NET.REG_DOMAIN(page_url) = 'tagged.com'
       and test_name = '8786f93d-34a7-4dfc-a89e-8d1970b753a9'
),
t2 as (
       select *
       from t1
       qualify count(*) over(partition by session_id) = 1
),

t3 as (

    select t2.*,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fs_clientservermask=%" LIMIT 1) AS fs_clientservermask,
       (SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fsrefresh=%" LIMIT 1) AS fsrefresh

    from `freestar-157323.prod_eventstream.auction_start_raw` t10
    join t2 using (session_id)
       where _PARTITIONDATE = '2025-9-14'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss in ("fs_testgroup=optimised", "fs_testgroup=optimised-static-timeout-3002")
        ) = 1
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fs_clientservermask=%"
        ) = 1
       AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fsrefresh=%"
        ) = 1

)


--select test_group, fs_testgroup, count(*)

--select fsrefresh,
select fs_clientservermask,
       count(*),
       countif(fs_testgroup='optimised-static-timeout-3002'),
       countif(fs_testgroup='optimised'),
       safe_divide(countif(fs_testgroup='optimised-static-timeout-3002'), count(*)),
       safe_divide(countif(fs_testgroup='optimised'), count(*)),
       countif(test_group=0),
       countif(test_group=1),
       safe_divide(countif(test_group=0), count(*)),
       safe_divide(countif(test_group=1), count(*))

from t3
group by 1
order by 2 desc

limit 100

--------

with t1 as (
select distinct test_group, session_id
from freestar-157323.prod_eventstream.pagehits_raw
where _PARTITIONDATE = '2025-9-14'
       and country_code = 'US'
       and `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) = 'desktop'
       and NET.REG_DOMAIN(page_url) = 'tagged.com'
       and test_name = '8786f93d-34a7-4dfc-a89e-8d1970b753a9'
),
t2 as (
       select *
       from t1
       qualify count(*) over(partition by session_id) = 1
),

t3 as (

    select distinct t2.test_group, session_id,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fs_clientservermask=%" LIMIT 1) AS fs_clientservermask,
       --(SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(t10.kvps) kvps WHERE kvps LIKE "fsrefresh=%" LIMIT 1) AS fsrefresh

    from `freestar-157323.prod_eventstream.auction_start_raw` t10
    join t2 using (session_id)
       where _PARTITIONDATE = '2025-9-14'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss in ("fs_testgroup=optimised", "fs_testgroup=optimised-static-timeout-3002")
        ) = 1
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t10.kvps) kvpss
            WHERE
                kvpss like "fs_clientservermask=%"
        ) = 1
       -- AND (
       --      SELECT COUNT(1)
       --      FROM UNNEST(t10.kvps) kvpss
       --      WHERE
       --          kvpss like "fsrefresh=%"
       --  ) = 1

),

t4 as (
       select session_id,
             sum(cpm / 1e7) as revenue
       from `freestar-157323.prod_eventstream.bidswon_raw` bwr
       where _PARTITIONDATE = '2025-9-14'

        --AND NET.REG_DOMAIN(page_url) = 'tagged.com'

       group by 1
)

--select test_group, fs_testgroup, count(*)

--select fsrefresh,
select fs_clientservermask,
       sum(revenue),
       count(*),
       sum(revenue),

       sum(if(fs_testgroup='optimised-static-timeout-3002', revenue, 0)) revenue_st,
       sum(if(fs_testgroup='optimised', revenue, 0)) revenue_opt,

       countif(fs_testgroup='optimised-static-timeout-3002'),
       countif(fs_testgroup='optimised'),

       safe_divide(countif(fs_testgroup='optimised-static-timeout-3002'), count(*)),
       safe_divide(countif(fs_testgroup='optimised'), count(*)),

       safe_divide(sum(if(fs_testgroup='optimised-static-timeout-3002', revenue, 0)), countif(fs_testgroup='optimised-static-timeout-3002')) * 1000 rps_st,
       safe_divide(sum(if(fs_testgroup='optimised', revenue, 0)), countif(fs_testgroup='optimised')) * 1000 rps_opt,


       countif(test_group=0),
       countif(test_group=1),

       sum(if(test_group=0, revenue, 0)) revenue_0,
       sum(if(test_group=1, revenue, 0)) revenue_1,

       safe_divide(countif(test_group=0), count(*)),
       safe_divide(countif(test_group=1), count(*)),

       safe_divide(sum(if(test_group=0, revenue, 0)), countif(test_group=0)) * 1000 rps_0,
       safe_divide(sum(if(test_group=1, revenue, 0)), countif(test_group=1)) * 1000 rps_1


from t3 left join t4 using (session_id)
group by 1
order by 2 desc

limit 100

-------------------


-- with t1 as (
-- select distinct --test_group,
--        session_id
-- from freestar-157323.prod_eventstream.pagehits_raw
-- where _PARTITIONDATE = '2025-10-08'
--        and country_code = 'US'
--        and `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) = 'desktop'
--        and NET.REG_DOMAIN(page_url) = 'tagged.com'
-- ),

-- t3 as (

--     select distinct fs_auction_id

--     from `freestar-157323.prod_eventstream.auction_start_raw` asr
--     join t1 using (session_id)
--        where _PARTITIONDATE = '2025-10-08'

-- ),

-- t4 as (
--        select fs_auction_id,  auction_timeout,
--            (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(bwr.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
--                    (SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(bwr.kvps) kvps WHERE kvps LIKE "%fsrefresh=%" LIMIT 1) AS fsrefresh



--        from `freestar-157323.prod_eventstream.bidsresponse_raw` bwr
--        join t3 using (fs_auction_id)

--        where _PARTITIONDATE = '2025-10-08'
--            AND (
--             SELECT COUNT(1)
--             FROM UNNEST(bwr.kvps) kvpss
--             WHERE
--                 kvpss like "fs_testgroup=optimised%"
--         ) = 1
--        AND (
--             SELECT COUNT(1)
--             FROM UNNEST(bwr.kvps) kvpss
--             WHERE
--                 kvpss like "fsrefresh%"
--         ) = 1

--         and fs_auction_id is not null
--         and status_message = 'Bid available'
-- ),

-- t5 as (

--        select fsrefresh='0' is_refresh, fs_testgroup, auction_timeout, count(*) count
--        from t4
--        group by 1, 2, 3
-- )

-- select *, 100 * count / sum(count) over(partition by is_refresh, fs_testgroup) percent
-- from t5
-- order by 1, 2, 3





with t1 as (
select distinct session_id
from freestar-157323.prod_eventstream.pagehits_raw
where _PARTITIONDATE = '2025-10-08'
       and country_code = 'US'
       and `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) = 'desktop'
       and NET.REG_DOMAIN(page_url) = 'tagged.com'
       and test_name = '8786f93d-34a7-4dfc-a89e-8d1970b753a9'
),

t3 as (

           select distinct test_group , fs_auction_id
    from `freestar-157323.prod_eventstream.auction_start_raw` asr
    join t1 using (session_id)
       where _PARTITIONDATE = '2025-10-08'
),

t4 as (
       select  fs_auction_id, auction_timeout, test_group,
              (SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(bwr.kvps) kvps WHERE kvps LIKE "%fsrefresh=%" LIMIT 1) AS fsrefresh


       from `freestar-157323.prod_eventstream.bidsresponse_raw` bwr
       join t3 using (fs_auction_id)

       where _PARTITIONDATE = '2025-10-08'

        and fs_auction_id is not null
        and status_message = 'Bid available'

       AND (
            SELECT COUNT(1)
            FROM UNNEST(bwr.kvps) kvpss
            WHERE
                kvpss like "fsrefresh%"
        ) = 1

        and fs_auction_id is not null
        and status_message = 'Bid available'


),

t5 as (

       select fsrefresh='0' is_refresh, test_group, auction_timeout, count(*) count
       from t4
       group by 1, 2, 3
)

select *, 100 * count / sum(count) over(partition by is_refresh, test_group) percent
from t5
order by 1, 2, 3












