
-- Question why do rps number is DAS report look consistently higher than what we see from eventstream? (DAS report rps: 7.5 -> 10)
-- DAS report uses freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_domain
-- an unexpanded version of this is: freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_domain

with t1 as (
select
    (SELECT REGEXP_EXTRACT(kvps, "fs_auction_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_auction_id=%" LIMIT 1) AS fs_auction_id,
    (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_clientservermask=%" LIMIT 1) AS  fs_clientservermask,
    (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
    (SELECT REGEXP_EXTRACT(kvps, "fs_session_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_session_id=%" LIMIT 1) AS fs_session_id
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
    where _PARTITIONDATE = '2024-08-21'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE
                kvpss LIKE "fs_auction_id=%"
                OR kvpss LIKE "fs_testgroup=%"
                OR kvpss LIKE "fs_clientservermask=%"
                OR kvpss LIKE "fs_session_id=%"
        ) >= 4
)
select count(distinct(fs_session_id))
where
from t1

-- 24,500,301


SELECT
        CAST(FORMAT('%.10f', COALESCE(ROUND(SUM(bwr.cpm), 0), 0) / 1e7) AS float64) AS revenue,
    FROM
        `freestar-157323.prod_eventstream.bidswon_raw` bwr
   where _PARTITIONDATE = '2024-08-21'

 --258251.4816101

 -- which gives an overall rps of :




-- how many rows in auction_end_raw have valid fs_auction_id?


with t1 as (
select
    session_id,
    auction_id,
    fs_auction_id,
     (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE                kvpss LIKE "fs_auction_id=%"
        ) = 1 as has_fs_auction_id,
    (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE kvpss LIKE "fs_testgroup=%"
        ) = 1 as has_fs_testgroup,
    (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE kvpss LIKE "fs_clientservermask=%"
        ) = 1 as has_cs_mask,
    (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE kvpss LIKE "fs_session_id=%"
        ) = 1 as has_fs_session_id,
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
    where _PARTITIONDATE in ('2024-08-19')#, '2024-08-20', '2024-08-21')
)
select
count(*) count, count(auction_id) has_auction_id, count(fs_auction_id) has_fs_auction_id, count(session_id) has_session_id,
  sum(if(t1.has_fs_auction_id,1,0)) as has_fs_auction_id_kvps,
  sum(if(t1.has_fs_session_id,1,0)) as has_fs_session_id,
  sum(if(t1.has_cs_mask,1,0)) as has_fs_cs_mask,
  sum(if(t1.has_fs_testgroup,1,0)) as has_fs_testgroup,

from t1


--count	        has_auction_id	has_fs_auction_id	has_session_id	has_fs_auction_id_kvps	has_fs_session_id	has_fs_cs_mask	has_fs_testgroup
--1053052700	1021125261	    0	                1053052700	    436536926	            1005779888	        777770418	    1006446328
-- so only 40% of rows have a valid fs_auction_id and no rows have a valid fs_auction_id in the main table (i.e. not in kvps)