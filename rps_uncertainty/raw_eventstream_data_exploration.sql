
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
from t1

-- 24,500,301


SELECT
        CAST(FORMAT('%.10f', COALESCE(ROUND(SUM(bwr.cpm), 0), 0) / 1e7) AS float64) AS revenue,
    FROM
        `freestar-157323.prod_eventstream.bidswon_raw` bwr
   where _PARTITIONDATE = '2024-08-21'

 --258251.4816101

 -- which gives an overall rps of :