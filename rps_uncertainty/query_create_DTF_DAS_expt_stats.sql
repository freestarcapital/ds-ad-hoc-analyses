

DECLARE start_date DATE DEFAULT DATE_SUB('{processing_date}', INTERVAL {days_back_start} DAY);
DECLARE end_date DATE DEFAULT DATE_SUB('{processing_date}', INTERVAL {days_back_end} DAY);

CREATE OR REPLACE TABLE `{project_id}.DAS_eventstream_session_data.DTF_DAS_expt_stats_{processing_date}_{days_back_start}_{days_back_end}`
    OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

  WITH uni AS (
  SELECT EventDateMST date,
  fs_clientservermask,
  fs_testgroup,
  GeoLookup.CountryCode country_code,
  device_category,
  `freestar-157323.ad_manager_dtf`.RTTClassify(device_category, rtt) rtt_category,
  NET.REG_DOMAIN(RefererURL) domain,
  fs_session_id,
  CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END AS revenue,
  CASE WHEN LineItemID > 0 THEN 1 ELSE 0 END impression,
  CASE WHEN LineItemID = 0 THEN 1 ELSE 0 END unfilled
  FROM `freestar-prod.data_transfer.NetworkImpressions` NetworkImpressions
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableLineItem_15184186` MatchTableLineItem
  ON LineItemID = ID AND MatchTableLineItem._PARTITIONDATE = EventDateMST
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
  ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = EventDateMST
  WHERE fs_clientservermask IS NOT NULL
  AND fs_session_id IS NOT NULL
  AND (
    LineItemID = 0 OR (
    REGEXP_CONTAINS(MatchTableLineItem.Name, '{HB}') AND NOT REGEXP_CONTAINS(MatchTableLineItem.Name, 'blockthrough'))
      OR
    REGEXP_CONTAINS(MatchTableLineItem.Name, 'A9 ')
    )
  AND NetworkImpressions.EventDateMST BETWEEN start_date AND end_date

  UNION ALL

  SELECT EventDateMST date,
  fs_clientservermask,
  fs_testgroup,
  GeoLookup.CountryCode country_code,
  device_category,
  `freestar-157323.ad_manager_dtf`.RTTClassify(device_category, rtt) rtt_category,
  NET.REG_DOMAIN(RefererURL) domain,
  fs_session_id,
  EstimatedBackfillRevenue AS revenue,
  1 impression,
  0 unfilled
  FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` NetworkImpressions
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
  ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = EventDateMST
  WHERE fs_clientservermask IS NOT NULL
  AND fs_session_id IS NOT NULL
  AND NetworkImpressions.EventDateMST BETWEEN start_date AND end_date
),

agg as
(
    select fs_session_id, fs_clientservermask, country_code, device_category, sum(revenue) as revenue, min(date) date
    from uni
    where fs_testgroup = 'experiment'
    group by 1, 2, 3, 4
)

select fs_session_id, fs_clientservermask, country_code, device_category, date, --, revenue
    sum(revenue) over(partition by fs_session_id) revenue
from agg
