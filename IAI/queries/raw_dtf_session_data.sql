DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('{start_date}'), DATE('{end_date}'));

CREATE OR REPLACE TABLE `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data_{test_id}` AS

with pgv as (
    select distinct session_id
    from `freestar-157323.prod_eventstream.pagehits_raw`
    where _PARTITIONDATE in UNNEST(ddates)
    and test_name = '{test_id}' and test_group = 1
),

uni AS (
  SELECT date(EventTimeStamp) date,
  fs_session_id,
  REGEXP_EXTRACT(CustomTargeting,".*fs-auuid=(.*?)[;$]") fs_auction_id,
  MatchAdUnit.Name ad_unit_name,
  CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END AS revenue,
  CASE WHEN LineItemID > 0 THEN 1 ELSE 0 END impression,
  CASE WHEN LineItemID = 0 THEN 1 ELSE 0 END unfilled
  FROM `freestar-prod.data_transfer.NetworkImpressions` NetworkImpressions
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableLineItem_15184186` MatchTableLineItem
  ON LineItemID = ID AND MatchTableLineItem._PARTITIONDATE = date(EventTimeStamp)
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
  ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = date(EventTimeStamp)
  join pgv on fs_session_id = pgv.session_id

  LEFT JOIN `freestar-prod.data_transfer.match_ad_unit_15184186` MatchAdUnit
  ON MatchAdUnit.Id = NetworkImpressions.AdUnitId AND MatchAdUnit.date = date(EventTimeStamp)

  WHERE fs_session_id IS NOT NULL
  AND (
    LineItemID = 0 OR (
    REGEXP_CONTAINS(MatchTableLineItem.Name, '{HB}') AND NOT REGEXP_CONTAINS(MatchTableLineItem.Name, 'blockthrough'))
      OR
    REGEXP_CONTAINS(MatchTableLineItem.Name, 'A9 ')
    )
  AND NetworkImpressions.EventDateMST in UNNEST(ddates)

  UNION ALL

  SELECT date(EventTimeStamp) date,
  fs_session_id,
  REGEXP_EXTRACT(CustomTargeting,".*fs-auuid=(.*?)[;$]") fs_auction_id,
  MatchAdUnit.Name ad_unit_name,
  EstimatedBackfillRevenue AS revenue,
  1 impression,
  0 unfilled
  FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` NetworkImpressions
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
  ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = date(EventTimeStamp)
  join pgv on fs_session_id = pgv.session_id

  LEFT JOIN `freestar-prod.data_transfer.match_ad_unit_15184186` MatchAdUnit
  ON MatchAdUnit.Id = NetworkImpressions.AdUnitId AND MatchAdUnit.date = date(EventTimeStamp)

  WHERE fs_session_id IS NOT NULL
  AND NetworkImpressions.EventDateMST in UNNEST(ddates)

)
SELECT date, fs_session_id,
    fs_auction_id,
    ad_unit_name,
    SUM(revenue) AS revenue,
    SUM(impression) impressions,
    SUM(unfilled) unfilled
FROM uni
GROUP BY 1, 2, 3, 4
