DECLARE ddate DATE DEFAULT "<DDATE>";

WITH sessions AS (
  SELECT net.reg_domain(auc_end.page_url) domain,
  auc_end.session_id,
  CAST(AVG(SAFE_CAST(REGEXP_EXTRACT(kvps, "browsiPricePredicition=(.*)") AS NUMERIC)) AS NUMERIC) price_prediction,
  MAX(page.country_code), MAX(page.device_class), COUNT(DISTINCT auc_end.pageview_id) page_count
  FROM `freestar-157323.prod_eventstream.auction_end_raw` auc_end,
    auc_end.kvps kvps
  LEFT JOIN `freestar-157323.prod_eventstream.pagehits_raw` page
  ON page.pageview_id = auc_end.pageview_id AND page._PARTITIONDATE = auc_end._PARTITIONDATE
  WHERE auc_end._PARTITIONDATE = ddate
  AND kvps like 'browsiPricePredicition=%'
  GROUP BY 1,2
), ad_requests AS (
  SELECT
        net.reg_domain(RefererURL) domain,
        GeoLookup.CountryCode country_code,
        `freestar-157323.ad_manager_dtf`.device_category(`freestar-157323.ad_manager_dtf`.DeviceType(DeviceCategory, OS)) device_category,
        REGEXP_EXTRACT(CustomTargeting,".*fs_session_id=(.*?)[;$]") session_id,
        CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END rev,
        CASE WHEN LineItemId = 0 THEN 1 ELSE 0 END unfilled,
        CASE WHEN LineItemId = 0 THEN 0 ELSE 1 END imps
  FROM `freestar-prod.data_transfer.NetworkImpressions` NetworkImpressions
  left join `freestar-157323.ad_manager_dtf.p_MatchTableLineItem_15184186` match
        on NetworkImpressions.LineItemId=match.Id AND match._PARTITIONDATE = EventDateMST
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
        ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = NetworkImpressions.EventDateMST
  WHERE EventDateMST = ddate

UNION ALL

  SELECT
        net.reg_domain(RefererURL) domain,
        GeoLookup.CountryCode country_code,
        `freestar-157323.ad_manager_dtf`.device_category(`freestar-157323.ad_manager_dtf`.DeviceType(DeviceCategory, OS)) device_category,
        REGEXP_EXTRACT(CustomTargeting,".*fs_session_id=(.*?)[;$]") session_id,
        EstimatedBackfillRevenue rev,
        0 unfilled,
        1 imps
  FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` NetworkImpressions
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
        ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = NetworkImpressions.EventDateMST
  WHERE EventDateMST = ddate
), agg AS (
  SELECT domain, session_id, SUM(rev) rev, SUM(imps) imps, SUM(unfilled) unfilled, SUM(imps)+SUM(unfilled) ad_requests,
  SAFE_DIVIDE(SUM(rev)*1000, SUM(imps)+SUM(unfilled)) cpma, SAFE_DIVIDE(SUM(rev)*1000, SUM(imps)) cpm,
  SUM(rev)*1000 rps
  FROM ad_requests
  where country_code = 'US' and device_category = 'desktop'
  GROUP BY 1, 2
)
SELECT agg.domain, sessions.price_prediction, agg.cpma, SAFE_DIVIDE(rev*1000, sessions.page_count) rpp
FROM sessions JOIN agg ON agg.session_id = sessions.session_id and agg.domain = sessions.domain
WHERE ad_requests > 10 AND ad_requests < 100