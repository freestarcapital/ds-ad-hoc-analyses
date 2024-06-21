DECLARE ddate_start DATE DEFAULT "<DDATE_START>";
DECLARE ddate_end DATE DEFAULT "<DDATE_END>";

with session_base as (
    SELECT net.reg_domain(auc_end.page_url) domain,
    auc_end.session_id,
    server_time,
    SAFE_CAST(REGEXP_EXTRACT(kvps, "browsiPricePredicition=(.*)") AS NUMERIC) price_prediction
  FROM `freestar-157323.prod_eventstream.auction_end_raw` auc_end,
    auc_end.kvps kvps
  WHERE ddate_start <= auc_end._PARTITIONDATE and auc_end._PARTITIONDATE <= ddate_end
  AND kvps like 'browsiPricePredicition=%'
), session_base_with_rn as(
  select *,
  row_number() over(partition by domain, session_id order by server_time) as rn,
  count(*) over(partition by domain, session_id) rn_max
  from session_base
), sessions as (
  select domain, session_id,
    max(if(rn=1, price_prediction, 0)) price_prediction_first,
    max(if(rn=rn_max, price_prediction, 0)) price_prediction_last,
  avg(price_prediction) price_prediction_session_avg
  from session_base_with_rn
  group by 1, 2
), ad_requests AS (
  SELECT
        net.reg_domain(RefererURL) domain,
        REGEXP_EXTRACT(CustomTargeting,".*fs_session_id=(.*?)[;$]") session_id,
        CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END rev,
        CASE WHEN LineItemId = 0 THEN 1 ELSE 0 END unfilled,
        CASE WHEN LineItemId = 0 THEN 0 ELSE 1 END imps
  FROM `freestar-prod.data_transfer.NetworkImpressions` a
  left join `freestar-157323.ad_manager_dtf.p_MatchTableLineItem_15184186` match on a.LineItemId=match.Id AND match._PARTITIONDATE = EventDateMST
  WHERE ddate_start <=  EventDateMST and EventDateMST <= ddate_end

UNION ALL

  SELECT
        net.reg_domain(RefererURL) domain,
        REGEXP_EXTRACT(CustomTargeting,".*fs_session_id=(.*?)[;$]") session_id,
        EstimatedBackfillRevenue rev,
        0 unfilled,
        1 imps
  FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` a
  WHERE ddate_start <=  EventDateMST and EventDateMST <= ddate_end
), agg AS (
  SELECT domain, session_id, SAFE_DIVIDE(SUM(rev)*1000, SUM(imps)+SUM(unfilled)) cpma
  FROM ad_requests
  GROUP BY 1, 2
)
SELECT agg.domain, price_prediction_first, price_prediction_last, price_prediction_session_avg, agg.cpma,
FROM sessions JOIN agg ON agg.session_id = sessions.session_id and agg.domain = sessions.domain