
DECLARE ddate DATE DEFAULT DATE("2025-08-12");

CREATE OR REPLACE TABLE streamamp-qa-239417.DAS_1_9_experiments.test_ab as

WITH
test_sessions as (
    select session_id, min(test_group) test_group
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where _PARTITIONDATE = ddate
        and test_name in (
            --'8790f122-798e-4dd5-8c32-6d11fe7f75aa', -- DeepAI -
            --'44e29598-2e00-4137-bf4a-669d9216e967', -- Baseball Reference -
            '35657b1a-fb51-43ff-8e7f-d1572120cf89')--, -- Pro Football Ref -
            --'326b6fda-b2b4-4974-a52b-67ed88c521ee', -- Signup Genius -
            --'3d1ff03b-87ea-4d55-a81e-9b6705f668fc') -- Perchance -
    group by 1
    --qualify max(test_group) over(partition by session_id) = min(test_group) over(partition by session_id)
),

device_class_cte AS (
    SELECT
        session_id,
        min(device_class) device_class,
        min(os) os
    FROM
        `freestar-157323.prod_eventstream.pagehits_raw`
    WHERE
        _PARTITIONDATE = ddate
        and session_id in (select session_id from test_sessions)
    GROUP BY
        session_id
),

auc_end AS (
    SELECT
        TIMESTAMP_MILLIS(server_time) ts,
        placement_id,
        DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) AS date,
        iso AS country_code,
        NET.REG_DOMAIN(auc_end.page_url) AS domain,
        session_id,
        fs_auction_id,
        unfilled
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
    WHERE
        _PARTITIONDATE = ddate
        and iso is not null AND TRIM(iso) != ''
    and fs_auction_id is not null
    and auction_type != 'GAM'
    and session_id in (select session_id from test_sessions)
),

auc_end_w_bwr AS (
    SELECT
        auc_end.date,
        auc_end.country_code,
        `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) AS device_category,
        auc_end.domain,
        auc_end.session_id,
        auc_end.fs_auction_id,
        bwr.bidder winning_bidder,
        case when unfilled then 0 else 1 end impression,
        case when unfilled then 1 else 0 end unfilled,
        CAST(FORMAT('%.10f', COALESCE(ROUND((bwr.cpm), 0), 0) / 1e7) AS float64) AS revenue,
    FROM
        auc_end
    LEFT JOIN `freestar-157323.prod_eventstream.bidswon_raw` bwr
    ON
        bwr.fs_auction_id = auc_end.fs_auction_id
        AND bwr.placement_id = auc_end.placement_id
        AND bwr._PARTITIONDATE = ddate
    LEFT JOIN
        device_class_cte
    ON
        auc_end.session_id = device_class_cte.session_id
    WHERE bwr.session_id in (select session_id from test_sessions)
),

dtf_auctions as (
      select
          DATE(EventTimestamp) date_utc,
          EventDateMST date_mst,
          GeoLookup.CountryCode country_code,
          `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
          net.reg_domain(RefererURL) domain,
          fs_session_id,
          REGEXP_EXTRACT(CustomTargeting,".*fs-auuid=(.*?)[;$]") fs_auction_id,
          case when LineItemID = 0 then 'unfilled' when REGEXP_CONTAINS(LineItem.Name, '{H}') then 'house' when REGEXP_CONTAINS(LineItem.Name, 'A9 ') then 'amazon' end winning_bidder,
          CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END AS revenue,
          impression,
          unfilled
      FROM `freestar-prod.data_transfer.NetworkImpressions` NetworkImpressions
      LEFT JOIN `freestar-prod.data_transfer.match_line_item_15184186` LineItem
      ON
          LineItem.Id = NetworkImpressions.LineItemId
          AND LineItem.date between date_sub(ddate, interval 1 day) and ddate
      LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
      ON
          GeoLookup.Id = NetworkImpressions.CountryId
          AND GeoLookup._PARTITIONDATE = ddate
      LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableAdUnit_15184186` AdUnit
      ON
          AdUnit.Id = NetworkImpressions.AdUnitId
          AND AdUnit._PARTITIONDATE = ddate
      WHERE NetworkImpressions.EventDateMST between date_sub(ddate, interval 1 day) and ddate
        AND (
            LineItemID = 0 --getting all the unfilled, by adding this we won't be needing unioned_processing and final_auction_level CTEs, validated the numbers as well.
            OR (
            REGEXP_CONTAINS(LineItem.Name, '{H}')) --house ads
            OR
            REGEXP_CONTAINS(LineItem.Name, 'A9 ') --amazon
            )
        and fs_session_id is not null
        and fs_session_id in (select session_id from test_sessions)

  union all

      select
          DATE(EventTimestamp) date_utc,
          EventDateMST date_mst,
          GeoLookup.CountryCode country_code,
          `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
          net.reg_domain(RefererURL) domain,
          fs_session_id,
          REGEXP_EXTRACT(CustomTargeting,".*fs-auuid=(.*?)[;$]") fs_auction_id,
          'non-prebid' winning_bidder,
          EstimatedBackfillRevenue AS revenue,
          impression,
          unfilled,
      from `freestar-prod.data_transfer.NetworkBackfillImpressions` NetworkImpressions
      LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
      ON
          GeoLookup.Id = NetworkImpressions.CountryId
          AND GeoLookup._PARTITIONDATE = ddate
      LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableAdUnit_15184186` AdUnit
      ON
          AdUnit.Id = NetworkImpressions.AdUnitId
          AND AdUnit._PARTITIONDATE = ddate
      WHERE NetworkImpressions.EventDateMST between date_sub(ddate, interval 1 day) and ddate
          and fs_session_id is not null
          and fs_session_id in (select session_id from test_sessions)),

auc_end_n_dtf_unioned as (
      select
        date,
        country_code,
        device_category,
        domain,
        session_id,
        fs_auction_id,
        winning_bidder,
        revenue,
        impression,
        unfilled
      from auc_end_w_bwr #prebid and prebid+gam
      where date=ddate

    union all

      select
        date_utc date,
        country_code,
        device_category,
        domain,
        fs_session_id session_id ,
        fs_auction_id,
        winning_bidder,
        revenue,
        impression,
        unfilled
      from dtf_auctions #gam+prebid+gam
      where date_utc=ddate
),

agg as (
    select date, session_id, winning_bidder, domain, country_code, device_category,
        count(distinct(fs_auction_id)) auctions,
        sum(impression) impression,
        sum(unfilled) unfilled,
        sum(revenue) revenue
    from auc_end_n_dtf_unioned
    where date = ddate
    group by date, session_id, winning_bidder, domain, country_code, device_category
)

select *
from agg
join test_sessions using (session_id)
