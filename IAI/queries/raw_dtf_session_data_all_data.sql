DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('{start_date}'), DATE('{end_date}'));

CREATE OR REPLACE TABLE `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data_new_all_data_{start_date}_{end_date}` AS

-- with pgv as (
--     select
--         page_url,
--         net.reg_domain(page_url) domain,
--         session_id,
--     from `freestar-157323.prod_eventstream.pagehits_raw`
--     where _PARTITIONDATE in UNNEST(ddates)
-- ),

with uni AS (
    SELECT
        EventDateMST date,
        fs_session_id,
        CustomTargeting LIKE '%fs_placementname=%flying_carpet%' is_flying_carpet,
        CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END AS revenue,
        CASE WHEN LineItemID > 0 THEN 1 ELSE 0 END impression,
        CASE WHEN LineItemID = 0 THEN 1 ELSE 0 END unfilled
    FROM `freestar-prod.data_transfer.NetworkImpressions` NetworkImpressions
    LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableLineItem_15184186` MatchTableLineItem
    ON LineItemID = ID AND MatchTableLineItem._PARTITIONDATE = EventDateMST
    WHERE fs_session_id IS NOT NULL
--        AND CustomTargeting LIKE '%fs_placementname=%flying_carpet%'
        AND
        (
            LineItemID = 0
            OR
            (
                REGEXP_CONTAINS(MatchTableLineItem.Name, '{HB}') AND NOT REGEXP_CONTAINS(MatchTableLineItem.Name, 'blockthrough'))
                OR
                REGEXP_CONTAINS(MatchTableLineItem.Name, 'A9 '
            )
        )
        AND NetworkImpressions.EventDateMST in UNNEST(ddates)

    UNION ALL

    SELECT
        EventDateMST date,
        fs_session_id,
        CustomTargeting LIKE '%fs_placementname=%flying_carpet%' is_flying_carpet,
        EstimatedBackfillRevenue AS revenue,
        1 impression,
        0 unfilled
    FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` NetworkImpressions
    WHERE fs_session_id IS NOT NULL
--        AND CustomTargeting LIKE '%fs_placementname=%flying_carpet%'
        AND NetworkImpressions.EventDateMST in UNNEST(ddates)

)
SELECT date,
--    domain,
--    page_url,
    fs_session_id,
    SUM(if(is_flying_carpet, revenue, 0)) AS flying_carpet_revenue,
    SUM(if(is_flying_carpet, impression, 0)) AS flying_carpet_impressions,
    SUM(if(is_flying_carpet, unfilled, 0)) AS flying_carpet_unfilled,
    SUM(revenue) AS all_revenue,
    SUM(impression) AS all_impressions,
    SUM(unfilled) AS all_unfilled

FROM uni
--LEFT JOIN pgv ON fs_session_id = pgv.session_id

GROUP BY 1, 2; --, 3, 4;

