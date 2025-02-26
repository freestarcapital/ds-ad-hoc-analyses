DECLARE from_backfill_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY);
DECLARE to_backfill_date_exclusive DATE DEFAULT CURRENT_DATE();

BEGIN TRANSACTION;
DELETE FROM floors_us.daily_uplift_us_gam_v2 WHERE date >= from_backfill_date AND date < to_backfill_date_exclusive;
INSERT INTO floors_us.daily_uplift_us_gam_v2
WITH base AS (
SELECT  a.EventDateMST date,
		-- net.reg_domain(RefererURL) IN ('boredpanda.com', 'deepai.org', 'emojipedia.org', 'gocomics.com', 'lse.co.uk', 'tagged.com', 'aljazeera.com', 'aljazeera.net', 'hadviser.com', 'creaders.net') control,
				net.reg_domain(RefererURL) IN ('gobankingrates.com', 'vecteezy.com', 'typingclub.com', 'aljazeera.com', 'newrepublic.com', 'bandsintown.com', 'adsbexchange.com', 'aftvnews.com', 'aljazeera.net', 'creaders.net', 'worldometers.info', 'fontspace.com', 'traderie.com', 'netronline.com', 'bismanonline-app.com', 'bismanonline.com', 'azlyrics.com', 'statbroadcast.com', 'couponwells.com', 'kprofiles.com', 'myforecast.com', 'byowner.com', 'latest-hairstyles.com', 'powerthesaurus.org', 'audiomack.com', 'scrabble-solver.com', 'mapsofworld.com', 'pixlr.com', 'personality-database.com', 'newser.com', 'lipsum.com', 'getemoji.com') control,
        REGEXP_EXTRACT(CustomTargeting,".*floors_id=(.*?)[;$]") floors_id,
        CountryId,
        CASE WHEN CostType="CPM" THEN CostPerUnitInNetworkCurrency/1000 ELSE 0 END rev,
        CASE WHEN LineItemId = 0 THEN 1 ELSE 0 END unfilled,
        CASE WHEN LineItemId = 0 THEN 0 ELSE 1 END imps
FROM `freestar-prod.data_transfer.NetworkImpressions` a
left join `freestar-157323.ad_manager_dtf.p_MatchTableLineItem_15184186` match on a.LineItemId=match.Id AND match._PARTITIONDATE = a.EventDateMST
left join `freestar-157323.ad_manager_dtf.p_MatchTableCompany_15184186` co on a.AdvertiserId=co.Id AND co._PARTITIONDATE = a.EventDateMST
WHERE a.EventDateMST >= from_backfill_date AND a.EventDateMST < to_backfill_date_exclusive
AND (REGEXP_CONTAINS(co.Name, '(?i)^((T13-.*)|(fspb_.*)|(Google.*)|(Amazon)|(freestar_prebid)|(Mingle2)|(Brickseek)|(FootballDB)|(Ideas People.*)|(Blue Media Services)|(Mediaforce)|(WhatIsMyIPAddress)|(-)|(Open Bidding)|(AdSparc.*)|(Triple13)|(Adexchange)|(Ad Exchange)|(Freestar))$') OR LineItemId = 0)

UNION ALL

SELECT  a.EventDateMST date,
		-- net.reg_domain(RefererURL) IN ('boredpanda.com', 'deepai.org', 'emojipedia.org', 'gocomics.com', 'lse.co.uk', 'tagged.com', 'aljazeera.com', 'aljazeera.net', 'hadviser.com', 'creaders.net') control,
		net.reg_domain(RefererURL) IN ('gobankingrates.com', 'vecteezy.com', 'typingclub.com', 'aljazeera.com', 'newrepublic.com', 'bandsintown.com', 'adsbexchange.com', 'aftvnews.com', 'aljazeera.net', 'creaders.net', 'worldometers.info', 'fontspace.com', 'traderie.com', 'netronline.com', 'bismanonline-app.com', 'bismanonline.com', 'azlyrics.com', 'statbroadcast.com', 'couponwells.com', 'kprofiles.com', 'myforecast.com', 'byowner.com', 'latest-hairstyles.com', 'powerthesaurus.org', 'audiomack.com', 'scrabble-solver.com', 'mapsofworld.com', 'pixlr.com', 'personality-database.com', 'newser.com', 'lipsum.com', 'getemoji.com') control,
        REGEXP_EXTRACT(CustomTargeting,".*floors_id=(.*?)[;$]") floors_id,
        CountryId,
        EstimatedBackfillRevenue rev,
        0 unfilled,
        1 imps
FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` a
WHERE a.EventDateMST >= from_backfill_date AND a.EventDateMST < to_backfill_date_exclusive

),
control AS
(
    SELECT date, CountryId,
    IFNULL(SAFE_DIVIDE(SUM(IFNULL(rev, 0)), SUM(imps)+SUM(unfilled)), 0) * 1000 AS cpma,
    SUM(IFNULL(imps, 0)+IFNULL(unfilled, 0)) ad_requests,
    SUM(IFNULL(rev, 0)) rev
    FROM base
	WHERE control
    GROUP BY 1,2
),
optimised AS
(
    SELECT date, CountryId,
    IFNULL(SAFE_DIVIDE(SUM(IFNULL(rev, 0)), SUM(imps)+SUM(unfilled)), 0) * 1000 AS cpma,
    SUM(IFNULL(imps, 0)+IFNULL(unfilled, 0)) ad_requests,
    SUM(IFNULL(rev, 0)) rev
    FROM base
	WHERE (floors_id IS NOT NULL AND floors_id NOT IN ('timeout', 'control', 'learning') AND control IS NOT NULL)
    GROUP BY 1,2
)

SELECT date, SUM((optimised.cpma - control.cpma)/1000 * optimised.ad_requests) uplift,
SAFE_DIVIDE(SUM(optimised.cpma * (optimised.rev+control.rev)), SUM(optimised.rev+control.rev)) weighted_avg_optimised_cpma,
SAFE_DIVIDE(SUM(control.cpma * (optimised.rev+control.rev)), SUM(optimised.rev+control.rev)) weighted_avg_control_cpma
FROM optimised JOIN control USING (date, CountryId)
GROUP BY date
ORDER BY date;
COMMIT TRANSACTION;


