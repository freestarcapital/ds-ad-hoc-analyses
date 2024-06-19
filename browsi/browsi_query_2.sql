DECLARE ddate DATE DEFAULT "<DDATE>";

SELECT "" domain, bwr.cpm/1e4 cpma, predict.browsiPricePredicition price_prediction
 FROM `freestar-157323.prod_eventstream.bidswon_raw` bwr
 JOIN
 (SELECT (SELECT REGEXP_EXTRACT(kvps, "fs_auction_id=(.*)") fs_auction_id FROM auc_end.kvps WHERE kvps LIKE "%fs_auction_id=%") fs_auction_id,
 (SELECT REGEXP_EXTRACT(kvps, "browsiPricePredicition=(.*)") browsiPricePredicition FROM auc_end.kvps WHERE kvps LIKE "%browsiPricePredicition=%") browsiPricePredicition
 FROM `freestar-157323.prod_eventstream.auction_end_raw` auc_end
 WHERE _PARTITIONDATE = ddate
 ) predict
ON bwr.auction_id = predict.fs_auction_id
WHERE _PARTITIONDATE = ddate
AND browsiPricePredicition IS NOT NULL
