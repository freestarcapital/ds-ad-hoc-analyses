with pgv as (
  select pageview_id, hit_time, server_time pgv_server_time
  FROM `freestar-157323.prod_eventstream.pagehits_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null
  qualify count(*) over (partition by pageview_id) = 1
), aer as (
  select pageview_id, max(server_time) max_time_aer
  from `freestar-157323.prod_eventstream.auction_end_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null and server_time is not null
  group by 1
), asr as (
  select pageview_id, max(server_time) max_time_asr
  from `freestar-157323.prod_eventstream.auction_start_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null and server_time is not null
  group by 1
)
, brr as (
  select pageview_id,
    min(request_timestamp) request_timestamp_min, min(response_timestamp) response_timestamp_min,
    max(request_timestamp) request_timestamp_max, max(response_timestamp) response_timestamp_max,
    max(greatest(coalesce(request_timestamp, 0), coalesce(response_timestamp, 0))) max_time_brr,
    min(auction_timeout) auction_timeout_min, max(auction_timeout) auction_timeout_max
  from `freestar-157323.prod_eventstream.bidsresponse_raw`
  WHERE _PARTITIONDATE = '2024-10-24' and pageview_id is not null
  group by 1

)
select *, greatest(coalesce(max_time_asr, 0), coalesce(max_time_aer, 0)) max_time,
    Timestamp_diff(TIMESTAMP_MILLIS(greatest(coalesce(max_time_asr, 0), coalesce(max_time_aer, 0), hit_time)),
        TIMESTAMP_MILLIS(hit_time), MILLISECOND) AS duration_ms_hit,
    Timestamp_diff(TIMESTAMP_MILLIS(greatest(coalesce(max_time_asr, 0), coalesce(max_time_aer, 0), pgv_server_time)),
        TIMESTAMP_MILLIS(pgv_server_time), MILLISECOND) AS pgv_server_time,

    Timestamp_diff(TIMESTAMP_MILLIS(max_time_asr), TIMESTAMP_MILLIS(request_timestamp_min), MILLISECOND) AS auc_st_to_req_min,
    Timestamp_diff(TIMESTAMP_MILLIS(max_time_asr), TIMESTAMP_MILLIS(response_timestamp_min), MILLISECOND) AS auc_st_to_resp_min,
    Timestamp_diff(TIMESTAMP_MILLIS(max_time_asr), TIMESTAMP_MILLIS(request_timestamp_max), MILLISECOND) AS auc_st_to_req_max,
    Timestamp_diff(TIMESTAMP_MILLIS(max_time_asr), TIMESTAMP_MILLIS(response_timestamp_max), MILLISECOND) AS auc_st_to_resp_max,
    Timestamp_diff(TIMESTAMP_MILLIS(hit_time), TIMESTAMP_MILLIS(max_time_asr), MILLISECOND) AS hit_to_auc_st,
   Timestamp_diff(TIMESTAMP_MILLIS(hit_time), TIMESTAMP_MILLIS(request_timestamp_min), MILLISECOND) AS hit_to_req_min,
    Timestamp_diff(TIMESTAMP_MILLIS(hit_time), TIMESTAMP_MILLIS(response_timestamp_min), MILLISECOND) AS hit_to_resp_min,
    Timestamp_diff(TIMESTAMP_MILLIS(hit_time), TIMESTAMP_MILLIS(request_timestamp_max), MILLISECOND) AS hit_to_req_max,
    Timestamp_diff(TIMESTAMP_MILLIS(hit_time), TIMESTAMP_MILLIS(response_timestamp_max), MILLISECOND) AS hit_to_resp_max