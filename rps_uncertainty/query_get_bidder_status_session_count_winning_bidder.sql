 select count(*)
  from `streamamp-qa-239417.DAS_eventstream_session_data.{session_data_tablename}`
  where REGEXP_CONTAINS(fs_clientservermask, '{bidder_mask}')
  {and_filter_string}