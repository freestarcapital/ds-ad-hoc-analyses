
with bidder_domains as (
select domain
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_START> DAY) AND DATE <= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_END> DAY)
    and fs_testgroup = 'experiment'
    and country_code is not null
    and status != 'disabled'
    and ad_product not like '%video%'
    and bidder = '<BIDDER>'
group by 1
having sum(session_count) > 1000
),  base_data as (
    SELECT bidder, domain, status,
        sum(revenue) revenue,
        sum(session_count) session_count,
        safe_divide(sum(revenue), sum(session_count)) * 1000 rps
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    join bidder_domains using (domain)
    WHERE DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_START> DAY) AND DATE <= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_END> DAY)
        and fs_testgroup = 'experiment'
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'

    group by bidder, domain, status
), results_1 as (
    select *, status = 'client' is_client, status in ('client', 'server') is_client_or_server
    from base_data
    qualify row_number() over (partition by bidder, domain order by rps desc) = 1
), results_2 as (
    select *, if(is_client, row_number() over (partition by domain, is_client order by rps desc), null) client_rank,
        countif(is_client) over (partition by domain) client_bidders,
        if(is_client_or_server, row_number() over (partition by domain, is_client_or_server order by rps desc), null) client_or_server_rank,
        countif(is_client_or_server) over (partition by domain) client_or_server_bidders
    from results_1
)
select *, (client_rank <= 8) and (client_or_server_rank <= 13) and is_client and is_client_or_server makes_cut
from results_2
