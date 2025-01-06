with rel_listings as (
    select id from listings where brokerage_id in (select id from brokerages where lower(name) like '%sotheby%')
), status as (
  select listing_id
    , status_code
    , price
    , update_transaction
    , row_number() over(partition by listing_id order by update_transaction desc) as rank
  from listing_histories
    inner join rel_listings on listing_id = rel_listings.id
), list_agent_one as (
    select listing_id
        , agent_id
        , row_number() over(partition by listing_id order by listing_agents.id) as rank
    from listing_agents
        inner join rel_listings on rel_listings.id = listing_id
    where not listing_id is null
)
select 'PRCH-' || listings.id as mls_id
    --, listings.id as pw_listing_id
    , listings.address
    , listings.unit_number
    , a1.first_name || ' ' || a1.last_name as first_agent_name
    , o.name as office
    , sale_or_rental
    , curr_status.price as last_price
    , case
        when curr_status.status_code = 50 then 'Coming Soon'
        when curr_status.status_code = 100 then 'Active'
        when curr_status.status_code = 200 then 'Contingent'
        when curr_status.status_code = 500 then 'Closed'
        when curr_status.status_code = 400 then 'Closed'
        when curr_status.status_code = 620 then 'Canceled'
        when curr_status.status_code = 240 then 'Pending'
        when curr_status.status_code = 300 then 'Expired'
        when curr_status.status_code = 600 then 'Withdrawn'
        when curr_status.status_code = 640 then 'Hold'
        else curr_status.status_code::varchar end as status
    --, to_char(listings.created_at, 'MM/DD/YYY HH24:MI:SS') as listing_created_date
    --, imported_object_id
    --, json_changes ->> 'expiration' as expiration_date_changes
    , to_char(imported_object_updates.created_at, 'MM/DD/YYY HH24:MI:SS') as change_date
    , to_char(expiration, 'MM/DD/YYY HH24:MI:SS') as current_expiration_date
    /*, a1.email as first_agent_email
    , coalesce(a1.office_phone, a1.cell_phone) as second_agent_phone
    , a2.first_name || ' ' || a2.last_name as second_agent_name
    , a2.email as first_agent_email
    , coalesce(a2.office_phone, a2.cell_phone) as second_agent_phone
    , a3.first_name || ' ' || a3.last_name as third_agent_name
    , a3.email as third_agent_email
    , coalesce(a3.office_phone, a3.cell_phone) as third_agent_phone
    , brokerages.name as brokerage*/
from imported_object_updates
    inner join rel_listings 
        on rel_listings.id = imported_object_id
    inner join listings 
        on listings.id = imported_object_id 
    --left join brokerages 
      --  on brokerages.id = listings.brokerage_id   
    left join list_agent_one la1 
        on la1.rank=1 and la1.listing_id = listings.id
    left join agents a1 
        on a1.id = la1.agent_id
    left join offices o on a1.office_id = o.id
    /*left join list_agent_one la2 
        on la2.rank=2 and la2.listing_id = listings.id
    left join agents a2 
        on a2.id = la2.agent_id
    left join list_agent_one la3 
        on la3.rank=3 and la3.listing_id = listings.id
    left join agents a3 
        on a3.id = la3.agent_id*/
    left join status curr_status 
        on curr_status.rank = 1 
        and curr_status.listing_id = listings.id
    
where updated_by='Broker' 
    and imported_object_type = 'Listing'
    and  json_changes ->> 'expiration' is not null
    and imported_object_updates.created_at between '2023-12-01' and '2024-03-15'
;