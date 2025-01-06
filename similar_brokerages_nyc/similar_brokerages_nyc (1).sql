with neighborhood as (
	select building_id
		, name 
	from building_geographies bg 
	inner join geographies 
		on geographies.id = bg.geography_id 
		and ancestry like '%1278%' -- TODO: 1278
		and ancestry_depth = 3
--	where building_id in (select * from service_area)
), status as (
  select listing_id
    , status_code
    , price
    , update_transaction
    , row_number() over(partition by listing_id order by update_transaction desc) as rank
  from listing_histories
  where listing_id in (select listings.id from listings inner join neighborhood n on n.building_id=listings.building_id)
    and status_code=500
    --and update_transaction >= '2024-01-01'
)

select brokerages.id
    , brokerages.name
    , brokerages.rebny_id as brokerage_mls_id
    --, array_agg(distinct(n.name)) as neighborhood_names
    , n.name as neighborhood_name
    , count(distinct(agents.id)) as agent_count
    , count(distinct(la.listing_id)) as total_listing_count
    , count(distinct(status.listing_id)) as sales_count
    , avg(price) as avg_sale_price
    , max(price) as max_sale_price
from brokerages
    inner join agents
        on agents.brokerage_id = brokerages.id
    inner join listing_agents la
        on la.agent_id = agents.id
    inner join listings
        on listings.id = la.listing_id
    left join neighborhood n
        on n.building_id=listings.building_id
    left join status
        on status.listing_id = listings.id
        and rank=1

where not brokerages.source in ('cincymls', 'icaar', 'ca_crmls')
group by 1,2,3,4