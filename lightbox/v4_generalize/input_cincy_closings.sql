with status as (
    select listing_id
		, status_code
		, price
		, update_transaction
		, row_number() over(partition by listing_id order by update_transaction desc) as rank
	from listing_histories
	where listing_id in (select id from listings where source='cincymls') and status_code = 500
)
select listings.id
    , rebny_id
    , r.listing_key
    , building_id
    , address
    , listings.zip
    , price as sold_price
    , update_transaction as last_event
    --, buildings.source_id as buildings_source_id
from listings 
inner join status on status.listing_id = listings.id and rank=1
left join reso_reso_properties r on listings.rebny_id = r.listing_id
--left join data_load.icaar_building_load i on i.pw_bid = building_id
--left join buildings on buildings.id = listings.building_id and buildings.source='lightbox'
where listings.source='cincymls'
;