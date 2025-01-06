/*with closed_listings as (
    select listing_id
	from listing_histories
	where listing_id in (select id from listings where source='crmls') and status_code = 500
)*/
select listings.id
    , rebny_id
    , building_id
    , address
    , listings.zip
	, listings.sale_date
	, listings.sale_price
	, buildings.display_address
	, buildings.city
	, buildings.zip
from listings
left join buildings on buildings.id = listings.building_id
where listings.source='crmls'
	and listings.id in (
    select listing_id
	from listing_histories
	where listing_id in (select id from listings where source='crmls') and status_code = 500
)
order by listings.id
offset 5000000
limit 2000000