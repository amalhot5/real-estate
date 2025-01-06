with status as (
    select listing_id
		, status_code
		, price
		, update_transaction
		, row_number() over(partition by listing_id order by update_transaction desc) as rank
	from listing_histories
	where listing_id in (select id from listings where source='icaar') and status_code = 500
)
select listings.id
    , rebny_id
    , building_id
    , address
    , listings.zip
    , price as sold_price
    , update_transaction as last_event
    , add_address_lid
    , add_house_number
    , add_prefix_direction
    , add_street_name
    , add_suffix_direction
    , add_suffix_type
    , parcel_lid
    , assessment_lid
    --, pw_bid as pw_building_id
    , land_assessed_value
    , land_market_value
from listings inner join status on status.listing_id = listings.id
left join data_load.icaar_building_load i on i.pw_bid = building_id
--where i.pw_bid is null