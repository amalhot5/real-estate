select id
	, listing_id
	, listing_key
    , close_date
    , contingent_date
    , expiration_date
    , listing_contract_date
    , purchase_contract_date
    , withdrawn_date
    , list_price
	--, list_price_low
    --, current_price
    , original_list_price
    , close_price
	, price_change_timestamp
	, standard_status
	, previous_list_price
    , status_change_timestamp
    , coming_soon_timestamp
from public.reso_reso_properties 
where originating_system_i_d = 'crmls'
order by id
limit 100000
offset 10000000;