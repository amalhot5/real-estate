SELECT listing_id
	, standard_status
 availability_date,
 cancellation_date,
 close_date,
 contingent_date,
 contract_status_change_date,
 expiration_date,
 land_lease_expiration_date,
 listing_contract_date,
 off_market_date,
 on_market_date,
 purchase_contract_date,
 withdrawn_date,
 updated_at,
 furnished_availability_date,
 vendor_ignore_update_y_n,
 tenant_lease_expires_date,
 estimated_close_date
 , close_price
, furnished_list_price
, list_price
, list_price_low
, original_list_price
, previous_list_price
, price_change_timestamp
FROM public.reso_reso_properties
where originating_system_i_d = 'icaar';