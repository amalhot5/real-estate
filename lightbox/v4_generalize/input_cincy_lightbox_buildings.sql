SELECT assessment_lid
	, add_address_lid
	, parcel_lid
	, total_assessed_value
	, land_assessed_value
	, building_assessed_value
	, total_market_value
 	, l.display_address
	, add_house_number
	, add_prefix_direction
	, add_street_name
	, add_suffix_type
	, add_suffix_direction
	, add_zip
	, objectid
	, es.listingkey
	, es.id as listing_id
	, es.display_address as listing_address
FROM cincy_2408.lightbox_full l
	left join cincy_2408.matches m on m.building_id = l.objectid 
	left join cincy_2408.euphrades_seen es on es.id = m.listing_id ;