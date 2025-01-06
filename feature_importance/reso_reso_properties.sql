SELECT listing_id
	, standard_status
	, above_grade_finished_area
	, appliances
	, architectural_style
	, association_amenities
	, case when association_fee_frequency = 'Annually' then association_fee / 12 else association_fee end as association_fee
	, association_y_n
	, attached_garage_y_n
	, listing_contract_date
	, cancellation_date
	, close_date
	, expiration_date
	, withdrawn_date
	, basement
	, bathrooms_total
	, buyer_agency_compensation
	, city
	, close_price
	, case when co_list_agent_key is null then 0 else 1 end as has_co_list_agent
	, common_interest
	--, building_materials
	, cooling
	, direction_faces
	, entry_level
	, exterior_features
	, fencing
	, fireplace_y_n
	, flooring
	, foundation_area
	, garage_spaces
	, garage_y_n
	, heating
	, high_school_district
	, horse_y_n
	, interior_features
	, laundry_features
	, levels
	, list_price
	, living_area
	, lot_features
	, lot_size_area
	, other_structures
	, originating_system_name
	, parking_features
	, parking_total
	, patio_and_porch_features
	, pets_allowed
	, photos_count
	, case when pool_features is null then 0 else 1 end as has_pool
	, property_type
	, property_sub_type
	, roof
	, road_frontage_type
	, rooms_total
	, structure_type
	, syndicate_to
	, r.view
	, case when virtual_tour_u_r_l_unbranded is null then 0 else 1 end as has_virtual_tour
	, year_built
	, sponsor_unit_y_n
	, attendance_type
	, renting_allowed_y_n
	, live_in_super_y_n
	, new_development_y_n
	, vow_included
	, idx_included
	, co_list_agent2_key
	, co_list_agent3_key
	, concession_months_free
	, concession_term_months
	, co_broke_agreement
	, list_office_i_d_x_participation_y_n
	, co_list_office_i_d_x_participation_y_n
	, zoning_types, flex_room_types
	, green_verification_y_n
	, auction_online_bid_y_n
	
	
FROM public.reso_reso_properties r
where state_or_province = 'NY'
and standard_status in ('Canceled', 'Expired', 'Withdrawn', 'Hold')
limit 65236;

-- get distribution for each standard status
/*select
    standard_status
    , count(distinct(listing_id))
from reso_reso_properties
where state_or_province = 'NY'
group by 1*/