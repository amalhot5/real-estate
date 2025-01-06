/*
    100 => "Commercial Sale",
    110 => "Residential Income",
    400 => "Rental",
    500 => "Residential",
    501 => "Residential",
    530 => "Condop",
    540 => "Condo",
    550 => "Co-op",
    560 => "Single-Family Townhouse",
    570 => "Mixed-Use Townhouse",
    580 => "Multi-Family Townhouse",
    590 => "Detached Home",
    600 => "Land",
    300 => "Manufactured in Park",
    200 => "Business Opportunity",
    610 => "Farm",
    999 => "Unknown"
*/
select case when lower(l.description) like '%balcony%' then 'private' 
            when lower(l.description) like '%balconies%' then 'private' 
            when lower(l.description) like '%private terrace%' then 'private' 
            when lower(l.description) like '%private roof%' then 'private' 
            when lower(l.description) like '%private outdoor%' then 'private' 
            when lower(l.description) like '%private patio%' then 'private' 
            when lower(l.description) like '%wrap terrace%' then 'private'
            when lower(l.description) like '%private yard%' then 'private'
			when lower(l.description) like '%private backyard%' then 'private'
			when lower(l.description) like '%private front yard%' then 'private'
			when a.balcony=true then 'private'
            when property_type_code in (560, 580, 590, 600, 610) then 'private'
            when lower(l.description) like '%apartment features%' then 'maybe private'
            when lower(l.description) like '%unit features%' then 'maybe private'
            --case lower(l.description) like '%private courtyard%' then true 
        
        --when lower(l.description) 
        else 'public' end as private_or_public
	--, count(*)
    , l.id
    , l.address
    , l.unit_number
    , l.description
	, l.property_type_code
	, property_images.original_url as image_url
from listings l
    inner join amenities a on a.amenities_type = 'Listing' and l.id = a.amenities_id
	left join property_images 
		on property_images.listing_id = l.id 
		and floor_plan = true 
		and not original_url is null
where hidden != true
    and not l.source in ('cincymls', 'ca_crmls', 'crmls', 'crmls-bk', 'cincymls_test', 'icaar')
    and ( 
        outdoor_space = true or (balcony=true or terrace=true or garden=true) 
    )
--group by 1
;