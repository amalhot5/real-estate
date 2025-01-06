select l.id as listing_id, property_images.original_url
--count(property_images.*)
from property_images
	inner join listings l on property_images.listing_id = l.id
    inner join amenities a on a.amenities_type = 'Listing' and l.id = a.amenities_id
where hidden != true
    and not l.source in ('cincymls', 'ca_crmls', 'crmls', 'crmls-bk', 'cincymls_test', 'icaar')
    and ( 
        outdoor_space = true or (balcony=true or terrace=true or garden=true) 
    )
    and floor_plan=true
    and not original_url is null

order by l.id
offset 100000
limit 100000