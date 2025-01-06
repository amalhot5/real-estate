select 
    source_id
    , data ->> 'ListingId' as listing_id
    , data ->> 'ListingKey' as listing_key 
	, data ->> 'ListingContractDate' as listing_date
from seen_objects 
where object_type = 'LISTING' 
    and feed_name = 'oh_cincy'
    
order by source_id
limit 100000
offset 1200000