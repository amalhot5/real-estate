select source_id, data
	FROM public.seen_objects where feed_name='oh_cincy' and object_type='LISTING' and data ->> 'Longitude' != '0.000000' limit 100;
	
select case when (data ->> 'Latitude' = '0.000000'
				 or data ->> 'Longitude' = '0.000000'
				 or data ->> 'Latitude' is null
				 or data ->> 'Longitude' is null) then 'invalid'
			else 'valid' end as is_valid
	, count(source_id)
from public.seen_objects 
where feed_name='oh_cincy' and object_type='LISTING'
group by 1