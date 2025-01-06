select data->>'MLSAreaMajor' as MLS_area_major
    , data->>'City' as city
    , data->>'CountyOrParish' as county_or_parish
    , data->>'Latitude' as Latitude
    , data->>'Longitude' as Longitude
from seen_objects where seen_objects.object_type='LISTING' 
    and seen_objects.feed_name = 'oh_cincy'
    and not (data->>'Latitude' is null)-- or left(data->>'Latitude',1)='0')
    and not (data->>'Longitude' is null)-- or left(data->>'Longitude',1)='0')
;