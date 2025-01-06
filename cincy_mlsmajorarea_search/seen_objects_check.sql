select count(*) 
from seen_objects so 
where object_type = 'LISTING' and feed_name = 'oh_cincy'
limit 10000;

select sum(case when "data" ->> 'City' is null then 0 else 1 end) as city_count
	, sum(case when "data" ->> 'PostalCode' is null then 0 else 1 end) as PostalCode_count
	, sum(case when "data" ->> 'MLSAreaMajor' is null then 0 else 1 end) as MLSAreaMajor_count
	, sum(case when "data" ->> 'CountyOrParish' is null then 0 else 1 end) as CountyOrParish_count
	, sum(case when "data" ->> 'Township' is null then 0 else 1 end) as Township_count
	, sum(case when "data" ->> 'SubdivisionName' is null then 0 else 1 end) as SubdivisionName_count
	, sum(case when "data" ->> 'Site' is null then 0 else 1 end) as Site_count
from seen_objects so 
where object_type = 'LISTING' and feed_name = 'oh_cincy';

select source_id
	, "data" ->> 'City' as city
	, "data" ->> 'PostalCode' as postal_code
	, "data" ->> 'MLSAreaMajor' as mls_area_major
	, "data" ->> 'CountyOrParish' as county_or_parish
	, "data" ->> 'SubdivisionName' as subdivision_name
	, "data" ->> 'Township' as township
	, "data" ->> 'Latitude' as latitude
	, "data" ->> 'Longitude' as longitude
from seen_objects so 
where object_type = 'LISTING' and feed_name = 'oh_cincy'
;